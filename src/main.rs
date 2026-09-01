use flate2::write::GzEncoder;
use flate2::Compression;
use reqwest::StatusCode;
use serde_json::{json, Value};
use std::collections::HashSet;
use std::env;
use std::fs::{self, File};
use std::io::Write;
use std::process::Command;
use std::thread::sleep;
use std::time::Duration;

// Multiple free RPC endpoints for round-robin rotation
const PUBLIC_RPCS: &[&str] = &[
    "https://bsc-dataseed.binance.org",
    "https://bsc-dataseed1.defibit.io",
    "https://bsc-dataseed1.ninicoin.io",
    "https://bsc.drpc.org",
    "https://binance.llamarpc.com",
    "https://bscrpc.com",
];

fn upload_to_release(tag: &str, file_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("Uploading {} directly to release tag '{}'...", file_name, tag);

    let status = Command::new("gh")
        .args(["release", "upload", tag, file_name, "--clobber"])
        .status()?;

    if status.success() {
        println!("Successfully uploaded {} to GitHub Release!", file_name);
        let _ = fs::remove_file(file_name);
    } else {
        eprintln!("Failed to upload {} to GitHub Release.", file_name);
    }
    Ok(())
}

// 1 single HTTP request mein multiple blocks ek saath fetch karna (JSON-RPC Batching)
fn fetch_blocks_chunk_with_retry(
    client: &reqwest::blocking::Client,
    blocks: &[u64],
    rpc_index: &mut usize,
) -> Vec<(String, String)> {
    let mut pairs = Vec::new();
    let mut backoff = Duration::from_secs(2);

    for _attempt in 0..5 {
        let rpc_url = PUBLIC_RPCS[*rpc_index % PUBLIC_RPCS.len()];
        *rpc_index += 1;

        // JSON-RPC Batch Payload
        let batch_payload: Vec<Value> = blocks
            .iter()
            .enumerate()
            .map(|(id, &b)| {
                json!({
                    "jsonrpc": "2.0",
                    "method": "eth_getBlockByNumber",
                    "params": [format!("0x{:x}", b), true],
                    "id": id as u64
                })
            })
            .collect();

        match client.post(rpc_url).json(&batch_payload).send() {
            Ok(resp) => {
                // Rate limit (429) ya Server overload (503) check
                if resp.status() == StatusCode::TOO_MANY_REQUESTS || resp.status() == StatusCode::SERVICE_UNAVAILABLE {
                    eprintln!("Rate limit hit on {}. Backing off for {:?}...", rpc_url, backoff);
                    sleep(backoff);
                    backoff *= 2;
                    continue;
                }

                if let Ok(Value::Array(responses)) = resp.json::<Value>() {
                    for single_resp in responses {
                        if let Some(transactions) = single_resp["result"]["transactions"].as_array() {
                            for tx in transactions {
                                let from = tx["from"].as_str().unwrap_or("").to_lowercase();
                                let to = tx["to"].as_str().unwrap_or("").to_lowercase();
                                if !from.is_empty() {
                                    pairs.push((from, to));
                                }
                            }
                        }
                    }
                    return pairs;
                }
            }
            Err(e) => {
                eprintln!("Network error on {} ({}). Retrying...", rpc_url, e);
                sleep(backoff);
                backoff *= 2;
            }
        }
    }

    pairs
}

fn process_batch(
    client: &reqwest::blocking::Client,
    release_tag: Option<&str>,
    start_block: u64,
    end_block: u64,
    batch_num: u32,
    rpc_index: &mut usize,
) -> Result<bool, Box<dyn std::error::Error>> {
    println!("\n==========================================");
    println!("Fetching Batch #{}: Blocks {} to {}", batch_num, start_block, end_block);
    println!("==========================================");

    let mut unique_addresses = HashSet::new();
    let chunk_size = 15; // Ek HTTP request me 15 blocks (Optimal for public nodes)

    let all_blocks: Vec<u64> = (start_block..end_block).collect();
    for chunk in all_blocks.chunks(chunk_size) {
        let txs = fetch_blocks_chunk_with_retry(client, chunk, rpc_index);
        for (from, to) in txs {
            unique_addresses.insert(from);
            if !to.is_empty() {
                unique_addresses.insert(to);
            }
        }
        // Polite delay taaki IP flag na ho
        sleep(Duration::from_millis(150));
    }

    if unique_addresses.is_empty() {
        println!("No addresses found in this range.");
        return Ok(true);
    }

    let file_name = format!("bnb_addresses_part_{:04}.csv.gz", batch_num);
    let file = File::create(&file_name)?;
    let mut encoder = GzEncoder::new(file, Compression::default());

    writeln!(encoder, "address")?;
    for addr in unique_addresses {
        writeln!(encoder, "{}", addr)?;
    }
    encoder.finish()?;

    println!("Batch #{} written successfully -> {}", batch_num, file_name);

    if let Some(tag) = release_tag {
        if let Err(e) = upload_to_release(tag, &file_name) {
            eprintln!("Upload error for {}: {:?}", file_name, e);
        }
    }

    Ok(true)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let release_tag = env::var("RELEASE_TAG").ok();

    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()?;

    let block_step: u64 = 2_000;
    let start_from_block: u64 = 35_000_000;
    let target_block: u64 = 35_050_000;

    let mut current_block = start_from_block;
    let mut batch_counter: u32 = 1;
    let mut rpc_index: usize = 0;

    println!("Starting safe multi-RPC scraper...");

    while current_block < target_block {
        let next_block = (current_block + block_step).min(target_block);

        let _ = process_batch(
            &client,
            release_tag.as_deref(),
            current_block,
            next_block,
            batch_counter,
            &mut rpc_index,
        );

        current_block = next_block;
        batch_counter += 1;
        sleep(Duration::from_secs(1));
    }

    println!("\nAll batches completed!");
    Ok(())
}

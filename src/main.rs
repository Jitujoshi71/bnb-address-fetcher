use flate2::write::GzEncoder;
use flate2::Compression;
use serde_json::Value;
use std::env;
use std::fs::{self, File};
use std::io::{copy, Cursor};
use std::process::Command;
use std::thread::sleep;
use std::time::Duration;

fn upload_to_release(tag: &str, file_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("Uploading {} directly to release tag '{}'...", file_name, tag);

    let status = Command::new("gh")
        .args(["release", "upload", tag, file_name, "--clobber"])
        .status()?;

    if status.success() {
        println!("Successfully uploaded {} to GitHub Release!", file_name);
        // Local file remove karein taaki disk space bachi rahe
        let _ = fs::remove_file(file_name);
    } else {
        eprintln!("Failed to upload {} to GitHub Release.", file_name);
    }
    Ok(())
}

fn fetch_batch(
    client: &reqwest::blocking::Client,
    api_key: &str,
    release_tag: Option<&str>,
    start_block: u64,
    end_block: u64,
    batch_num: u32,
) -> Result<bool, Box<dyn std::error::Error>> {
    println!("\n==========================================");
    println!("Fetching Batch #{}: Blocks {} to {}", batch_num, start_block, end_block);
    println!("==========================================");

    let sql_query = format!(
        "SELECT DISTINCT address FROM (SELECT \"from\" AS address FROM bnb.transactions WHERE block_number >= {} AND block_number < {} UNION ALL SELECT \"to\" AS address FROM bnb.transactions WHERE block_number >= {} AND block_number < {}) AS t WHERE address IS NOT NULL",
        start_block, end_block, start_block, end_block
    );

    let payload = serde_json::json!({
        "sql": sql_query
    });

    // 1. Submit Query to Dune
    let res = client
        .post("https://api.dune.com/api/v1/sql/execute")
        .header("X-DUNE-API-KEY", api_key)
        .header("Content-Type", "application/json")
        .json(&payload)
        .send()?;

    let resp_val: Value = res.json()?;
    let execution_id = match resp_val.get("execution_id").and_then(|v| v.as_str()) {
        Some(id) => id.to_string(),
        None => {
            eprintln!("Execution submit failed: {:?}", resp_val);
            return Ok(false);
        }
    };

    println!("Submitted successfully. Execution ID: {}", execution_id);

    // 2. Poll Query Status
    let status_url = format!("https://api.dune.com/api/v1/execution/{}/status", execution_id);
    let mut is_completed = false;

    for _ in 0..60 { // Max 60 attempts (~5 mins)
        sleep(Duration::from_secs(6));

        let res = match client.get(&status_url).header("X-DUNE-API-KEY", api_key).send() {
            Ok(r) => r,
            Err(e) => {
                eprintln!("Status polling retry... ({})", e);
                continue;
            }
        };

        if let Ok(json_res) = res.json::<Value>() {
            if let Some(state) = json_res.get("state").and_then(|s| s.as_str()) {
                if state == "QUERY_STATE_COMPLETED" {
                    is_completed = true;
                    println!("Query finished on Dune!");
                    break;
                } else if state == "QUERY_STATE_FAILED" || state == "QUERY_STATE_CANCELLED" {
                    eprintln!("Dune Query failed: {:?}", json_res);
                    return Ok(false);
                }
            }
        }
        println!("Still running batch #{} on Dune... waiting", batch_num);
    }

    if !is_completed {
        eprintln!("Batch #{} timed out.", batch_num);
        return Ok(false);
    }

    // 3. Download CSV Stream & Compress to .csv.gz
    let csv_url = format!("https://api.dune.com/api/v1/execution/{}/results/csv", execution_id);
    let mut csv_resp = client
        .get(&csv_url)
        .header("X-DUNE-API-KEY", api_key)
        .send()?;

    let file_name = format!("bnb_addresses_part_{:04}.csv.gz", batch_num);
    let file = File::create(&file_name)?;
    let mut encoder = GzEncoder::new(file, Compression::default());

    let mut content = Vec::new();
    csv_resp.copy_to(&mut content)?;

    let mut cursor = Cursor::new(content);
    copy(&mut cursor, &mut encoder)?;
    encoder.finish()?;

    println!("Batch #{} compressed successfully.", batch_num);

    // 4. Instant Live Upload to GitHub Release
    if let Some(tag) = release_tag {
        if let Err(e) = upload_to_release(tag, &file_name) {
            eprintln!("Upload error for {}: {:?}", file_name, e);
        }
    }

    Ok(true)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let api_key = env::var("DUNE_API_KEY").expect("DUNE_API_KEY required");
    let release_tag = env::var("RELEASE_TAG").ok();

    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(180))
        .build()?;

    let block_step: u64 = 500_000;
    let max_blocks: u64 = 42_000_000;

    let mut current_block: u64 = 0;
    let mut batch_counter: u32 = 1;

    println!("Starting real-time continuous batch pipeline...");

    while current_block < max_blocks {
        let next_block = (current_block + block_step).min(max_blocks);
        
        match fetch_batch(
            &client,
            &api_key,
            release_tag.as_deref(),
            current_block,
            next_block,
            batch_counter,
        ) {
            Ok(true) => (),
            Ok(false) => eprintln!("Batch #{} skipped.", batch_counter),
            Err(e) => eprintln!("Error in batch #{}: {:?}", batch_counter, e),
        }

        current_block = next_block;
        batch_counter += 1;

        // Rate limit cooling
        sleep(Duration::from_secs(3));
    }

    println!("\nAll batches finished and uploaded!");
    Ok(())
}

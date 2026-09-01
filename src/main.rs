use flate2::write::GzEncoder;
use flate2::Compression;
use serde::Deserialize;
use serde_json::json;
use std::env;
use std::fs::File;
use std::io::Write;
use std::thread::sleep;
use std::time::Duration;

#[derive(Deserialize, Debug)]
struct ExecutionResponse {
    execution_id: Option<String>,
    error: Option<String>,
}

#[derive(Deserialize, Debug)]
struct ResultResponse {
    state: String,
    result: Option<QueryResult>,
}

#[derive(Deserialize, Debug)]
struct QueryResult {
    rows: Vec<serde_json::Value>,
}

fn fetch_batch(
    client: &reqwest::blocking::Client,
    api_key: &str,
    start_block: u64,
    end_block: u64,
    batch_num: u32,
) -> Result<usize, Box<dyn std::error::Error>> {
    println!("\n==========================================");
    println!("Fetching Batch #{}: Blocks {} to {}", batch_num, start_block, end_block);
    println!("==========================================");

    let sql_query = format!(
        r#"
        SELECT DISTINCT address
        FROM (
            SELECT "from" AS address FROM bnb.transactions WHERE block_number >= {} AND block_number < {}
            UNION ALL
            SELECT "to" AS address FROM bnb.transactions WHERE block_number >= {} AND block_number < {}
        ) AS t
        WHERE address IS NOT NULL;
        "#,
        start_block, end_block, start_block, end_block
    );

    let payload = json!({
        "query_sql": sql_query,
        "performance": "medium"
    });

    let exec_res: ExecutionResponse = client
        .post("https://api.dune.com/api/v1/sql/execute")
        .header("X-DUNE-API-KEY", api_key)
        .header("Content-Type", "application/json")
        .json(&payload)
        .send()?
        .json()?;

    let execution_id = match exec_res.execution_id {
        Some(id) => id,
        None => {
            eprintln!("Execution failed for range {}-{}: {:?}", start_block, end_block, exec_res.error);
            return Ok(0);
        }
    };

    println!("Submitted successfully. Execution ID: {}", execution_id);

    let status_url = format!("https://api.dune.com/api/v1/execution/{}/results", execution_id);
    let mut rows: Vec<serde_json::Value> = Vec::new();

    loop {
        sleep(Duration::from_secs(8));
        let res: ResultResponse = client
            .get(&status_url)
            .header("X-DUNE-API-KEY", api_key)
            .send()?
            .json()?;

        if res.state == "QUERY_STATE_COMPLETED" {
            if let Some(r) = res.result {
                rows = r.rows;
            }
            break;
        } else if res.state == "QUERY_STATE_FAILED" || res.state == "QUERY_STATE_CANCELLED" {
            eprintln!("Query failed for batch #{} with state: {}", batch_num, res.state);
            return Ok(0);
        }
        println!("Still running batch #{} on Dune... waiting", batch_num);
    }

    if rows.is_empty() {
        println!("No rows returned for this block range.");
        return Ok(0);
    }

    // Save directly as compressed .csv.gz
    let file_name = format!("bnb_addresses_part_{:04}.csv.gz", batch_num);
    let file = File::create(&file_name)?;
    let enc = GzEncoder::new(file, Compression::default());
    let mut wtr = csv::Writer::from_writer(enc);

    wtr.write_record(&["address"])?;
    for row in &rows {
        if let Some(addr) = row.get("address").and_then(|v| v.as_str()) {
            wtr.write_record(&[addr])?;
        }
    }
    wtr.flush()?;

    println!("Saved {} records to {}", rows.len(), file_name);
    Ok(rows.len())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let api_key = env::var("DUNE_API_KEY").expect("DUNE_API_KEY env variable required");
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(120))
        .build()?;

    // Configuration
    let block_step: u64 = 2_000_000;  // 2 Million blocks per batch
    let max_blocks: u64 = 42_000_000;  // BNB current total height (~40M+)
    
    let mut current_block: u64 = 0;
    let mut batch_counter: u32 = 1;
    let mut total_addresses: usize = 0;

    println!("Starting full historical batch extraction...");

    while current_block < max_blocks {
        let next_block = (current_block + block_step).min(max_blocks);
        match fetch_batch(&client, &api_key, current_block, next_block, batch_counter) {
            Ok(count) => total_addresses += count,
            Err(e) => eprintln!("Error in batch #{}: {:?}", batch_counter, e),
        }

        current_block = next_block;
        batch_counter += 1;

        // Rate limit cooling
        sleep(Duration::from_secs(3));
    }

    println!("\nAll batches completed! Total processed rows: {}", total_addresses);
    Ok(())
}

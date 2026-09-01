use serde::Deserialize;
use serde_json::json;
use std::env;
use std::fs::File;
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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let api_key = env::var("DUNE_API_KEY").expect("DUNE_API_KEY environment variable missing");
    let client = reqwest::blocking::Client::new();

    let sql_query = r#"
        SELECT DISTINCT address
        FROM (
            SELECT "from" AS address FROM bnb.transactions WHERE block_time >= NOW() - interval '7' day
            UNION ALL
            SELECT "to" AS address FROM bnb.transactions WHERE block_time >= NOW() - interval '7' day
        ) AS t
        WHERE address IS NOT NULL
        LIMIT 50000;
    "#;

    println!("Submitting SQL query to Dune...");
    let payload = json!({
        "query_sql": sql_query,
        "performance": "medium"
    });

    let exec_res: ExecutionResponse = client
        .post("https://api.dune.com/api/v1/sql/execute")
        .header("X-DUNE-API-KEY", &api_key)
        .header("Content-Type", "application/json")
        .json(&payload)
        .send()?
        .json()?;

    let execution_id = match exec_res.execution_id {
        Some(id) => id,
        None => panic!("Execution submission failed: {:?}", exec_res.error),
    };

    println!("Execution ID: {}", execution_id);

    let status_url = format!("https://api.dune.com/api/v1/execution/{}/results", execution_id);
    let mut rows: Vec<serde_json::Value> = Vec::new();

    loop {
        println!("Polling results... waiting 5 seconds");
        sleep(Duration::from_secs(5));

        let res: ResultResponse = client
            .get(&status_url)
            .header("X-DUNE-API-KEY", &api_key)
            .send()?
            .json()?;

        if res.state == "QUERY_STATE_COMPLETED" {
            println!("Query execution finished successfully!");
            if let Some(r) = res.result {
                rows = r.rows;
            }
            break;
        } else if res.state == "QUERY_STATE_FAILED" || res.state == "QUERY_STATE_CANCELLED" {
            panic!("Query failed with state: {}", res.state);
        }
    }

    println!("Writing {} records to bnb_unique_addresses.csv...", rows.len());
    let file = File::create("bnb_unique_addresses.csv")?;
    let mut wtr = csv::Writer::from_writer(file);

    wtr.write_record(&["address"])?;

    for row in rows {
        if let Some(addr) = row.get("address").and_then(|v| v.as_str()) {
            wtr.write_record(&[addr])?;
        }
    }

    wtr.flush()?;
    println!("CSV generation completed successfully.");
    Ok(())
}

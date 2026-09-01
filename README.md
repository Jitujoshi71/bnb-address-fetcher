# BNB Address Fetcher (`bnb-address-fetcher`)

Free multi-RPC scraper written in Rust to extract unique active addresses (`from` and `to`) from BNB Chain blocks, compress them to `.csv.gz`, and upload directly to GitHub Releases.

## Features
- **Zero API Cost:** Uses free public JSON-RPC nodes with round-robin rotation.
- **Rate-Limit Safe:** Uses JSON-RPC array batching (15 blocks per call) + Exponential Backoff.
- **Low Memory & Disk Use:** Direct stream compression with immediate local cleanup after upload.
- **GitHub Actions Ready:** Run on-demand from the Actions tab.

## Local Setup & Run

```bash
# Clone the repository
git clone [https://github.com/](https://github.com/)<your-username>/bnb-address-fetcher.git
cd bnb-address-fetcher

# Set environment variables (Optional)
export START_BLOCK=35000000
export END_BLOCK=35010000
export BLOCK_STEP=1000
export RELEASE_TAG=v1.0.0

# Run in release mode
cargo run --release

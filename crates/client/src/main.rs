use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "raft-kv-cli", about = "Client for raft-kv cluster")]
struct Cli {
    #[arg(long, default_value = "http://127.0.0.1:8001")]
    addr: String,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    Get {
        key: String,
    },
    Set {
        key: String,
        value: String,
    },
    Delete {
        key: String,
    },
    /// Scan all keys with the given prefix (empty string = all keys).
    Scan {
        #[arg(long, default_value = "")]
        prefix: String,
    },
    Status,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let client = reqwest::Client::new();

    match cli.cmd {
        Cmd::Get { key } => {
            let res = client.get(format!("{}/kv/{key}", cli.addr)).send().await?;
            match res.status() {
                s if s.is_success() => println!("{}", res.text().await?),
                reqwest::StatusCode::NOT_FOUND => println!("(not found)"),
                s => anyhow::bail!("error: {s}"),
            }
        }
        Cmd::Set { key, value } => {
            let res = client
                .put(format!("{}/kv/{key}", cli.addr))
                .body(value)
                .send()
                .await?;
            anyhow::ensure!(res.status().is_success(), "set failed: {}", res.status());
            println!("ok");
        }
        Cmd::Delete { key } => {
            let res = client
                .delete(format!("{}/kv/{key}", cli.addr))
                .send()
                .await?;
            anyhow::ensure!(res.status().is_success(), "delete failed: {}", res.status());
            println!("ok");
        }
        Cmd::Scan { prefix } => {
            let url = format!("{}/kv?prefix={}", cli.addr, urlencoding::encode(&prefix));
            let res = client.get(url).send().await?;
            anyhow::ensure!(res.status().is_success(), "scan failed: {}", res.status());
            let map: std::collections::BTreeMap<String, String> = res.json().await?;
            if map.is_empty() {
                println!("(empty)");
            } else {
                for (k, v) in &map {
                    println!("{k} = {v}");
                }
            }
        }
        Cmd::Status => {
            let res = client.get(format!("{}/status", cli.addr)).send().await?;
            let json: serde_json::Value = res.json().await?;
            println!("{}", serde_json::to_string_pretty(&json)?);
        }
    }

    Ok(())
}

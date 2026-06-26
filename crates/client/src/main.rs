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
    Get { key: String },
    Set { key: String, value: String },
    Delete { key: String },
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
            let res = client.delete(format!("{}/kv/{key}", cli.addr)).send().await?;
            anyhow::ensure!(res.status().is_success(), "delete failed: {}", res.status());
            println!("ok");
        }
        Cmd::Status => {
            let res = client.get(format!("{}/status", cli.addr)).send().await?;
            let json: serde_json::Value = res.json().await?;
            println!("{}", serde_json::to_string_pretty(&json)?);
        }
    }

    Ok(())
}

use anyhow::{bail, Result};
use clap::Parser;
use dgraph_tonic::{Client, Mutate, Mutation};
use futures::prelude::*;
use indicatif::{ProgressBar, ProgressStyle, ProgressDrawTarget};
use serde_json::Value;
use tokio::io::{stdin, BufReader, AsyncBufReadExt};
use tokio_stream::wrappers::LinesStream;
use log::*;

#[derive(Parser, Debug)]
struct Args {
    /// HTTP or HTTPS url of gRPC endpoint for Alpha
    #[clap(short, long)]
    alpha: String,
    /// Keys that should be used to find existing documents for upsert (can be specified multiple
    /// times)
    #[clap(short = 'U', long)]
    upsert_keys: Vec<String>,
    /// Number of documents to load in each transaction
    #[clap(short = 's', long)]
    chunk_size: usize,
    /// How many transactions to run at once
    #[clap(short = 'c', long)]
    concurrency: usize,
    /// Disable progress output
    #[clap(short = 'q', long)]
    quiet: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();

    let client = Client::new(&args.alpha)?;

    let bar = ProgressBar::new_spinner();

    if args.quiet {
        bar.set_draw_target(ProgressDrawTarget::hidden());
    }

    bar.set_style(ProgressStyle::default_spinner()
        .template("{spinner} Elapsed:{elapsed_precise} Done:{pos} Rate:{per_sec}"));

    LinesStream::new(BufReader::new(stdin()).lines())
        .enumerate()
        .map(|(index, result)| result.map(|v| (index, v)))
        .try_chunks(args.chunk_size)
        .map_err(|e| e.into())
        .map_ok(|chunk| process_chunk(&client, &args.upsert_keys, chunk))
        .try_buffer_unordered(args.concurrency)
        .try_for_each(|completed| {
            let bar = &bar;
            async move { bar.inc(completed as u64); Ok(()) }
        })
        .await?;

    bar.finish_at_current_pos();

    Ok(())
}

async fn process_chunk(
    client: &Client,
    upsert_keys: &[String],
    chunk: Vec<(usize, String)>
) -> Result<usize> {
    let mut query = "{\n".into();

    let docs = chunk.into_iter().map(|(index, json)| {
        let mut doc: Value = serde_json::from_str(&json)?;
        let mut offset = 0;
        process_doc(upsert_keys, index, &mut offset, &mut query, &mut doc)?;
        Ok(doc)
    }).collect::<Result<Vec<Value>>>()?;

    query.push('}');

    trace!("query = {:?}, docs = {:?}", query, docs);

    let mut mutation = Mutation::new();

    mutation.set_set_json(&docs)?;

    client.new_mutated_txn().upsert_and_commit_now(
        query,
        mutation).await?;

    Ok(docs.len())
}

/// Generates upsert queries for the doc and inserts references to them in `uid` fields
fn process_doc(
    upsert_keys: &[String],
    index: usize,
    offset: &mut u32,
    query: &mut String,
    doc: &mut Value
) -> Result<()> {
    use std::fmt::Write;

    // look for keys in upsert_keys
    if let Some(obj) = doc.as_object_mut() {
        let mut vars = vec![];

        for key in upsert_keys {
            if let Some(value) = obj.get(key) {
                *offset += 1;
                let var_name = format!("v_{}_{}", index, offset);
                writeln!(query, "{} as var(func: eq(<{}>, {}))", var_name, key, value)?;
                vars.push(var_name);
            }
        }

        // Recurse for any sub-documents
        for (key, value) in obj.iter_mut() {
            if key == "uid" { continue; }

            match *value {
                Value::Object(_) => {
                    process_doc(upsert_keys, index, offset, query, value)?;
                },
                Value::Array(ref mut items) => {
                    for item in items.iter_mut() {
                        process_doc(upsert_keys, index, offset, query, item)?;
                    }
                },
                _ => ()
            }
        }

        // Insert vars as uid references to variables
        if !vars.is_empty() {
            obj.insert("uid".into(), Value::String(format!("uid({})", vars.join(","))));
        }
    } else {
        bail!("Expected object at {}, but found other document: {}", index, doc);
    }

    Ok(())
}

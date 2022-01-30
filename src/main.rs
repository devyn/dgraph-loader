use anyhow::{bail, Result};
use clap::Parser;
use dgraph_tonic::{Client, Mutate, Mutation, DgraphError, ClientError};
use futures::prelude::*;
use indicatif::{ProgressBar, ProgressStyle, ProgressDrawTarget};
use log::*;
use rand::Rng;
use regex::Regex;
use serde_json::{json, Value};
use std::sync::atomic::{Ordering::AcqRel, AtomicU64};
use std::time::Duration;
use std::mem::replace;
use tokio::io::{stdin, BufReader, AsyncBufReadExt};
use tokio_stream::wrappers::LinesStream;
use tonic::Code;

#[derive(Parser, Debug)]
struct Args {
    /// HTTP or HTTPS url of gRPC endpoint for Alpha
    #[clap(short, long)]
    alpha: String,
    /// Regex for keys that should be used to find existing documents for upsert (can be specified
    /// more than once)
    #[clap(short = 'U', long = "upsert-pattern")]
    upsert_patterns: Vec<String>,
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

    let upsert_patterns = args.upsert_patterns.iter()
        .map(|pat| Regex::new(&pat).map_err(|e| e.into()))
        .collect::<Result<Vec<_>>>()?;

    let client = Client::new(&args.alpha)?;

    let bar = ProgressBar::new_spinner();

    if args.quiet {
        bar.set_draw_target(ProgressDrawTarget::hidden());
    } else {
        bar.set_draw_target(ProgressDrawTarget::stderr_nohz());
    }

    bar.set_style(ProgressStyle::default_spinner()
        .template("{spinner} Elapsed:{elapsed_precise} N-Quads:{pos} Rate:{per_sec} {msg}"));

    let txns_counter = AtomicU64::new(0);
    let docs_counter = AtomicU64::new(0);
    let abort_counter = AtomicU64::new(0);

    LinesStream::new(BufReader::new(stdin()).lines())
        .enumerate()
        .map(|(index, result)| result.map(|v| (index, v)))
        .try_chunks(args.chunk_size)
        .map_err(|e| e.into())
        .map_ok(|chunk| process_chunk(&client, &upsert_patterns, chunk))
        .try_buffer_unordered(args.concurrency)
        .try_for_each(|stats| {
            let bar = &bar;
            let txns_counter = &txns_counter;
            let docs_counter = &docs_counter;
            let abort_counter = &abort_counter;

            async move {
                let done_txns = txns_counter.fetch_add(1, AcqRel);
                let done_docs = docs_counter.fetch_add(stats.completed_docs, AcqRel);
                let aborts = abort_counter.fetch_add(stats.aborted, AcqRel);
                bar.inc(stats.completed_nquads);
                bar.set_message(format!(
                        "Txns:{} Docs:{} Aborts:{}",
                        done_txns, done_docs, aborts));
                Ok(())
            }
        })
        .await?;

    bar.finish_at_current_pos();

    Ok(())
}

struct ProcessChunkStats {
    completed_docs: u64,
    completed_nquads: u64,
    aborted: u64,
}

async fn process_chunk(
    client: &Client,
    upsert_patterns: &[Regex],
    chunk: Vec<(usize, String)>
) -> Result<ProcessChunkStats> {
    let mut query = "{\n".into();

    let mut set_docs = vec![]; // merged to one mutation
    let mut mutations = vec![]; // other mutations

    let total_docs = chunk.len() as u64;
    let mut total_nquads = 0;

    for (index, json) in chunk {
        let mut doc: Value = serde_json::from_str(&json)?;
        let mut offset = 0;

        // Calculate anticipated nquad length
        total_nquads += count_nquads(&doc);

        process_doc(
            upsert_patterns,
            index,
            &mut offset,
            &mut query,
            &mut set_docs,
            &mut mutations,
            &mut doc
        )?;
    }

    query.push('}');

    trace!("query = {:?}", query);

    // Create final mutation for set-docs
    if set_docs.len() > 0 {
        let mut mutation = Mutation::new();

        mutation.set_set_json(&set_docs)?;

        mutations.push(mutation);
    }

    let mut aborted: u64 = 0;

    // Retry aborted transactions or too many requests
    'retry: loop {
        let res = client.new_mutated_txn().upsert_and_commit_now(
            &query,
            mutations.clone()).await;

        if let Err(e) = res {
            // Return if not a DgraphError
            match e.downcast::<DgraphError>()? {
                DgraphError::GrpcError(failure) => {
                    warn!("Grpc Error: Failure: {:?}", failure);

                    match failure.downcast_ref::<ClientError>() {
                        Some(ClientError::CannotDoRequest(ref status)) => {
                            // Found the tonic Status
                            match status.code() {
                                // Maybe aborted transaction, or too many requests
                                Code::Aborted | Code::ResourceExhausted => {
                                    aborted += 1;
                                    // Wait a (random) little bit, then retry
                                    let msec = rand::thread_rng().gen_range(500..1500);
                                    tokio::time::sleep(Duration::from_millis(msec)).await;
                                    continue 'retry;
                                },
                                _ => ()
                            }
                        },
                        _ => ()
                    }

                    // Otherwise... reconstruct the error and pass it up
                    return Err(DgraphError::GrpcError(failure).into());
                },
                other => return Err(other.into())
            }
        } else {
            break;
        }
    }

    Ok(ProcessChunkStats {
        completed_docs: total_docs,
        completed_nquads: total_nquads,
        aborted
    })
}

/// Generates upsert queries for the doc and inserts references to them in `uid` fields
///
/// Splits inner nodes into separate mutations, and creates conditional mutations for upsert keys
fn process_doc(
    upsert_patterns: &[Regex],
    index: usize,
    offset: &mut u32,
    query: &mut String,
    set_docs: &mut Vec<Value>, // will be merged to one mutation
    mutations: &mut Vec<Mutation>,
    doc: &mut Value
) -> Result<()> {
    use std::fmt::Write;

    if let Some(obj) = doc.as_object_mut() {
        let mut vars: Vec<(String, String, Value)> = vec![];

        // First, scan for any upsert keys and create variables for them
        for (key, value) in obj.iter() {
            if key == "uid" {
                bail!("Manually setting uid is not supported, at {}", index);
            }

            // Add upsert variable
            if upsert_patterns.iter().any(|p| p.is_match(key)) {
                *offset += 1;

                let var_name = format!("v_{}_{}", index, offset);
                vars.push((var_name, key.clone(), value.clone()));

            }
        }

        // Decide var name for the document itself (don't create extra query if only one var)
        let var_name = if vars.len() == 1 {
            vars[0].0.clone()
        } else {
            format!("v_{}", index)
        };

        // Add mutations for setting the upsert keys
        for (var, key, value) in &vars {
            let mut mutation = Mutation::new();

            trace!("Adding mutation for upsert key {} if var {} not set - set to {}",
                key, var, value);

            mutation.set_set_json(&json!(
                [{
                    "uid": format!("uid({})", var),
                    key: value
                }]
            ))?;
            mutation.set_cond(format!("@if(eq(len({}), 0))", var));

            mutations.push(mutation);

            // Remove upsert key from the main doc
            obj.remove(key);
        }

        // Add queries for vars
        for (var, key, value) in &vars {
            writeln!(query, "{} as var(func: eq(<{}>, {}), first: 1)", var, key, value)?;
        }

        // Add union query for main var if more than one var
        if vars.len() > 1 {
            let var_names = vars.iter().map(|(var, _, _)| var.as_str()).collect::<Vec<_>>();
            writeln!(query, "{} as var(func: uid({}), first: 1)", var_name, var_names.join(","))?;
        }

        // Recurse for any sub-documents
        for (_, value) in obj.iter_mut() {
            match *value {
                Value::Object(_) if is_node(&value) => {
                    process_doc(
                        upsert_patterns,
                        index,
                        offset,
                        query,
                        set_docs,
                        mutations,
                        value
                    )?;
                },
                Value::Array(ref mut items) if items.iter().all(|i| is_node(&i)) => {
                    for item in items.iter_mut() {
                        process_doc(
                            upsert_patterns,
                            index,
                            offset,
                            query,
                            set_docs,
                            mutations,
                            item
                        )?;
                    }
                },
                _ => ()
            }
        }

        // If we didn't set any vars, use a blank node
        let this_ref = if vars.len() > 0 {
            format!("uid({})", var_name)
        } else {
            format!("_:{}", var_name)
        };

        // Set uid on my object to reference
        obj.insert("uid".into(), json!(this_ref.clone()));

        // Add set-doc for setting all other properties, as long as there are still some to set
        if obj.len() > 1 {
            trace!("Adding set-doc for later mutation: {:?}", obj);

            // Create plain reference object to leave in place
            let mut uid_obj = serde_json::Map::new();

            uid_obj.insert("uid".into(), json!(this_ref.clone()));

            set_docs.push(json!(replace(obj, uid_obj)));
        }
    } else {
        bail!("Expected object at {}, but found other document: {}", index, doc);
    }

    Ok(())
}

static GEOJSON_TYPES: &[&str] = &[
    "Point",
    "LineString",
    "Polygon",
    "MultiPoint",
    "MultiLineString",
    "MultiPolygon",
    "GeometryCollection",
    "Feature",
    "FeatureCollection",
];

/// Check if value is an object and doesn't look like geojson
fn is_node(value: &Value) -> bool {
    let is_geojson = value["type"].as_str()
        .map(|s| GEOJSON_TYPES.contains(&s)).unwrap_or(false);
    value.is_object() && !is_geojson
}

/// Count anticipated number of nquads in a document
fn count_nquads(value: &Value) -> u64 {
    if is_node(value) {
        value.as_object().unwrap().iter()
            .filter(|&(key, _)| key != "uid")
            .map(|(_, value)| count_nquads(value))
            .sum()
    } else {
        1
    }
}

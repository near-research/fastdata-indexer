mod scylla_types;

use crate::scylla_types::{add_vote_memo_rows, VoteMemo, INDEXER_ID};
use dotenv::dotenv;
use fastnear_neardata_fetcher::fetcher;
use fastnear_primitives::near_indexer_primitives::types::BlockHeight;
use fastnear_primitives::near_primitives::views::{ActionView, ReceiptEnumView};
use fastnear_primitives::types::ChainId;
use scylladb::ScyllaDb;
use serde_json::Value;
use std::env;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

const PROJECT_ID: &str = "memo-indexer";
const DEFAULT_VOTE_METHOD_NAME: &str = "vote";

#[allow(clippy::too_many_arguments)]
fn parse_vote_memo(
    args: &[u8],
    contract_id: &str,
    voter_id: &str,
    receipt_id: fastnear_primitives::near_indexer_primitives::CryptoHash,
    tx_hash: Option<fastnear_primitives::near_indexer_primitives::CryptoHash>,
    signer_id: fastnear_primitives::near_indexer_primitives::types::AccountId,
    block_height: u64,
    block_timestamp: u64,
    shard_id: u32,
    receipt_index: u32,
    action_index: u32,
) -> Option<VoteMemo> {
    let mut obj: serde_json::Map<String, Value> = serde_json::from_slice(args).ok()?;

    let proposal_id = obj.remove("proposal_id")?.as_u64()? as u32;
    let vote_option = obj.remove("vote")?.as_u64()? as u8;

    let memo = obj.remove("__memo").map(|v| match v {
        Value::String(s) => s,
        _ => v.to_string(),
    });

    // Remove known bulky fields we don't want to store
    obj.remove("merkle_proof");
    obj.remove("v_account");

    let extra_fields = if obj.is_empty() {
        None
    } else {
        serde_json::to_string(&obj).ok()
    };

    let order_id =
        ((shard_id as u64) * 100_000 + receipt_index as u64) * 1_000 + action_index as u64;

    Some(VoteMemo {
        contract_id: contract_id.to_string(),
        proposal_id,
        voter_id: voter_id.parse().ok()?,
        vote_option,
        memo,
        extra_fields,
        receipt_id,
        tx_hash,
        signer_id,
        block_height,
        block_timestamp,
        shard_id,
        receipt_index,
        action_index,
        order_id,
    })
}

#[tokio::main]
async fn main() {
    dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter("memo-indexer=info,scylladb=info")
        .init();

    let chain_id: ChainId = env::var("CHAIN_ID")
        .expect("CHAIN_ID required")
        .try_into()
        .expect("Invalid chain id");

    let vote_contract_ids: Vec<String> = env::var("VOTE_CONTRACT_IDS")
        .expect("VOTE_CONTRACT_IDS required")
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();

    let vote_method_name = env::var("VOTE_METHOD_NAME")
        .unwrap_or_else(|_| DEFAULT_VOTE_METHOD_NAME.to_string());

    let scylla_session = ScyllaDb::new_scylla_session()
        .await
        .expect("Can't create scylla session");

    ScyllaDb::test_connection(&scylla_session)
        .await
        .expect("Can't connect to scylla");

    tracing::info!(target: PROJECT_ID, "Connected to Scylla");

    let scylladb = ScyllaDb::new(chain_id, scylla_session, true)
        .await
        .expect("Can't create scylla db");

    scylla_types::create_tables(&scylladb)
        .await
        .expect("Error creating tables");

    let insert_query = scylla_types::prepare_insert_vote_memo(&scylladb)
        .await
        .expect("Error preparing insert vote memo query");

    let insert_by_voter_query = scylla_types::prepare_insert_vote_memo_by_voter(&scylladb)
        .await
        .expect("Error preparing insert vote memo by voter query");

    let last_processed_block_height = scylladb
        .get_last_processed_block_height(INDEXER_ID)
        .await
        .expect("Error getting last processed block height");

    tracing::info!(target: PROJECT_ID, "Latest processed block height in DB: {:?}", last_processed_block_height);

    let num_threads = env::var("NUM_THREADS")
        .ok()
        .map(|num_threads| num_threads.parse().expect("Invalid number of threads"))
        .unwrap_or(8);

    let client = reqwest::Client::new();
    let last_block_height = fetcher::fetch_last_block(&client, chain_id)
        .await
        .unwrap()
        .block
        .header
        .height;

    tracing::info!(target: PROJECT_ID, "Last neardata block height: {}", last_block_height);

    let start_block_height: BlockHeight = last_processed_block_height
        .map(|h| h + 1)
        .unwrap_or_else(|| {
            env::var("START_BLOCK_HEIGHT")
                .ok()
                .map(|start_block_height| start_block_height.parse().expect("Invalid block height"))
                .unwrap_or(last_block_height)
        });

    let auth_bearer_token = env::var("FASTNEAR_AUTH_BEARER_TOKEN").ok();
    let mut config = fetcher::FetcherConfigBuilder::new()
        .start_block_height(start_block_height)
        .num_threads(num_threads)
        .chain_id(chain_id);
    if let Some(token) = auth_bearer_token.clone() {
        config = config.auth_bearer_token(token);
    }

    let is_running = Arc::new(AtomicBool::new(true));
    let ctrl_c_running = is_running.clone();

    ctrlc::set_handler(move || {
        ctrl_c_running.store(false, Ordering::SeqCst);
        tracing::info!(target: PROJECT_ID, "Received Ctrl+C, starting shutdown...");
    })
    .expect("Error setting Ctrl+C handler");

    let block_update_interval = std::time::Duration::from_millis(
        env::var("BLOCK_UPDATE_INTERVAL_MS")
            .ok()
            .map(|ms| ms.parse().expect("Invalid number of blocks"))
            .unwrap_or(5000),
    );

    tracing::info!(target: PROJECT_ID,
        "Starting {} fetcher with {} threads from height {}. Using auth token: {}. Contracts: {:?}, Method: {}",
        chain_id,
        num_threads,
        start_block_height,
        auth_bearer_token.is_some(),
        vote_contract_ids,
        vote_method_name,
    );

    let (sender, mut receiver) = mpsc::channel((num_threads * 10) as _);
    tokio::spawn(fetcher::start_fetcher(
        config.build(),
        sender,
        is_running.clone(),
    ));

    let mut last_block_update = std::time::SystemTime::now();
    while let Some(block) = receiver.recv().await {
        let block_height = block.block.header.height;
        let block_timestamp = block.block.header.timestamp;
        tracing::info!(target: PROJECT_ID, "Received block: {}", block_height);

        let mut rows = vec![];

        for shard in block.shards {
            for (receipt_index, reo) in shard.receipt_execution_outcomes.into_iter().enumerate() {
                let receipt = reo.receipt;
                let receipt_id = receipt.receipt_id;
                let predecessor_id = receipt.predecessor_id;
                let receiver_id = receipt.receiver_id;
                let tx_hash = reo.tx_hash;
                let receiver_id_str: &str = receiver_id.as_ref();
                let predecessor_id_str: &str = predecessor_id.as_ref();

                if !vote_contract_ids.iter().any(|c| c == receiver_id_str) {
                    continue;
                }

                if let ReceiptEnumView::Action {
                    signer_id, actions, ..
                } = receipt.receipt
                {
                    for (action_index, action) in actions.into_iter().enumerate() {
                        if let ActionView::FunctionCall {
                            method_name, args, ..
                        } = action
                        {
                            if method_name == vote_method_name {
                                if let Some(vote_memo) = parse_vote_memo(
                                    &args,
                                    receiver_id_str,
                                    predecessor_id_str,
                                    receipt_id,
                                    tx_hash,
                                    signer_id.clone(),
                                    block_height,
                                    block_timestamp,
                                    shard.shard_id.into(),
                                    receipt_index as _,
                                    action_index as _,
                                ) {
                                    tracing::info!(target: PROJECT_ID,
                                        "Found vote memo: contract={} proposal={} voter={} vote={} memo={:?}",
                                        vote_memo.contract_id,
                                        vote_memo.proposal_id,
                                        vote_memo.voter_id,
                                        vote_memo.vote_option,
                                        vote_memo.memo,
                                    );
                                    rows.push(vote_memo);
                                }
                            }
                        }
                    }
                }
            }
        }

        let current_time = std::time::SystemTime::now();
        let duration = current_time
            .duration_since(last_block_update)
            .expect("Time went backwards");
        let mut need_to_save_last_processed_block_height = duration >= block_update_interval;

        if !rows.is_empty() {
            tracing::info!(target: PROJECT_ID, "Inserting {} vote memo rows into Scylla", rows.len());
            add_vote_memo_rows(
                &scylladb,
                &insert_query,
                &insert_by_voter_query,
                rows,
                block_height,
            )
            .await
            .expect("Error adding vote memo rows");
            need_to_save_last_processed_block_height = true;
        }

        if !is_running.load(Ordering::SeqCst) {
            tracing::info!(target: PROJECT_ID, "Shutting down fetcher");
            need_to_save_last_processed_block_height = true;
        }

        if need_to_save_last_processed_block_height {
            tracing::info!(target: PROJECT_ID, "Saving last processed block height: {}", block_height);
            scylladb
                .set_last_processed_block_height(INDEXER_ID, block_height)
                .await
                .expect("Error setting last processed block height");
            last_block_update = current_time;
        }

        if !is_running.load(Ordering::SeqCst) {
            break;
        }
    }

    tracing::info!(target: PROJECT_ID, "Successfully shut down");
}

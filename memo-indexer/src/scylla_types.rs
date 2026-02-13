use fastnear_primitives::near_indexer_primitives::types::{AccountId, BlockHeight};
use fastnear_primitives::near_indexer_primitives::CryptoHash;
use scylla::serialize::row::SerializeRow;
use scylla::statement::batch::{Batch, BatchType};
use scylla::statement::prepared::PreparedStatement;
use scylla::{DeserializeRow, SerializeRow};
use scylladb::{ScyllaDb, SCYLLADB};

pub(crate) const INDEXER_ID: &str = "memo-1";

#[derive(Debug, Clone)]
pub struct VoteMemo {
    pub contract_id: String,
    pub proposal_id: u32,
    pub voter_id: AccountId,
    pub vote_option: u8,
    pub memo: Option<String>,
    pub extra_fields: Option<String>,
    pub receipt_id: CryptoHash,
    pub tx_hash: Option<CryptoHash>,
    pub signer_id: AccountId,
    pub block_height: u64,
    pub block_timestamp: u64,
    pub shard_id: u32,
    pub receipt_index: u32,
    pub action_index: u32,
    pub order_id: u64,
}

#[derive(Debug, Clone, DeserializeRow, SerializeRow)]
pub(crate) struct VoteMemoRow {
    pub contract_id: String,
    pub proposal_id: i32,
    pub voter_id: String,
    pub vote_option: i32,
    pub memo: Option<String>,
    pub extra_fields: Option<String>,
    pub receipt_id: String,
    pub tx_hash: Option<String>,
    pub signer_id: String,
    pub block_height: i64,
    pub block_timestamp: i64,
    pub shard_id: i32,
    pub receipt_index: i32,
    pub action_index: i32,
    pub order_id: i64,
}

impl From<VoteMemo> for VoteMemoRow {
    fn from(data: VoteMemo) -> Self {
        Self {
            contract_id: data.contract_id,
            proposal_id: data.proposal_id as i32,
            voter_id: data.voter_id.to_string(),
            vote_option: data.vote_option as i32,
            memo: data.memo,
            extra_fields: data.extra_fields,
            receipt_id: data.receipt_id.to_string(),
            tx_hash: data.tx_hash.map(|h| h.to_string()),
            signer_id: data.signer_id.to_string(),
            block_height: data.block_height as i64,
            block_timestamp: data.block_timestamp as i64,
            shard_id: data.shard_id as i32,
            receipt_index: data.receipt_index as i32,
            action_index: data.action_index as i32,
            order_id: data.order_id as i64,
        }
    }
}

/// Row struct for vote_memos_by_voter table (same fields, different column order for PK).
#[derive(Debug, Clone, DeserializeRow, SerializeRow)]
pub(crate) struct VoteMemoByVoterRow {
    pub voter_id: String,
    pub contract_id: String,
    pub proposal_id: i32,
    pub vote_option: i32,
    pub memo: Option<String>,
    pub extra_fields: Option<String>,
    pub receipt_id: String,
    pub tx_hash: Option<String>,
    pub signer_id: String,
    pub block_height: i64,
    pub block_timestamp: i64,
    pub shard_id: i32,
    pub receipt_index: i32,
    pub action_index: i32,
    pub order_id: i64,
}

impl From<VoteMemoRow> for VoteMemoByVoterRow {
    fn from(row: VoteMemoRow) -> Self {
        Self {
            voter_id: row.voter_id,
            contract_id: row.contract_id,
            proposal_id: row.proposal_id,
            vote_option: row.vote_option,
            memo: row.memo,
            extra_fields: row.extra_fields,
            receipt_id: row.receipt_id,
            tx_hash: row.tx_hash,
            signer_id: row.signer_id,
            block_height: row.block_height,
            block_timestamp: row.block_timestamp,
            shard_id: row.shard_id,
            receipt_index: row.receipt_index,
            action_index: row.action_index,
            order_id: row.order_id,
        }
    }
}

pub(crate) async fn create_tables(scylla_db: &ScyllaDb) -> anyhow::Result<()> {
    let queries = [
        "CREATE TABLE IF NOT EXISTS vote_memos (
            contract_id text,
            proposal_id int,
            voter_id text,
            vote_option int,
            memo text,
            extra_fields text,
            receipt_id text,
            tx_hash text,
            signer_id text,
            block_height bigint,
            block_timestamp bigint,
            shard_id int,
            receipt_index int,
            action_index int,
            order_id bigint,
            PRIMARY KEY ((contract_id, proposal_id), voter_id)
        )",
        "CREATE TABLE IF NOT EXISTS vote_memos_by_voter (
            voter_id text,
            contract_id text,
            proposal_id int,
            vote_option int,
            memo text,
            extra_fields text,
            receipt_id text,
            tx_hash text,
            signer_id text,
            block_height bigint,
            block_timestamp bigint,
            shard_id int,
            receipt_index int,
            action_index int,
            order_id bigint,
            PRIMARY KEY ((voter_id), contract_id, proposal_id)
        )",
    ];
    for query in queries.iter() {
        tracing::debug!(target: SCYLLADB, "Creating table: {}", query);
        scylla_db.scylla_session.query_unpaged(*query, &[]).await?;
    }
    Ok(())
}

pub(crate) async fn prepare_insert_vote_memo(
    scylla_db: &ScyllaDb,
) -> anyhow::Result<PreparedStatement> {
    ScyllaDb::prepare_query(
        &scylla_db.scylla_session,
        "INSERT INTO vote_memos (contract_id, proposal_id, voter_id, vote_option, memo, extra_fields, receipt_id, tx_hash, signer_id, block_height, block_timestamp, shard_id, receipt_index, action_index, order_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        scylla::frame::types::Consistency::LocalQuorum,
    )
    .await
}

pub(crate) async fn prepare_insert_vote_memo_by_voter(
    scylla_db: &ScyllaDb,
) -> anyhow::Result<PreparedStatement> {
    ScyllaDb::prepare_query(
        &scylla_db.scylla_session,
        "INSERT INTO vote_memos_by_voter (voter_id, contract_id, proposal_id, vote_option, memo, extra_fields, receipt_id, tx_hash, signer_id, block_height, block_timestamp, shard_id, receipt_index, action_index, order_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        scylla::frame::types::Consistency::LocalQuorum,
    )
    .await
}

pub(crate) async fn add_vote_memo_rows(
    scylla_db: &ScyllaDb,
    insert_query: &PreparedStatement,
    insert_by_voter_query: &PreparedStatement,
    rows: Vec<VoteMemo>,
    last_processed_block_height: BlockHeight,
) -> anyhow::Result<()> {
    let mut batch = Batch::new(BatchType::Logged);
    let memo_rows = rows
        .into_iter()
        .map(VoteMemoRow::from)
        .collect::<Vec<_>>();
    let by_voter_rows = memo_rows
        .iter()
        .cloned()
        .map(VoteMemoByVoterRow::from)
        .collect::<Vec<_>>();
    for _row in &memo_rows {
        batch.append_statement(insert_query.clone());
    }
    for _row in &by_voter_rows {
        batch.append_statement(insert_by_voter_query.clone());
    }
    batch.append_statement(scylla_db.insert_last_processed_block_height_query.clone());
    let mut values: Vec<&dyn SerializeRow> = vec![];
    for row in &memo_rows {
        values.push(row);
    }
    for row in &by_voter_rows {
        values.push(row);
    }
    let last_processed_block_height_row =
        (INDEXER_ID.to_string(), last_processed_block_height as i64);
    values.push(&last_processed_block_height_row);

    scylla_db.scylla_session.batch(&batch, values).await?;

    Ok(())
}

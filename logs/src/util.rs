use async_raft::NodeId;
use chrono::Utc;
use std::sync::atomic;
use std::sync::atomic::AtomicU64;

const SEQUENCE_BITS: u64 = 12;
const NODE_ID_SHIFT: u64 = SEQUENCE_BITS;
const TIMESTAMP_SHIFT: u64 = SEQUENCE_BITS + 10;

static SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub fn generate_id(node_id: NodeId) -> u64 {
    let now = Utc::now().timestamp_millis() as u64;
    let sequence = SEQUENCE.fetch_add(1, atomic::Ordering::SeqCst) & ((1 << SEQUENCE_BITS) - 1);
    (now << TIMESTAMP_SHIFT) | (node_id << NODE_ID_SHIFT) | sequence
}

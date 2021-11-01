use std::collections::HashMap;
use std::sync::atomic::{AtomicIsize, AtomicBool};
use std::sync::{Arc, Mutex};
use std::cell::RefCell;

use crate::chord_node;
use crate::node_info;
use crate::stabilizer;
use crate::router;
use crate::endpoints;
use crate::data_store;
use crate::chord_util;

type ArMu<T> = Arc<Mutex<T>>;

pub const ID_SPACE_BITS : u32 = 32; // 160 <- sha1での本来の値
pub const ID_SPACE_RANGE : u32 = 0xFFFFFFFF; // 0を含めての数である点に注意
pub const ID_MAX : u32 = 0xFFFFFFFF - 1;

// マスターデータ相当のものは含まない
pub const REPLICA_NUM : u32 = 6;
pub const REPLICA_ID_DISTANCE : u32 =  0xFFFFFFFF / 8;

// successor_info_listに保持するNodeInfoオブジェクトの要素数
// 30ノード規模を想定し、ln(32) = 6 から、6としている
pub const SUCCESSOR_INFO_LIST_LEN : i32 = 6;

// 何回のstabilize_successor呼出しごとにsuccessor_info_list埋めを行うか
pub const FILL_SUCC_LIST_INTERVAL_TIMES : i32 = 5;

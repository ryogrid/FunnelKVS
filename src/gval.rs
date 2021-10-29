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
//pub const ID_SPACE_RANGE : u32 = 2u32.pow(ID_SPACE_BITS); // 0を含めての数である点に注意
pub const ID_SPACE_RANGE : u32 = 0xFFFFFFFF; // 0を含めての数である点に注意
//pub const ID_MAX : u32 = ID_SPACE_RANGE - 1;
pub const ID_MAX : u32 = 0xFFFFFFFF - 1;

// マスターデータ相当のものは含まない
pub const REPLICA_NUM : u32 = 6;
pub const REPLICA_ID_DISTANCE : u32 =  0xFFFFFFFF / 8;
/*
pub const ENABLE_DATA_STORE_OPERATION_DPRINT : bool = false;
pub const ENABLE_ROUTING_INFO_DPRINT : bool = true;
pub const GLOBAL_GET_RETRY_CNT_LIMIT_TO_DEBEUG_PRINT : i32 = 30;
*/

// 検証を分かりやすくするために何ノード目として生成されたか
// のデバッグ用IDを持たせるためのカウンタ
pub static mut already_born_node_num : AtomicIsize = AtomicIsize::new(0);

// デバッグ用の変数群
pub static mut global_get_retry_cnt : AtomicIsize = AtomicIsize::new(0);
pub static mut global_put_retry_cnt : AtomicIsize = AtomicIsize::new(0);

// 既に発行したputの回数
pub static mut already_issued_put_cnt : AtomicIsize = AtomicIsize::new(0);
//pub static mut is_network_constructed : AtomicBool = AtomicBool::new(false);





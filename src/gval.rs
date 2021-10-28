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

/*
// paramaters for executing by PyPy3 on my desktop machine
//pub const JOIN_INTERVAL_SEC : f32 = 1.0; //2.0 //0.9 //0.7 //0.5 //1
pub static mut JOIN_INTERVAL_SEC : AtomicIsize = AtomicIsize::new(1);
pub const PUT_INTERVAL_SEC : f32 = 0.05; //0.5 //0.01 //0.5 # 1
pub const GET_INTERVAL_SEC : f32 = 0.05; //0.5 //0.01 //0.5 //1

// ノード増加の勢いは 係数-1/係数 となる
pub const NODE_KILL_INTERVAL_SEC : f32 = 120.0; //20 #JOIN_INTERVAL_SEC * 10

// 全ノードがstabilize_successorを実行することを1バッチとした際に
// stabilize処理担当のスレッドにより呼び出されるstabilize処理を行わせる
// メソッドの一回の呼び出しで何バッチが実行されるか
pub const STABILIZE_SUCCESSOR_BATCH_TIMES : u32 = 20; //10 //20

// 全ノードがstabilize_finger_tableを複数回呼びされることで、finger_tableの全要素を更新
// することを1バッチとした際に、stabilize処理担当のスレッドにより呼び出されるstabilize処理
// を行わせるメソッドの一回の呼び出しで何バッチが実行されるか
pub const STABILIZE_FTABLE_BATCH_TIMES : i32 = 2; //1
*/

// 一時的にこれより短くなる場合もある
pub const SUCCESSOR_LIST_NORMAL_LEN : i32 = 3;

// 160bit符号なし整数の最大値
// Chordネットワーク上のID空間の上限
// ID_MAX = 2**ID_SPACE_BITS - 1

// 30bit符号なし整数の最大値
// Chordネットワーク上のID空間の上限
// TODO: 検証時の実行時間短縮のためにハッシュ関数で求めた値の代わりに乱数
//       を用いているため bit数 を少なくしている

pub const KEEP_NODE_NUM : i32 = 50; //100
pub const NODE_NUM_MAX : i32 = 50;//10000;
pub const LOCK_ACQUIRE_TIMEOUT : i32 = 3; //10

// stabilize_successorのループの回せる回数の上限
pub const TRYING_GET_SUCC_TIMES_LIMIT : i32 = SUCCESSOR_LIST_NORMAL_LEN * 5;
pub const STABILIZE_THREAD_NUM : i32 = 1; //3 //10
pub const ENABLE_DATA_STORE_OPERATION_DPRINT : bool = false;
pub const ENABLE_ROUTING_INFO_DPRINT : bool = true;
pub const GLOBAL_GET_RETRY_CNT_LIMIT_TO_DEBEUG_PRINT : i32 = 30;

// 検証を分かりやすくするために何ノード目として生成されたか
// のデバッグ用IDを持たせるためのカウンタ
pub static mut already_born_node_num : AtomicIsize = AtomicIsize::new(0);

pub static mut is_network_constructed : AtomicBool = AtomicBool::new(false);

// デバッグ用の変数群
pub static mut global_get_retry_cnt : AtomicIsize = AtomicIsize::new(0);

// 既に発行したputの回数
pub static mut already_issued_put_cnt : AtomicIsize = AtomicIsize::new(0);

// partial_join_opが実行されることを待っているノードが存在するか否か
// join と partial_join_op の間で、該当ノードがkillされることを避けるために用いる
pub static mut is_waiting_partial_join_op_exists : AtomicBool = AtomicBool::new(false);

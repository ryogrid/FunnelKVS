/*
# coding:utf-8

import threading

ID_SPACE_BITS = 30 # 160 <- sha1での本来の値
ID_SPACE_RANGE = 2**ID_SPACE_BITS # 0を含めての数である点に注意

# paramaters for executing by PyPy3 on my desktop machine
JOIN_INTERVAL_SEC = 1.0 #2.0 #0.9 #0.7 # 0.5 # 1
PUT_INTERVAL_SEC = 0.05 #0.5 # 0.01 #0.5 # 1
GET_INTERVAL_SEC = 0.05 #0.5 # 0.01 #0.5 # 1

# ノード増加の勢いは 係数-1/係数 となる
NODE_KILL_INTERVAL_SEC = 120.0 #20 #JOIN_INTERVAL_SEC * 10

# 全ノードがstabilize_successorを実行することを1バッチとした際に
# stabilize処理担当のスレッドにより呼び出されるstabilize処理を行わせる
# メソッドの一回の呼び出しで何バッチが実行されるか
STABILIZE_SUCCESSOR_BATCH_TIMES = 20 #10 #20
# 全ノードがstabilize_finger_tableを複数回呼びされることで、finger_tableの全要素を更新
# することを1バッチとした際に、stabilize処理担当のスレッドにより呼び出されるstabilize処理
# を行わせるメソッドの一回の呼び出しで何バッチが実行されるか
STABILIZE_FTABLE_BATCH_TIMES = 2 #1

# 一時的にこれより短くなる場合もある
SUCCESSOR_LIST_NORMAL_LEN = 3

# # 160bit符号なし整数の最大値
# # Chordネットワーク上のID空間の上限
# ID_MAX = 2**ID_SPACE_BITS - 1

# 30bit符号なし整数の最大値
# Chordネットワーク上のID空間の上限
# TODO: 検証時の実行時間短縮のためにハッシュ関数で求めた値の代わりに乱数
#       を用いているため bit数 を少なくしている
ID_MAX = ID_SPACE_RANGE - 1

KEEP_NODE_NUM = 50 #100
NODE_NUM_MAX = 10000

LOCK_ACQUIRE_TIMEOUT = 3 #10

# プロセス内の全てのデータへのアクセスに対するロック変数
# 実装していく過程で細粒度のロックに対応できていない場合や、デバッグ用途に用いる
lock_of_all_data = threading.Lock()

# TODO: all_node_dictとall_data_listのロックはRustの該当するコレクションがスレッドセーフか
#       確認してから必要なところだけに絞る必要あり（例えば、readアクセスでも結果にセンシティブなところ以外は不要ではないかなど）

# アドレス文字列をキーとしてとり、対応するノードのChordNodeオブジェクトを返すハッシュ
# IPアドレスが分かれば、対応するノードと通信できることと対応している
all_node_dict : Dict[str, 'ChordNode'] = {}
lock_of_all_node_dict = threading.Lock()

# DHT上で保持されている全てのデータが保持されているリスト
# KeyValueオブジェクトを要素として持つ
# 全てのノードはputの際はDHTにデータをputするのとは別にこのリストにデータを追加し、
# getする際はDHTに対してgetを発行するためのデータをこのリストからランダム
# に選び、そのkeyを用いて探索を行う. また value も保持しておき、取得できた内容と
# 照らし合わせられるようにする
all_data_list : List['KeyValue'] = []
lock_of_all_data_list = threading.Lock()

# 検証を分かりやすくするために何ノード目として生成されたか
# のデバッグ用IDを持たせるためのカウンタ
already_born_node_num = 0

is_network_constructed = False

# デバッグ用の変数群
global_get_retry_cnt = 0
GLOBAL_GET_RETRY_CNT_LIMIT_TO_DEBEUG_PRINT = 30

# マスターデータとレプリカの区別なく、データIDをKeyに、当該IDに対応するデータを
# 保持しているノードのリストを得られる dict
all_data_placement_dict : Dict[str, List['NodeInfo']] = {}

# 既に発行したputの回数
already_issued_put_cnt = 0

# stabilize_successorのループの回せる回数の上限
TRYING_GET_SUCC_TIMES_LIMIT = SUCCESSOR_LIST_NORMAL_LEN * 5

# # デバッグ用のフラグ
# # killスレッドがWriter、他のスレッドはReaderとしてロックを取得する
# kill_thread_lock_factory = rwlock.RWLockFairD()
# kill_thread_write_lock = kill_thread_lock_factory.gen_wlock()
# kill_thread_read_lock = kill_thread_lock_factory.gen_rlock()

STABILIZE_THREAD_NUM = 3 #10

ENABLE_DATA_STORE_OPERATION_DPRINT = False
ENABLE_ROUTING_INFO_DPRINT = False

# partial_join_opが実行されることを待っているノードが存在するか否か
# join と partial_join_op の間で、該当ノードがkillされることを避けるために用いる
is_waiting_partial_join_op_exists = False
*/

use std::collections::HashMap;
use std::sync::atomic::{AtomicIsize, AtomicBool};
use std::sync::Arc;
use std::cell::RefCell;
use parking_lot::{ReentrantMutex, const_reentrant_mutex};

use crate::chord_node;
use crate::node_info;
use crate::stabilizer;
use crate::router;
use crate::taskqueue;
use crate::endpoints;
use crate::data_store;
use crate::chord_util;

type ArRmRs<T> = Arc<ReentrantMutex<RefCell<T>>>;

pub const ID_SPACE_BITS : u32 = 30; // 160 <- sha1での本来の値
pub const ID_SPACE_RANGE : u32 = 2i32.pow(ID_SPACE_BITS) as u32; // 0を含めての数である点に注意

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

// 一時的にこれより短くなる場合もある
pub const SUCCESSOR_LIST_NORMAL_LEN : i32 = 3;

// 160bit符号なし整数の最大値
// Chordネットワーク上のID空間の上限
// ID_MAX = 2**ID_SPACE_BITS - 1

// 30bit符号なし整数の最大値
// Chordネットワーク上のID空間の上限
// TODO: 検証時の実行時間短縮のためにハッシュ関数で求めた値の代わりに乱数
//       を用いているため bit数 を少なくしている
pub const ID_MAX : u32 = ID_SPACE_RANGE - 1;
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

//TODO: ジェネリクスで指定している型を適切なものに変更する. GlobalDatas at gval 
pub struct GlobalDatas {
// アドレス文字列をキーとしてとり、対応するノードのChordNodeオブジェクトを返すハッシュ
// IPアドレスが分かれば、対応するノードと通信できることと対応している
    pub all_node_dict : HashMap<String, ArRmRs<chord_node::ChordNode>>,
    // DHT上で保持されている全てのデータが保持されているリスト
    // KeyValueオブジェクトを要素として持つ
    // 全てのノードはputの際はDHTにデータをputするのとは別にこのリストにデータを追加し、
    // getする際はDHTに対してgetを発行するためのデータをこのリストからランダム
    // に選び、そのkeyを用いて探索を行う. また value も保持しておき、取得できた内容と
    // 照らし合わせられるようにする
    pub all_data_list : Vec<ArRmRs<chord_util::KeyValue>>,
    // 以下のxxx_lock_dictは各ノードのaddress_strをキーに ArRmRs<bool>をダミーの
    // ミューテックスとして保持する
    // 各々のdictは各ChordNodeオブジェクトのフィールドに対応しており、あるノード
    // の対応するフィールドの値への参照の取得と関係なしに排他を行うために用いる
    // Rustのロック機構の設計では、柔軟にクリティカルセクションを設定することが
    // 困難であるため導入した
    // クリティカルセクションに入りたいスレッドは、まずHashMapに対応するロック用
    // オブジェクトが格納されているか確認し、存在すれば既に格納されていたものを取得する.
    // 存在しなければ生成し追加する.
    // その後、ロック用オブジェクトのロックを獲得し、排他が必要な処理を行う
    // アドレス文字列でノードを特定しているため、排他を行う対象のオブジェクトの実体は
    // 大本でもコピーされたものでも関係はない
    pub ninfo_lock_dict: HashMap<String, ArRmRs<bool>>,
    pub dstore_lock_dict: HashMap<String, ArRmRs<bool>>,
    pub tqueue_lock_dict: HashMap<String, ArRmRs<bool>>
}

impl GlobalDatas {
    pub fn new() -> GlobalDatas {
        GlobalDatas {
            all_node_dict : HashMap::new(),
            all_data_list : Vec::new(),
            ninfo_lock_dict : HashMap::new(),
            dstore_lock_dict : HashMap::new(),
            tqueue_lock_dict : HashMap::new()
        }
    }
}

lazy_static! {
    pub static ref global_datas : ArRmRs<GlobalDatas> = ArRmRs_new!(GlobalDatas::new());
}

/*
// マスターデータとレプリカの区別なく、データIDをKeyに、当該IDに対応するデータを
// 保持しているノードのリストを得られる dict
all_data_placement_dict : Dict[str, List['NodeInfo']] = {}
*/
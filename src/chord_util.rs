extern crate rand;

use std::sync::{Arc, Mutex};
use std::borrow::{Borrow, BorrowMut};
use std::cell::RefMut;
use std::cell::RefCell;
use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use std::collections::HashMap;

use rand::Rng;
use chrono::{Local, DateTime, Date};
use serde::{Serialize, Deserialize};

use crate::gval;
use crate::chord_node;
use crate::node_info;
use crate::stabilizer;
use crate::router;
use crate::endpoints;
use crate::data_store;

type ArMu<T> = Arc<Mutex<T>>;

// TODO: (rustr)ディープコピーを取得するメソッドを定義しておきたい at DataIdAndValue
#[derive(Serialize, Deserialize)]
#[derive(Debug, Clone)]
pub struct DataIdAndValue {
    pub data_id : u32,
    pub val_str : String
}

impl DataIdAndValue {
    pub fn new(data_id : u32, val_str : String) -> DataIdAndValue {
        DataIdAndValue {data_id : data_id, val_str : val_str}
    }
}

// GeneralError型で利用するエラーコード
pub const ERR_CODE_NOT_IMPLEMENTED : u32 = 0;
pub const ERR_CODE_NODE_IS_DOWNED : u32 = 1;
pub const ERR_CODE_APPROPRIATE_NODE_NOT_FOND : u32 = 2;
pub const ERR_CODE_INTERNAL_CONTROL_FLOW_PROBLEM : u32 = 3;
pub const ERR_CODE_HTTP_REQUEST_ERR : u32 = 4;
pub const ERR_CODE_PRED_IS_NONE: u32 = 5;
pub const ERR_CODE_NOT_TANTOU: u32 = 6;
pub const ERR_CODE_QUERIED_DATA_NOT_FOUND: u32 = 7;
pub const ERR_CODE_DATA_TO_GET_NOT_FOUND: u32 = 8;
pub const ERR_CODE_DATA_TO_GET_IS_DELETED: u32 = 9;

#[derive(Serialize, Deserialize)]
#[derive(Debug, Clone)]
pub struct GeneralError {
    pub message: String,
    pub line : usize,
    pub column: usize,
    pub err_code: u32,
}

impl GeneralError {
    pub fn new(message: String, err_code: u32) -> GeneralError {
        GeneralError {message: message, line: 0, column: 0, err_code: err_code}
    }
}

impl std::fmt::Display for GeneralError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        return write!(f, "({})", self.message);
    }
}

// 0からlimitより1少ない数までの値の乱数を返す
pub fn get_rnd_int_with_limit(limit : u32) -> u32{
    let mut rng = rand::thread_rng(); // 乱数生成器の初期化
    let limit_inner:i32 = limit as i32;
    let rand_val: i32 = rng.gen_range(0..limit_inner);
    return rand_val as u32;
}

// 任意の文字列をハッシュ値（定められたbit数で表現される整数値）に変換しint型で返す
// RustのDefaultHasherでハッシュをとった64bit値の下位32bitを u32 型で返す
pub fn hash_str_to_int(input_str : &String) -> u32 {
    let mut hasher = DefaultHasher::new();
    let str_bytes = input_str.as_bytes();

    for elem in str_bytes{
        hasher.write_u8(*elem);
    }
    
    let hash_val_u64 = hasher.finish();
    let hash_val_u32 = hash_val_u64 as u32;

    return hash_val_u32;
}

pub fn get_unixtime_in_nanos() -> i32{
    let now = SystemTime::now();
    let unixtime = now.duration_since(UNIX_EPOCH).expect("back to the future");
    return unixtime.subsec_nanos() as i32;
}

// UNIXTIME（ナノ秒精度）にいくつか値を加算した値からアドレス文字列を生成する
pub fn gen_address_str() -> String{
    return (get_unixtime_in_nanos() + 10).to_string();
}

pub fn overflow_check_and_conv(id : u64) -> u32 {
    let mut ret_id = id;
    if id > gval::ID_MAX as u64 {
        // 1を足すのは MAX より 1大きい値が 0 となるようにするため
        ret_id = id - ((gval::ID_MAX + 1) as u64);
    }
    return ret_id as u32;
}

pub fn conv_id_to_ratio_str(id : u32) -> String {
    let ratio = (id as f64 / gval::ID_MAX as f64) * 100.0;
    return format!("{:.4}", ratio);
}

pub fn calc_distance_between_nodes_left_mawari(base_id : u32, target_id : u32) -> u32 {
    // successorが自分自身である場合に用いられる場合を考慮し、base_id と target_id が一致する場合は
    // 距離0と考えることもできるが、一周分を距離として返す
    if base_id == target_id {
        //return (gval::ID_SPACE_RANGE - 1) as u32;
        return gval::ID_SPACE_RANGE;
    }

    // 0をまたいだ場合に考えやすくするためにtarget_idを0にずらしたと考えて、
    // base_idを同じ数だけずらす
    let mut slided_base_id = base_id as i64 - target_id as i64;
    if slided_base_id < 0 {
        // マイナスの値をとった場合は値0を通り越しているので
        // それにあった値に置き換える
        slided_base_id = (gval::ID_MAX as i64) + (slided_base_id as i64);
    }

    // 0を跨いだ場合の考慮はされているのであとは単純に値の大きな方から小さな方との差
    // が結果となる. ここでは slided_target_id は 0 であり、slided_base_id は必ず正の値
    // となっているので、 slided_base_idの値を返せばよい

    return slided_base_id as u32;
}

pub fn calc_distance_between_nodes_right_mawari(base_id : u32, target_id : u32) -> u32 {
    // successorが自分自身である場合に用いられる場合を考慮し、base_id と target_id が一致する場合は
    // 距離0と考えることもできるが、一周分を距離として返す
    if base_id == target_id {
        //return gval::ID_SPACE_RANGE - 1;
        return gval::ID_SPACE_RANGE;
    }

    // 0をまたいだ場合に考えやすくするためにbase_idを0にずらしたと考えて、target_idを
    // 同じ数だけずらす
    let mut slided_target_id = (target_id as i64) - (base_id as i64);
    if slided_target_id < 0 {
        // マイナスの値をとった場合は値0を通り越しているので
        // それにあった値に置き換える
        slided_target_id = (gval::ID_MAX as i64) + (slided_target_id as i64);
    }

    // 0を跨いだ場合の考慮はされているので、あとは単純に値の大きな方から小さな方との差
    // が結果となる. ここでは slided_base_id は 0 であり、slided_target_id は必ず正の値
    // となっているので、 slided_target_idの値を返せばよい

    return slided_target_id as u32;
}

pub fn exist_between_two_nodes_right_mawari(from_id : u32, end_id : u32, target_id : u32) -> bool { 
    let distance_end = calc_distance_between_nodes_right_mawari(from_id, end_id);
    let distance_target = calc_distance_between_nodes_right_mawari(from_id, target_id);

    if distance_target < distance_end {
        return true;
    } else {
        return false;
    }
}

// TODO: (rustr) グローバル定数を見て、ファイルに書き出すフラグが立っていたら、ファイルに書くようにする (dprint)
//               スレッドセーフなロガーライブラリを採用する必要がありそう？？？
//               最初は3ノードで動作確認をするので、その時点ではstdoutに書き出す形で問題ない
pub fn dprint(print_str : &String) {
    let local = Local::now();
    let local_naive = local.naive_local();
    println!("{:?},{}", local_naive, print_str);
}

pub fn gen_debug_str_of_node(node_info : &node_info::NodeInfo) -> String {
    return node_info.born_id.to_string() + &",".to_string() + &format!("{:X}", node_info.node_id) + &",".to_string()
       + &conv_id_to_ratio_str(node_info.node_id);
}

pub fn gen_debug_str_of_data(data_id : u32) -> String {
    return format!("{:X}", data_id) + &",".to_string() + &conv_id_to_ratio_str(data_id);
}

pub fn get_node_info(self_node: ArMu<node_info::NodeInfo>, client_pool: ArMu<HashMap<String, ArMu<reqwest::Client>>>) -> node_info::NodeInfo {
    let self_node_ref = self_node.lock().unwrap();
    //let ret = node_info::partial_clone_from_ref_strong_without_ftable(&self_node_ref);
    let ret = node_info::partial_clone_from_ref_strong(&self_node_ref);
    return ret;
}

pub fn iv_clone_from_ref(iv_ref: &DataIdAndValue) -> DataIdAndValue {
    return DataIdAndValue::new(iv_ref.data_id, iv_ref.val_str.clone());
}

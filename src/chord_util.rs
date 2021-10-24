extern crate rand;

use std::sync::{Arc, Mutex};
use std::borrow::{Borrow, BorrowMut};
use std::cell::RefMut;
use std::cell::RefCell;
use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;

use parking_lot::{ReentrantMutex, const_reentrant_mutex};
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

//type ArRmRs<T> = Arc<ReentrantMutex<RefCell<T>>>;
type ArMu<T> = Arc<Mutex<T>>;

// TODO: (rustr)ディープコピーを取得するメソッドを定義しておきたい at DataIdAndValue
#[derive(Serialize, Deserialize)]
#[derive(Debug, Clone)]
pub struct DataIdAndValue {
    pub data_id : i32,
    pub value_data : String
}

impl DataIdAndValue {
    pub fn new(data_id : i32, value_data : String) -> DataIdAndValue {
        DataIdAndValue {data_id : data_id, value_data : value_data}
    }
}

// GeneralError型で利用するエラーコード
pub const ERR_CODE_NODE_IS_DOWNED : u32 = 1;
pub const ERR_CODE_APPROPRIATE_NODE_NOT_FOND : u32 = 2;
pub const ERR_CODE_INTERNAL_CONTROL_FLOW_PROBLEM : u32 = 3;
pub const ERR_CODE_HTTP_REQUEST_ERR : u32 = 4;

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
/*    
    // TODO: ID_SPACE_BITS ビットで表現できる符号なし整数をID空間とする.
    //       通常、ID_SPACE_BITS は sha1 で 160 となるが、この検証コードでは
    //       ハッシュ関数を用いなくても問題の起きない実装となっているため、より小さい
    //       ビット数で表現可能な IDスペース 内に収まる値を乱数で求めて返す
    let rand_val: u32 = get_rnd_int_with_limit(gval::ID_SPACE_RANGE);
    return rand_val;
*/
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
/*
# 計算したID値がID空間の最大値を超えていた場合は、空間内に収まる値に変換する
@classmethod
def overflow_check_and_conv(cls, id : int) -> int:
    ret_id = id
    if id > gval.ID_MAX:
        # 1を足すのは MAX より 1大きい値が 0 となるようにするため
        ret_id = id - (gval.ID_MAX + 1)
    return ret_id
*/

pub fn conv_id_to_ratio_str(id : u32) -> String {
    let ratio = (id as f64 / gval::ID_MAX as f64) * 100.0;
    return format!("{:.4}", ratio);
}
/*
# idがID空間の最大値に対して何パーセントの位置かを適当な精度の浮動小数の文字列
# にして返す
@classmethod
def conv_id_to_ratio_str(cls, id : int) -> str:
    ratio = (id / gval.ID_MAX) * 100.0
    return '%2.4f' % ratio
*/

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
/*
# ID空間が環状になっていることを踏まえて base_id から前方をたどった場合の
# ノード間の距離を求める
# ここで前方とは、IDの値が小さくなる方向である
@classmethod
def calc_distance_between_nodes_left_mawari(cls, base_id : int, target_id : int) -> int:
    # successorが自分自身である場合に用いられる場合を考慮し、base_id と target_id が一致する場合は
    # 距離0と考えることもできるが、一周分を距離として返す
    if base_id == target_id:
        return gval.ID_SPACE_RANGE - 1

    # 0をまたいだ場合に考えやすくするためにtarget_idを0にずらし、base_idを
    # 同じ数だけずらす
    slided_target_id = 0
    slided_base_id = base_id - target_id
    if(slided_base_id < 0):
        # マイナスの値をとった場合は値0を通り越しているので
        # それにあった値に置き換える
        slided_base_id = gval.ID_MAX + slided_base_id

    # 0を跨いだ場合の考慮はされているのであとは単純に値の大きな方から小さな方との差
    # が結果となる. ここでは slided_target_id は 0 であり、slided_base_id は必ず正の値
    # となっているので、 slided_base_idの値を返せばよい

    return slided_base_id
*/

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

    // 0を跨いだ場合の考慮はされているのであとは単純に値の大きな方から小さな方との差
    // が結果となる. ここでは slided_base_id は 0 であり、slided_target_id は必ず正の値
    // となっているので、 slided_base_idの値を返せばよい

    return slided_target_id as u32;
}
/*
# ID空間が環状になっていることを踏まえて base_id から後方をたどった場合の
# ノード間の距離を求める
# ここで後方とは、IDの値が大きくなる方向である
@classmethod
def calc_distance_between_nodes_right_mawari(cls, base_id : int, target_id : int) -> int:
    # successorが自分自身である場合に用いられる場合を考慮し、base_id と target_id が一致する場合は
    # 距離0と考えることもできるが、一周分を距離として返す
    if base_id == target_id:
        return gval.ID_SPACE_RANGE - 1

    # 0をまたいだ場合に考えやすくするためにtarget_idを0にずらし、base_idを
    # 同じ数だけずらす
    slided_target_id = target_id - base_id
    if(slided_target_id < 0):
        # マイナスの値をとった場合は値0を通り越しているので
        # それにあった値に置き換える
        slided_target_id = gval.ID_MAX + slided_target_id

    # 0を跨いだ場合の考慮はされているのであとは単純に値の大きな方から小さな方との差
    # が結果となる. ここでは slided_base_id は 0 であり、slided_target_id は必ず正の値
    # となっているので、 slided_base_idの値を返せばよい

    return slided_target_id
*/

pub fn exist_between_two_nodes_right_mawari(from_id : u32, end_id : u32, target_id : u32) -> bool { 
    let distance_end = calc_distance_between_nodes_right_mawari(from_id, end_id);
    let distance_target = calc_distance_between_nodes_right_mawari(from_id, target_id);

    if distance_target < distance_end {
        return true;
    } else {
        return false;
    }
}
/*
# from_id から IDが大きくなる方向にたどった場合に、 end_id との間に
# target_idが存在するか否かを bool値で返す
@classmethod
def exist_between_two_nodes_right_mawari(cls, from_id : int, end_id : int, target_id : int) -> bool:
    distance_end = ChordUtil.calc_distance_between_nodes_right_mawari(from_id, end_id)
    distance_target = ChordUtil.calc_distance_between_nodes_right_mawari(from_id, target_id)

    if distance_target < distance_end:
        return True
    else:
        return False
*/

// TODO: (rustr) グローバル定数を見て、ファイルに書き出すフラグが立っていたら、ファイルに書くようにする (dprint)
//               スレッドセーフなロガーライブラリを採用する必要がありそう？？？
//               最初は3ノードで動作確認をするので、その時点ではstdoutに書き出す形で問題ない
pub fn dprint(print_str : &String) {
    let local = Local::now();
    let local_naive = local.naive_local();
    println!("{:?},{}", local_naive, print_str);
}
/*
# TODO: マルチプロセス安全ないしそれに近いものにする必要あり dprint
@classmethod
def dprint(cls, print_str : str, flush=False):
    print(str(datetime.datetime.now()) + "," + print_str, flush=flush)
*/

pub fn gen_debug_str_of_node(node_info : &node_info::NodeInfo) -> String {
    return node_info.born_id.to_string() + &",".to_string() + &format!("{:X}", node_info.node_id) + &",".to_string()
       + &conv_id_to_ratio_str(node_info.node_id);
}
/* 
    @classmethod
    def gen_debug_str_of_node(cls, node_info : Optional['NodeInfo']) -> str:
        casted_info : 'NodeInfo' = cast('NodeInfo', node_info)
        return str(casted_info.born_id) + "," + hex(casted_info.node_id) + "," \
               + ChordUtil.conv_id_to_ratio_str(casted_info.node_id)
*/

pub fn gen_debug_str_of_data(data_id : u32) -> String {
    return format!("{:X}", data_id) + &",".to_string() + &conv_id_to_ratio_str(data_id);
}
/*
    @classmethod
    def gen_debug_str_of_data(cls, data_id : int) -> str:
        return hex(data_id) + "," + ChordUtil.conv_id_to_ratio_str(data_id)
*/

pub fn get_node_info(self_node: ArMu<node_info::NodeInfo>) -> node_info::NodeInfo {
    let self_node_ref = self_node.lock().unwrap();
    let ret = node_info::partial_clone_from_ref_strong(&self_node_ref);
    return ret;
}

/*
// Attention: InternalControlFlowException を raiseする場合がある
// TODO: 実システム化する際は アドレス指定で呼び出せる（ChordNodeオブジェクトのメソッドという形でない）
//       RPC化する必要がありそう。もしくはこのメソッドの呼び出し自体を無くすか。 is_node_alive
pub fn is_node_alive(address : &String) -> Result<bool, GeneralError> {
    // TODO: (rustr) 現状は故障ノードを想定しないため必ずtrueを返す
    return Ok(true);

    // let tmp = get_node_by_address(address);
    // match tmp {
    //     Ok(arc_val) => return Ok(true),
    //     Err(err) => {
    //         if err.err_code == ERR_CODE_INTERNAL_CONTROL_FLOW_PROBLEM {
    //             return Err(err);
    //         }else{ // ERR_CODE_NODE_IS_DOWNED
    //             return Ok(false);
    //         }
    //     }
    // }
}
*/

/*
    # TODO: InternalExp at is_node_alive

    # Attention: InternalControlFlowException を raiseする場合がある
    # TODO: 実システム化する際は アドレス指定で呼び出せる（ChordNodeオブジェクトのメソッドという形でない）
    #       RPC化する必要がありそう。もしくはこのメソッドの呼び出し自体を無くすか。 is_node_alive
    @classmethod
    def is_node_alive(cls, address : str) -> PResult[Optional[bool]]:
        ret = ChordUtil.get_node_by_address(address)
        if(ret.is_ok):
            return PResult.Ok(True)
        else:
            if ret.err_code == ErrorCode.NodeIsDownedException_CODE:
                return PResult.Ok(False)
            else: #ret.err_code == ErrorCode.InternalControlFlowException_CODE:
                return PResult.Err(False, ErrorCode.InternalControlFlowException_CODE)

        #return True
*/
/*
# coding:utf-8

import sys
import time
import random
import datetime
import dataclasses
import traceback
from . import gval

class ErrorCode:
    KeyError_CODE = 1
    NodeIsDownedException_CODE = 2
    AppropriateNodeNotFoundException_CODE = 3
    InternalControlFlowException_CODE = 4

T = TypeVar('T')

class PResult(Generic[T]):

    @classmethod
    def Ok(cls, result: T) -> 'PResult[T]':
        return PResult[T](result, True)

    @classmethod
    def Err(cls, result: T,  err_code : int) -> 'PResult[T]':
        return PResult[T](result, False, err_code = err_code)

    def __init__(self, result: T, is_ok: bool, err_code = None):
        self.result : T = result
        self.err_code : Optional[int] = err_code
        self.is_ok : bool = is_ok

class ChordUtil:
    # 任意の文字列をハッシュ値（定められたbit数で表現される整数値）に変換しint型で返す
    # アルゴリズムはSHA1, 160bitで表現される正の整数となる
    # メモ: 10進数の整数は組み込みの hex関数で 16進数表現での文字列に変換可能
    # TODO: 本来のハッシュ関数に戻す必要あり hash_str_to_int
    @classmethod
    def hash_str_to_int(cls, input_str : str) -> int:
        # hash_hex_str = hashlib.sha1(input_str.encode()).hexdigest()
        # hash_id_num = int(hash_hex_str, 16)

        # TODO: ID_SPACE_BITS ビットで表現できる符号なし整数をID空間とする.
        #       通常、ID_SPACE_BITS は sha1 で 160 となるが、この検証コードでは
        #       ハッシュ関数を用いなくても問題の起きない実装となっているため、より小さい
        #       ビット数で表現可能な IDスペース 内に収まる値を乱数で求めて返す
        hash_id_num = random.randint(0, gval.ID_SPACE_RANGE - 1)
        return hash_id_num

    # 与えたリストの要素のうち、ランダムに選択した1要素を返す
    @classmethod
    def get_random_elem(cls, list_like : List[Any]) -> Any:
        length = len(list_like)
        idx = random.randint(0, length - 1)
        return list_like[idx]

    @classmethod
    def get_random_data(cls) -> 'KeyValue':
        # with gval.lock_of_all_data_list:
        return ChordUtil.get_random_elem(gval.all_data_list)

    # UNIXTIME（ミリ秒精度）にいくつか値を加算した値からアドレス文字列を生成する
    @classmethod
    def gen_address_str(cls) -> str:
        return str(time.time() + 10)

    # 計算したID値がID空間の最大値を超えていた場合は、空間内に収まる値に変換する
    @classmethod
    def overflow_check_and_conv(cls, id : int) -> int:
        ret_id = id
        if id > gval.ID_MAX:
            # 1を足すのは MAX より 1大きい値が 0 となるようにするため
            ret_id = id - (gval.ID_MAX + 1)
        return ret_id

    # idがID空間の最大値に対して何パーセントの位置かを適当な精度の浮動小数の文字列
    # にして返す
    @classmethod
    def conv_id_to_ratio_str(cls, id : int) -> str:
        ratio = (id / gval.ID_MAX) * 100.0
        return '%2.4f' % ratio

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

    # TODO: マルチプロセス安全ないしそれに近いものにする必要あり dprint
    @classmethod
    def dprint(cls, print_str : str, flush=False):
        print(str(datetime.datetime.now()) + "," + print_str, flush=flush)

    @classmethod
    def print_no_lf(cls, print_str : str):
        print(print_str, end="")

    @classmethod
    def gen_debug_str_of_node(cls, node_info : Optional['NodeInfo']) -> str:
        casted_info : 'NodeInfo' = cast('NodeInfo', node_info)
        return str(casted_info.born_id) + "," + hex(casted_info.node_id) + "," \
               + ChordUtil.conv_id_to_ratio_str(casted_info.node_id)

    @classmethod
    def gen_debug_str_of_data(cls, data_id : int) -> str:
        return hex(data_id) + "," + ChordUtil.conv_id_to_ratio_str(data_id)

    # TODO: InteernalExp, DownedeExp at get_node_by_address

    # Attention: 取得しようとしたノードが all_node_dict に存在しないことは、そのノードが 離脱（ダウンしている状態も含）
    #            したことを意味するため、当該状態に対応する NodeIsDownedException 例外を raise する
    # TODO: 実システム化する際は rpcで生存チェックをした上で、rpcで取得した情報からnode_info プロパティの値だけ適切に埋めた
    #       ChordNodeオブジェクトを返す get_node_by_address
    @classmethod
    def get_node_by_address(cls, address : str) -> PResult[Optional['ChordNode']]:
        try:
            # with gval.lock_of_all_node_dict:
            ret_val = gval.all_node_dict[address]
        except KeyError:
            # join処理の途中で構築中のノード情報を取得しようとしてしまった場合に発生する
            # traceback.print_stack(file=sys.stdout)
            # print("KeyError occured", flush=True)

            return PResult.Err(None, ErrorCode.InternalControlFlowException_CODE)

        if ret_val.is_alive == False:
            ChordUtil.dprint("get_node_by_address_1,NODE_IS_DOWNED," + ChordUtil.gen_debug_str_of_node(ret_val.node_info))
            return PResult.Err(None, ErrorCode.NodeIsDownedException_CODE)

        return PResult.Ok(ret_val)

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


    # デバッグ用のメソッド
    # グローバル変数にあるデータIDに対応するデータがどのノードに存在するかを記録する
    # 本メソッドは新たにデータをstoreした際に呼び出す
    @classmethod
    def add_data_placement_info(cls, data_id : int, node_info : 'NodeInfo'):
        try:
            node_list : List['NodeInfo'] = gval.all_data_placement_dict[str(data_id)]
        except KeyError:
            node_list = []
            gval.all_data_placement_dict[str(data_id)] = node_list

        # 既に引数で指定されたノードでの存在が記録されていた場合、同じノードのエントリが
        # 重複してしまうので追加せずに終了する
        if node_info in node_list:
            return

        node_list.append(node_info)

    # デバッグ用のメソッド
    # グローバル変数にあるデータIDに対応するデータがどのノードに存在するかを記録する
    # 本メソッドはデータの削除が行われた際に呼び出す
    @classmethod
    def remove_data_placement_info(cls, data_id : int, node_info : 'NodeInfo'):
        try:
            node_list : List['NodeInfo'] = gval.all_data_placement_dict[str(data_id)]
        except KeyError:
            # 本来は起きてはならないエラーだが対処のし様もないのでワーニングを出力しておく
            ChordUtil.dprint("remove_data_1," + ChordUtil.gen_debug_str_of_node(node_info) + ","
                             + ChordUtil.gen_debug_str_of_data(data_id)
                             + ",WARNING__DATA_AND_BELONGS_NODE_RERATION_MAY_BE_BROKEN")
            return

        node_list.remove(node_info)

    # デバッグ用のメソッド
    # グローバル変数にあるデータIDに対応するデータがどのノードに存在するかを出力する
    # 本メソッドはglobal_getが行われた際に呼び出す
    @classmethod
    def print_data_placement_info(cls, data_id : int, after_notfound_limit = False):
        try:
            node_list : List['NodeInfo'] = gval.all_data_placement_dict[str(data_id)]
        except KeyError:
            # データを持っているノードがいないか、記録のバグ
            ChordUtil.dprint("print_data_placement_info_1,"
                             + ChordUtil.gen_debug_str_of_data(data_id)
                             + ",DATA_HAVING_NODE_DOES_NOT_EXIST_OR_INFORMATION_BUG")
            return

        if after_notfound_limit:
            additional_str = "NOT_FOUND_LIMIT_REACHED,"
        else:
            additional_str = ""

        # ロックをとっていないので面倒な処理が頭に入っている
        # なお、処理中に node_list の要素が増えた場合や出力済みのデータが削除された場合は
        # 表示に不整合が生じるが大きな問題ではない認識
        list_len = len(node_list)
        for idx in range(0, list_len):
            if idx < len(node_list):
                ChordUtil.dprint("print_data_placement_info_INFO," + additional_str
                                 + ChordUtil.gen_debug_str_of_data(data_id) + ","
                                 + ChordUtil.gen_debug_str_of_node(node_list[idx]))

    @classmethod
    def dprint_data_storage_operations(cls, callee_node : 'NodeInfo', operation_type : str, data_id : int):
        if gval.ENABLE_DATA_STORE_OPERATION_DPRINT == False:
            return
        ChordUtil.dprint("dprint_data_storage_operations," + ChordUtil.gen_debug_str_of_node(callee_node) + ","
                         + operation_type + "," + ChordUtil.gen_debug_str_of_data(data_id))

    @classmethod
    def dprint_routing_info(cls, callee_node : 'ChordNode', calee_method : str):
        if gval.ENABLE_ROUTING_INFO_DPRINT == False:
            return
        ChordUtil.dprint("dprint_routing_info__PRED," + ChordUtil.gen_debug_str_of_node(callee_node.node_info) + ","
                         + calee_method + "," + "PREDECESSOR_INFO," + str(callee_node.node_info.predecessor_info))
        ChordUtil.dprint("dprint_routing_info__SUCC," +ChordUtil.gen_debug_str_of_node(callee_node.node_info) + "," + calee_method + ","
                         + "SUCCESSOR_INFO_LIST," + str(len(callee_node.node_info.successor_info_list)) + ","
                         + " ,| ".join([str(ninfo)  for ninfo in callee_node.node_info.successor_info_list]))

# all_data_listグローバル変数に格納される形式としてのみ用いる
class KeyValue:
    def __init__(self, key : Optional[str], value : str):
        self.key : Optional[str] = key
        self.value_data : str = value
        self.data_id : Optional[int] = None
        # keyのハッシュ値
        if key == None:
            self.data_id = None
        else:
            self.data_id = ChordUtil.hash_str_to_int(cast(str, key))

    def __eq__(self, other):
        if not isinstance(other, KeyValue):
            return False
        return self.data_id == other.data_id

# TODO: ディープコピーを取得するメソッドを定義しておきたい at DataIdAndValue
@dataclasses.dataclass
class DataIdAndValue:
    data_id : int
    value_data : str

    def __eq__(self, other):
        if not isinstance(other, DataIdAndValue):
            return False
        return self.data_id == other.data_id
*/
extern crate rand;

use std::sync::Arc;
use std::borrow::{Borrow, BorrowMut};
use std::cell::RefMut;
use std::cell::RefCell;
use std::sync::atomic::Ordering;

use parking_lot::{ReentrantMutex, const_reentrant_mutex};
use rand::Rng;
use chrono::{Local, DateTime, Date};

use crate::gval;
use crate::chord_node;
use crate::node_info;
use crate::stabilizer;
use crate::router;
use crate::taskqueue;
use crate::endpoints;
use crate::data_store;

type ArRmRs<T> = Arc<ReentrantMutex<RefCell<T>>>;

// all_data_listグローバル変数に格納される形式としてのみ用いる
#[derive(Debug, Clone)]
pub struct KeyValue {
    pub key : Option<String>,
    pub value_data : String,
    pub data_id : Option<i32>
}

impl KeyValue {
    pub fn new(key : Option<String>, value : String) -> KeyValue {
        let tmp_data_id : Option<i32> = match &key {
            Some(key_string) => Some(hash_str_to_int(key_string)),
            None => None
        };
        KeyValue {key : Some(key.unwrap()), value_data : value, data_id : tmp_data_id}
    }
}
/*
    def __eq__(self, other):
        if not isinstance(other, KeyValue):
            return False
        return self.data_id == other.data_id
*/

// TODO: ディープコピーを取得するメソッドを定義しておきたい at DataIdAndValue
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
/*
    def __eq__(self, other):
        if not isinstance(other, DataIdAndValue):
            return False
        return self.data_id == other.data_id
*/

/*
#[derive(Debug, Clone)]
pub struct NodeIsDownedError {
    pub message: String,
    pub line : usize,
    pub column: usize,
}

#[derive(Debug, Clone)]
pub struct AppropriateNodeNotFoundError {
    pub message: String,
    pub line : usize,
    pub column: usize,
}

#[derive(Debug, Clone)]
pub struct InternalControlFlowError {
    pub message: String,
    pub line : usize,
    pub column: usize,
}
*/

// GeneralError型で利用するエラーコード
pub const ERR_CODE_NODE_IS_DOWNED : u32 = 1;
pub const ERR_CODE_APPROPRIATE_NODE_NOT_FOND : u32 = 2;
pub const ERR_CODE_INTERNAL_CONTROL_FLOW_PROBLEM : u32 = 3;

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
pub fn get_rnd_int_with_limit(limit : i32) -> i32{
    let mut rng = rand::thread_rng(); // 乱数生成器の初期化
    let rand_val: i32 = rng.gen_range(0..limit);
    return rand_val;
}

// 任意の文字列をハッシュ値（定められたbit数で表現される整数値）に変換しint型で返す
// アルゴリズムはSHA1, 160bitで表現される正の整数となる
// メモ: 10進数の整数は組み込みの hex関数で 16進数表現での文字列に変換可能
// TODO: 本来のハッシュ関数に戻す必要あり hash_str_to_int
pub fn hash_str_to_int(_input_str : &String) -> i32 {
    // hash_hex_str = hashlib.sha1(input_str.encode()).hexdigest()
    // hash_id_num = int(hash_hex_str, 16)

    // TODO: ID_SPACE_BITS ビットで表現できる符号なし整数をID空間とする.
    //       通常、ID_SPACE_BITS は sha1 で 160 となるが、この検証コードでは
    //       ハッシュ関数を用いなくても問題の起きない実装となっているため、より小さい
    //       ビット数で表現可能な IDスペース 内に収まる値を乱数で求めて返す
    let rand_val: i32 = get_rnd_int_with_limit(gval::ID_SPACE_RANGE);
    return rand_val;
}

pub fn overflow_check_and_conv(id : i32) -> i32 {
    let mut ret_id = id;
    if id > gval::ID_MAX {
        // 1を足すのは MAX より 1大きい値が 0 となるようにするため
        ret_id = id - (gval::ID_MAX + 1);
    }
    return ret_id;
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

pub fn conv_id_to_ratio_str(id : i32) -> String {
    let ratio = (id / gval::ID_MAX) as f32 * 100.0;
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

pub fn calc_distance_between_nodes_left_mawari(base_id : i32, target_id : i32) -> i32 {
    // successorが自分自身である場合に用いられる場合を考慮し、base_id と target_id が一致する場合は
    // 距離0と考えることもできるが、一周分を距離として返す
    if base_id == target_id {
        return gval::ID_SPACE_RANGE - 1;
    }

    // 0をまたいだ場合に考えやすくするためにtarget_idを0にずらし、base_idを
    // 同じ数だけずらす
    let mut slided_base_id = base_id - target_id;
    if slided_base_id < 0 {
        // マイナスの値をとった場合は値0を通り越しているので
        // それにあった値に置き換える
        slided_base_id = gval::ID_MAX + slided_base_id
    }

    // 0を跨いだ場合の考慮はされているのであとは単純に値の大きな方から小さな方との差
    // が結果となる. ここでは slided_target_id は 0 であり、slided_base_id は必ず正の値
    // となっているので、 slided_base_idの値を返せばよい

    return slided_base_id
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

pub fn calc_distance_between_nodes_right_mawari(base_id : i32, target_id : i32) -> i32 {
    // successorが自分自身である場合に用いられる場合を考慮し、base_id と target_id が一致する場合は
    // 距離0と考えることもできるが、一周分を距離として返す
    if base_id == target_id {
        return gval::ID_SPACE_RANGE - 1;
    }

    // 0をまたいだ場合に考えやすくするためにtarget_idを0にずらし、base_idを
    // 同じ数だけずらす
    let mut slided_target_id = target_id - base_id;
    if slided_target_id < 0 {
        // マイナスの値をとった場合は値0を通り越しているので
        // それにあった値に置き換える
        slided_target_id = gval::ID_MAX + slided_target_id;
    }

    // 0を跨いだ場合の考慮はされているのであとは単純に値の大きな方から小さな方との差
    // が結果となる. ここでは slided_base_id は 0 であり、slided_target_id は必ず正の値
    // となっているので、 slided_base_idの値を返せばよい

    return slided_target_id
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

pub fn exist_between_two_nodes_right_mawari(from_id : i32, end_id : i32, target_id : i32) -> bool { 
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

pub fn gen_debug_str_of_node(node_info : Option<&node_info::NodeInfo>) -> String {
    let casted_info = node_info.unwrap();
    return casted_info.born_id.to_string() + &",".to_string() + &format!("{:X}", casted_info.node_id) + &",".to_string()
       + &conv_id_to_ratio_str(casted_info.node_id);
}
/* 
    @classmethod
    def gen_debug_str_of_node(cls, node_info : Optional['NodeInfo']) -> str:
        casted_info : 'NodeInfo' = cast('NodeInfo', node_info)
        return str(casted_info.born_id) + "," + hex(casted_info.node_id) + "," \
               + ChordUtil.conv_id_to_ratio_str(casted_info.node_id)
*/

pub fn gen_debug_str_of_data(data_id : i32) -> String {
    return format!("{:X}", data_id) + &",".to_string() + &conv_id_to_ratio_str(data_id);
}
/*
    @classmethod
    def gen_debug_str_of_data(cls, data_id : int) -> str:
        return hex(data_id) + "," + ChordUtil.conv_id_to_ratio_str(data_id)
*/


// Attention: 取得しようとしたノードが all_node_dict に存在しないことは、そのノードが 離脱（ダウンしている状態も含）
//            したことを意味するため、当該状態に対応する NodeIsDownedException 例外を raise する
// TODO: 実システム化する際は rpcで生存チェックをした上で、rpcで取得した情報からnode_info プロパティの値だけ適切に埋めた
//       ChordNodeオブジェクトを返す get_node_by_address
pub fn get_node_by_address(address : &String) -> Result<Option<ArRmRs<chord_node::ChordNode>>, GeneralError> {
    let gd_refcell = get_refcell_from_arc_with_locking!(gval::global_datas);
    let gd_ref = get_ref_from_refcell!(gd_refcell);

    let get_result = gd_ref.all_node_dict.get(address);
    let ret_val = 
        match get_result {
            // join処理の途中で構築中のノード情報を取得しようとしてしまった場合に発生する
            None => return Err(GeneralError::new("".to_string(), ERR_CODE_INTERNAL_CONTROL_FLOW_PROBLEM)),
            Some(arc_val) => arc_val,
        };


    let node_refcell = get_refcell_from_arc_with_locking!(ret_val);
    let node_ref = get_ref_from_refcell!(node_refcell);

    let callee_ninfo_refcell = get_refcell_from_arc_with_locking!(node_ref.node_info);
    let callee_ninfo_ref = get_ref_from_refcell!(callee_ninfo_refcell);

    if node_ref.is_alive.load(Ordering::Relaxed) == false {
        dprint(&("get_node_by_address_1,NODE_IS_DOWNED,".to_string() + &gen_debug_str_of_node(Some(callee_ninfo_ref))));
        return Err(GeneralError::new("".to_string(), ERR_CODE_NODE_IS_DOWNED));
    }

    return Ok(Some(Arc::clone(ret_val)));
}

/*
    # TODO: InteernalExp, DownedeExp at get_node_by_address

    # Attention: 取得しようとしたノードが all_node_dict に存在しないことは、そのノードが 離脱（ダウンしている状態も含）
    #            したことを意味するため、当該状態に対応する NodeIsDownedException 例外を raise する
    # TODO: 実システム化する際は rpcで生存チェックをした上で、rpcで取得した情報からnode_info プロパティの値だけ適切に埋めた
    #       ChordNodeオブジェクトを返す get_node_by_address
    @classmethod
    def get_node_by_address(cls, address : str) -> PResult[Optional['ChordNode']]:
        try:
            # with gval.lock_of_all_node_dict:
            ret_val = gval.all_node_dict[address]
        except KeyError:
            # join処理の途中で構築中のノード情報を取得しようとしてしまった場合に発生する
            # traceback.print_stack(file=sys.stdout)
            # print("KeyError occured", flush=True)

            return PResult.Err(None, ErrorCode.InternalControlFlowException_CODE)

        if ret_val.is_alive == False:
            ChordUtil.dprint("get_node_by_address_1,NODE_IS_DOWNED," + ChordUtil.gen_debug_str_of_node(ret_val.node_info))
            return PResult.Err(None, ErrorCode.NodeIsDownedException_CODE)

        return PResult.Ok(ret_val)
*/


// Attention: InternalControlFlowException を raiseする場合がある
// TODO: 実システム化する際は アドレス指定で呼び出せる（ChordNodeオブジェクトのメソッドという形でない）
//       RPC化する必要がありそう。もしくはこのメソッドの呼び出し自体を無くすか。 is_node_alive
pub fn is_node_alive(address : &String) -> Result<bool, GeneralError> {
    let tmp = get_node_by_address(address);
    match tmp {
        Ok(arc_val) => return Ok(true),
        Err(err) => {
            if err.err_code == ERR_CODE_INTERNAL_CONTROL_FLOW_PROBLEM {
                return Err(err);
            }else{ // ERR_CODE_NODE_IS_DOWNED
                return Ok(false);
            }
        }
    }
}
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
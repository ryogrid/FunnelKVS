extern crate rand;

use std::sync::{Arc, Mutex};
use std::borrow::{Borrow, BorrowMut};
use std::cell::RefMut;
use std::cell::RefCell;
use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::{ReentrantMutex, const_reentrant_mutex};
use rand::Rng;
use chrono::{Local, DateTime, Date};

use crate::gval;
use crate::chord_node;
use crate::node_info;
use crate::stabilizer;
use crate::router;
use crate::endpoints;
use crate::data_store;

//type ArRmRs<T> = Arc<ReentrantMutex<RefCell<T>>>;
type ArMu<T> = Arc<Mutex<T>>;

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
pub fn get_rnd_int_with_limit(limit : u32) -> u32{
    let mut rng = rand::thread_rng(); // 乱数生成器の初期化
    let limit_inner:i32 = limit as i32;
    let rand_val: i32 = rng.gen_range(0..limit_inner);
    return rand_val as u32;
}

// 任意の文字列をハッシュ値（定められたbit数で表現される整数値）に変換しint型で返す
// アルゴリズムはSHA1, 160bitで表現される正の整数となる
// メモ: 10進数の整数は組み込みの hex関数で 16進数表現での文字列に変換可能
// TODO: 本来のハッシュ関数に戻す必要あり hash_str_to_int
pub fn hash_str_to_int(_input_str : &String) -> u32 {
    // hash_hex_str = hashlib.sha1(input_str.encode()).hexdigest()
    // hash_id_num = int(hash_hex_str, 16)

    // TODO: ID_SPACE_BITS ビットで表現できる符号なし整数をID空間とする.
    //       通常、ID_SPACE_BITS は sha1 で 160 となるが、この検証コードでは
    //       ハッシュ関数を用いなくても問題の起きない実装となっているため、より小さい
    //       ビット数で表現可能な IDスペース 内に収まる値を乱数で求めて返す
    let rand_val: u32 = get_rnd_int_with_limit(gval::ID_SPACE_RANGE);
    return rand_val;
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
        return (gval::ID_SPACE_RANGE - 1) as u32;
    }

    // 0をまたいだ場合に考えやすくするためにtarget_idを0にずらし、base_idを
    // 同じ数だけずらす
    let mut slided_base_id = base_id as i32 - target_id as i32;
    if slided_base_id < 0 {
        // マイナスの値をとった場合は値0を通り越しているので
        // それにあった値に置き換える
        slided_base_id = (gval::ID_MAX as i32) + (slided_base_id as i32);
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
        return gval::ID_SPACE_RANGE - 1;
    }

    // 0をまたいだ場合に考えやすくするためにtarget_idを0にずらし、base_idを
    // 同じ数だけずらす
    let mut slided_target_id = (target_id as i32) - (base_id as i32);
    if slided_target_id < 0 {
        // マイナスの値をとった場合は値0を通り越しているので
        // それにあった値に置き換える
        slided_target_id = (gval::ID_MAX as i32) + (slided_target_id as i32);
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

// TODO: (rust) 将来的にはNodeInfoのロックを保持し続けなくて済むように出力に必要な要素を各々引数に渡す形に
//              する必要がありそう
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

pub fn get_node_info_by_address(address : &String) -> Result<&node_info::NodeInfo, GeneralError> {
    // TODO: (rustr) 通信をして、successor_list と predecessor_info も埋めた NodeInfo を返すようなものにする感じかな・・・
    return Err(GeneralError::new("not implemented yet".to_string(), ERR_CODE_INTERNAL_CONTROL_FLOW_PROBLEM));
}
/*
// Attention: 取得しようとしたノードが all_node_dict に存在しないことは、そのノードが 離脱（ダウンしている状態も含）
//            したことを意味するため、当該状態に対応する NodeIsDownedException 例外を raise する
// TODO: 実システム化する際は rpcで生存チェックをした上で、rpcで取得した情報からnode_info プロパティの値だけ適切に埋めた
//       ChordNodeオブジェクトを返す get_node_by_address
pub fn get_node_by_address(address : &String) -> Result<ArRmRs<chord_node::ChordNode>, GeneralError> {
    // let gd_refcell = get_refcell_from_arc_with_locking!(gval::global_datas);
    // let gd_ref = get_ref_from_refcell!(gd_refcell);

    //println!("get_node_by_address {:?}", address);
    let get_result = gd_ref.all_node_dict.get(address);
    let ret_val_cloned = 
        match get_result {
            // join処理の途中で構築中のノード情報を取得しようとしてしまった場合に発生する
            None => { return Err(GeneralError::new("".to_string(), ERR_CODE_INTERNAL_CONTROL_FLOW_PROBLEM))},
            Some(arc_val) => Arc::clone(arc_val),
        };

    {
        let node_refcell = get_refcell_from_arc_with_locking!(ret_val_cloned);
        let node_ref = get_ref_from_refcell!(node_refcell);

        let callee_ninfo_refcell = get_refcell_from_arc_with_locking!(node_ref.node_info);
        let callee_ninfo_ref = get_ref_from_refcell!(callee_ninfo_refcell);

        if node_refcell.borrow().is_alive.load(Ordering::Relaxed) == false {
            dprint(&("get_node_by_address_1,NODE_IS_DOWNED,".to_string() + &gen_debug_str_of_node(Some(callee_ninfo_ref))));
            return Err(GeneralError::new("".to_string(), ERR_CODE_NODE_IS_DOWNED));
        }
        // ret_val_clonedからborrowしたいた参照を無効にする
    }

    return Ok(ret_val_cloned);
}
*/

/*
pub fn get_lock_obj(kind : &str, address : &String) -> ArRmRs<bool> {
    let gd_refcell = get_refcell_from_arc_with_locking!(gval::global_datas);
    let gd_refmut = get_refmut_from_refcell!(gd_refcell);

    //println!("get_node_by_address {:?}", address);
    let get_result;
    if kind == "ninfo" {
        get_result = gd_refmut.ninfo_lock_dict.get(address);
    }else if kind == "dstore" {
        get_result = gd_refmut.dstore_lock_dict.get(address);
    }else if kind == "tqueue" {
        get_result = gd_refmut.tqueue_lock_dict.get(address);
    }else{
        panic!("unknown kind is passed at get_lock_obj");
    }
    
    let ret_val_cloned: ArRmRs<bool> = 
        match get_result {
            None => {
                // まだ存在しなかった
                if kind == "ninfo" {
                    let new_lock_obj = ArRmRs_new!(false);
                    gd_refmut.ninfo_lock_dict.insert(address.clone(), Arc::clone(&new_lock_obj));
                    new_lock_obj 
                }else if kind == "dstore" {
                    let new_lock_obj = ArRmRs_new!(false);
                    gd_refmut.dstore_lock_dict.insert(address.clone(), Arc::clone(&new_lock_obj));
                    new_lock_obj
                }else if kind == "tqueue" {
                    let new_lock_obj = ArRmRs_new!(false);
                    gd_refmut.tqueue_lock_dict.insert(address.clone(), Arc::clone(&new_lock_obj));
                    new_lock_obj
                }else{
                    panic!("unknown kind is passed at get_lock_obj");                    
                }                
            },
            Some(arc_val) => Arc::clone(arc_val),
        };

    return ret_val_cloned;
}
*/

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
    // TODO: (rustr) 現状は故障ノードを想定しないため必ずtrueを返す
    return Ok(true);

/*
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
*/
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
use std::sync::{Arc, Mutex};
use std::cell::{RefMut, RefCell, Ref};
use serde::{Serialize, Deserialize};

use crate::gval;
use crate::chord_node;
use crate::chord_util;
use crate::stabilizer;
use crate::endpoints;
use crate::data_store;
use crate::router;

type ArMu<T> = Arc<Mutex<T>>;

#[derive(Serialize, Deserialize)]
#[derive(Debug)]
pub struct NodeInfo {
//    pub existing_node : ArRmRs<chord_node::ChordNode>,
    pub node_id : u32,
    pub address_str: String,
    // デバッグ用のID
    // 何ノード目として生成されたかの値
    // TODO: 実システムでは開発中（というか、スクリプトで順にノード起動していくような形）でないと
    //       利用できないことは念頭おいて置く必要あり NodeInfo#born_id
    pub born_id : i32,
    // 以下の2つはNodeInfoオブジェクトを保持.
    // ある時点で取得したものが保持されており、変化する場合のあるフィールド
    // の内容は最新の内容となっているとは限らないため注意が必要.
    // そのような情報が必要な場合はChordNodeオブジェクトから参照し、
    // 必要であれば、その際に下のフィールドにdeepcopyを設定しなおさ
    // なければならない.
    //
    // 状況に応じて伸縮するが、インデックス0には必ず 非None な要素が入っている
    // ように制御する
    pub successor_info_list: Vec<NodeInfo>,
    // join後はNoneになることのないように制御される
    // Option<NodeInfo>だと再帰的定義となってコンパイルエラーとなり、
    // Arc<Option<NodeInfo>> とすると参照アクセスする時にうまくいかないので
    // 要素数が0もしくは1のVecとして定義する。Noneに対応する状態はlen()の結果が0の時
    // 格納されている要素自体はimmutableとして扱わなければならないので注意
    pub predecessor_info: Vec<NodeInfo>,
    // NodeInfoオブジェクトを要素として持つリスト
    // インデックスの小さい方から狭い範囲が格納される形で保持する
    // sha1で生成されるハッシュ値は160bit符号無し整数であるため要素数は160となる
    // TODO: 現在は ID_SPACE_BITS が検証時の実行時間の短縮のため30となっている
    
    pub finger_table: Vec<Option<NodeInfo>>, // = [None] * gval.ID_SPACE_BITS
}

impl NodeInfo {
    pub fn new() -> NodeInfo {
        NodeInfo {
            node_id : 0, //TODO: node_idの初期値を-1から0に変更したので注意
            address_str: "".to_string(),
            born_id : -1,
            successor_info_list : Vec::new(),
            predecessor_info : Vec::new(),
            finger_table : vec![None; gval::ID_SPACE_BITS as usize]
        }
    }
}

// 単純にdeepcopyするとチェーン構造になっているものが全てコピーされてしまう
// ため、そこの考慮を行ったデータを返す
// 上述の考慮により、コピーした NodeInfoオブジェクト の successor_infoと
// predecessor_info および finger_table は deepcopy の対象ではあるが、
// それらには空のVecが設定される. これにより、あるノードがコピーされた NodeInfo を保持
// していても、自身のpredecessor や successor は自身が保持しているそれらのNodeInfo
// オブジェクトの情報から辿ることができるが、その先は辿ることは直接的にはできないことになる
//（predecessor や successor の NodeInfoオブジェクトをRPCで当該ノードから取得すれば可能）
// 用途としては、あるノードの NodeInfo を他のノードが取得し保持する際に利用される
// ことを想定して実装されている.
impl Clone for NodeInfo {
    fn clone(&self) -> Self {
        let mut ret_node_info = NodeInfo::new();

        ret_node_info.node_id = self.node_id;
        ret_node_info.address_str = self.address_str.clone();
        ret_node_info.born_id = self.born_id;
        ret_node_info.successor_info_list = vec![];
        ret_node_info.predecessor_info = vec![];
    
        return ret_node_info;
    }  
}

// 実装の参照からコピーを作成する
// cloneした場合と異なり、predecessor_info, successor_info_list, finger_table
// も一段階だけは値を埋めて返す（各NodeInfoオブジェクトはcloneされたもの）
pub fn partial_clone_from_ref_strong(node_info_ref: &NodeInfo) -> NodeInfo {
    let mut ret_node_info = NodeInfo::new();

    ret_node_info.node_id = node_info_ref.node_id;
    ret_node_info.address_str = node_info_ref.address_str.clone();
    ret_node_info.born_id = node_info_ref.born_id;
    ret_node_info.successor_info_list = vec![];
    for each_ninfo in &node_info_ref.successor_info_list {
        ret_node_info.successor_info_list.push((*each_ninfo).clone());
    }
    ret_node_info.finger_table = vec![];
    for each_ninfo in &node_info_ref.finger_table {
        let tmp_val = match each_ninfo {
            None => None,
            Some(val) => {
                let ret_val = Some((*val).clone());
                ret_val
            }
        };
        ret_node_info.finger_table.push(tmp_val);
    }    
    ret_node_info.predecessor_info = vec![];
    for each_ninfo in &node_info_ref.predecessor_info {
        ret_node_info.predecessor_info.push((*each_ninfo).clone());
    }

    //println!("clone_strong: {:?}", ret_node_info);
    return ret_node_info;    
}

pub fn set_pred_info(self_node: ArMu<NodeInfo>, node_info: NodeInfo){
    let mut self_node_ref = self_node.lock().unwrap();
    if self_node_ref.predecessor_info.len() == 0 {
        self_node_ref.predecessor_info.push(node_info);
    }else{
        self_node_ref.predecessor_info[0] = node_info;
    }
}

// RPC呼出しが接続失敗やタイムアウトで終了し、かつ、対象がsuccessorで
// あった場合にリカバリ処理を行う
pub fn handle_downed_node_info(self_node: &mut NodeInfo, target_node: &NodeInfo, err: &chord_util::GeneralError){
    chord_util::dprint(&("handle_downed_node_info called!".to_string()));

    //successorについて
    let old_successor_id = self_node.successor_info_list[0].node_id;
    if err.err_code == chord_util::ERR_CODE_HTTP_REQUEST_ERR 
        && target_node.node_id == self_node.successor_info_list[0].node_id {

        // finger_tableを先頭から辿ってsuccessorに設定可能なものがあれば設定する
        for idx in 0..(gval::ID_SPACE_BITS as usize) {
            match &self_node.finger_table[idx] {
                    None => { continue; }
                    Some(ninfo) => {
                        if ninfo.node_id != self_node.node_id && ninfo.node_id != old_successor_id{
                            chord_util::dprint(&("assign new successor!".to_string() + " from " + chord_util::gen_debug_str_of_node(&self_node.successor_info_list[0]).as_str() + " to " + chord_util::gen_debug_str_of_node(ninfo).as_str()));
                            self_node.successor_info_list[0] = (*ninfo).clone();
                            break;
                        }
                    }
            };
        }
    }

    // predecessorについて
    if self_node.predecessor_info.len() != 0 {
        if err.err_code == chord_util::ERR_CODE_HTTP_REQUEST_ERR 
            && target_node.node_id == self_node.predecessor_info[0].node_id {
            // predecessorであった場合は predecessor のまま設定しておくと都合が悪いので
            // お役御免とする
            self_node.predecessor_info.clear();
        }    
    }

    // finger tableの情報について
    if err.err_code == chord_util::ERR_CODE_HTTP_REQUEST_ERR {
        // finger_tableを先頭から辿ってダウンが判明したノードがいたらNoneに設定する
        for idx in 0..(gval::ID_SPACE_BITS as usize) {
            match &self_node.finger_table[idx] {
                None => { continue; }
                Some(ninfo) => {
                    if ninfo.node_id == target_node.node_id {
                        self_node.finger_table[idx] = None;
                    }
                }
            };
        }
    }
}


/*
# coding:utf-8

import copy

from . import gval
from .chord_util import ChordUtil
import threading

# メモ: オブジェクトをdictのキーとして使用可能としてある
class NodeInfo:

    def __init__(self):
        self.node_id: int = -1
        self.address_str: str = ""

        # デバッグ用のID
        # 何ノード目として生成されたかの値
        # TODO: 実システムでは開発中（というか、スクリプトで順にノード起動していくような形）でないと
        #       利用できないことは念頭おいて置く必要あり NodeInfo#born_id
        self.born_id: int = -1

        # 以下の2つはNodeInfoオブジェクトを保持.
        # ある時点で取得したものが保持されており、変化する場合のあるフィールド
        # の内容は最新の内容となっているとは限らないため注意が必要.
        # そのような情報が必要な場合はChordNodeオブジェクトから参照し、
        # 必要であれば、その際に下のフィールドにdeepcopyを設定しなおさ
        # なければならない.

        # 状況に応じて伸縮するが、インデックス0には必ず 非None な要素が入っている
        # ように制御する
        self.successor_info_list: List[NodeInfo] = []
        # join後はNoneになることのないように制御される
        self.predecessor_info: Optional[NodeInfo] = None

        # predecessor_info と successor_info_list のそれぞれに対応する
        # ロック変数(re-entrantロック)
        self.lock_of_pred_info : threading.RLock = threading.RLock()
        self.lock_of_succ_infos : threading.RLock = threading.RLock()

        # stored_data, master2data_idx、master_node_dict 全てのフィールドに対する
        # ロック変数(re-entrantロック)
        self.lock_of_datastore : threading.RLock = threading.RLock()

        # NodeInfoオブジェクトを要素として持つリスト
        # インデックスの小さい方から狭い範囲が格納される形で保持する
        # sha1で生成されるハッシュ値は160bit符号無し整数であるため要素数は160となる
        # TODO: 現在は ID_SPACE_BITS が検証時の実行時間の短縮のため30となっている
        self.finger_table: List[Optional[NodeInfo]] = [None] * gval.ID_SPACE_BITS

    # 単純にdeepcopyするとチェーン構造になっているものが全てコピーされてしまう
    # ため、そこの考慮を行い、また、finger_tableはコピーしない形での deepcopy
    # を返す.
    # 上述の考慮により、コピーした NodeInfoオブジェクト の successor_infoと
    # predecessor_infoは deepcopy の対象ではあるが、それらの中の同名のフィールド
    # にはNoneが設定される. これにより、あるノードがコピーされた NodeInfo を保持
    # した場合、predecessor や successorは辿ることができるが、その先は辿ることが
    # 直接的にはできないことになる（predecessor や successorの ChordNodeオブジェクト
    # を引いてやれば可能）
    # 用途としては、あるノードの node_info を他のノードが取得し保持する際に利用される
    # ことを想定して実装されている.
    def get_partial_deepcopy(self) -> 'NodeInfo':
        ret_node_info: NodeInfo = NodeInfo()

        ret_node_info.node_id = copy.copy(self.node_id)
        ret_node_info.address_str = copy.copy(self.address_str)
        ret_node_info.born_id = copy.copy(self.born_id)
        ret_node_info.successor_info_list = []
        ret_node_info.predecessor_info = None

        # ロック関連のフィールドは本メソッドでコピーすることで生まれた
        # オブジェクトにおいて利用されることがあったとしても、ロックの
        # 対象は上記でコピーしているオブジェクトではなく、フィールドそのもの
        # であるため、コピーの必要はない

        return ret_node_info

    def __eq__(self, other):
        if not isinstance(other, NodeInfo):
            return False
        return self.node_id == other.node_id

    def __hash__(self):
        return self.node_id

    def __str__(self):
        return ChordUtil.gen_debug_str_of_node(self)
*/

use std::sync::Arc;
use std::cell::RefCell;
use parking_lot::{ReentrantMutex, const_reentrant_mutex};

use crate::gval;
use crate::chord_node;
use crate::chord_util;
use crate::stabilizer;
use crate::taskqueue;
use crate::endpoints;
use crate::data_store;
use crate::router;

type ArRmRs<T> = Arc<ReentrantMutex<RefCell<T>>>;

#[derive(Debug, Clone)]
pub struct NodeInfo {
    pub existing_node : ArRmRs<chord_node::ChordNode>,
    pub node_id : i32,
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
    pub successor_info_list: ArRmRs<Vec<NodeInfo>>,
    // join後はNoneになることのないように制御される
    //predecessor_info: Option<&'static NodeInfo>,
    pub predecessor_info: Arc<ReentrantMutex<Option<NodeInfo>>>,
    // NodeInfoオブジェクトを要素として持つリスト
    // インデックスの小さい方から狭い範囲が格納される形で保持する
    // sha1で生成されるハッシュ値は160bit符号無し整数であるため要素数は160となる
    // TODO: 現在は ID_SPACE_BITS が検証時の実行時間の短縮のため30となっている
    
    pub finger_table: ArRmRs<Vec<Option<NodeInfo>>>, // = [None] * gval.ID_SPACE_BITS
}

impl NodeInfo {
    pub fn new(parent : ArRmRs<chord_node::ChordNode>) -> NodeInfo {
        let si_list = Arc::new(const_reentrant_mutex(RefCell::new(Vec::new())));
        let pred_info = Arc::new(const_reentrant_mutex(None));
        let ftable = Arc::new(const_reentrant_mutex(RefCell::new(vec![None; gval::ID_SPACE_BITS as usize])));
        NodeInfo {
            existing_node : parent,
            node_id : -1,
            address_str: "".to_string(),
            born_id : -1,
            successor_info_list : si_list,
            predecessor_info : pred_info,
            finger_table : ftable
        }
    }
}
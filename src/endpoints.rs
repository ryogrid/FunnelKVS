/*
# coding:utf-8

from .chord_util import ChordUtil, InternalControlFlowException,\
    NodeIsDownedExceptiopn, DataIdAndValue, KeyValue, PResult

class Endpoints:

    def __init__(self, existing_node : 'ChordNode'):
        self.existing_node = existing_node

    def rrpc__global_put(self, data_id : int, value_str : str) -> bool:
        return self.existing_node.global_put(data_id, value_str)

    def grpc__put(self, data_id : int, value_str : str) -> bool:
        return self.existing_node.put(data_id, value_str)

    def grpc__global_get_recover_prev(self, data_id : int) -> Tuple[str, Optional['ChordNode']]:
        return self.existing_node.global_get_recover_prev(data_id)

    def grpc__global_get_recover_succ(self, data_id: int) -> Tuple[str, Optional['ChordNode']]:
        return self.existing_node.global_get_recover_succ(data_id)

    def rrpc__global_get(self, data_id : int) -> str:
        return self.existing_node.global_get(data_id)

    def grpc__get(self, data_id : int, for_recovery = False) -> str:
        return self.existing_node.get(data_id, for_recovery)

    def grpc__global_delete(self, data_id : int) -> bool:
        return self.existing_node.global_delete(data_id)

    def grpc__pass_node_info(self) -> 'NodeInfo':
        return self.existing_node.pass_node_info()

    def grpc__get_all_tantou_data(self, node_id : Optional[int] = None) -> List[DataIdAndValue]:
        return self.existing_node.data_store.get_all_tantou_data(node_id)

    def grpc__receive_replica(self, pass_datas : List[DataIdAndValue]):
        return self.existing_node.data_store.receive_replica(pass_datas)

    def grpc__delegate_my_tantou_data(self, node_id : int) -> List[KeyValue]:
        return self.existing_node.data_store.delegate_my_tantou_data(node_id)

    def grpc__get_all_data(self) -> List[DataIdAndValue]:
        return self.existing_node.data_store.get_all_data()

    # TODO: AppropriateExp, DownedExp, InternalExp at grpc__find_successor
    def grpc__find_successor(self, id : int) -> PResult[Optional['ChordNode']]:
        return self.existing_node.router.find_successor(id)

    def grpc__closest_preceding_finger(self, id : int) -> 'ChordNode':
        return self.existing_node.router.closest_preceding_finger(id)

    def grpc__pass_successor_list(self) -> List['NodeInfo']:
        return self.existing_node.stabilizer.pass_successor_list()

    def grpc__pass_predecessor_info(self) -> Optional['NodeInfo']:
        return self.existing_node.stabilizer.pass_predecessor_info()

    def grpc__set_routing_infos_force(self, predecessor_info : 'NodeInfo', successor_info_0 : 'NodeInfo', ftable_enry_0 : 'NodeInfo'):
        return self.existing_node.stabilizer.set_routing_infos_force(predecessor_info, successor_info_0, ftable_enry_0)

    # TODO: InternalExp, DownedExp at grpc__stabilize_succesor_inner
    def grpc__stabilize_successor_inner(self) -> PResult[Optional['NodeInfo']]:
        return self.existing_node.stabilizer.stabilize_successor_inner()

    # TODO: InternalExp at grpc__check_predecessor
    def grpc__check_predecessor(self, node_info : 'NodeInfo') -> PResult[bool]:
        return self.existing_node.stabilizer.check_predecessor(node_info)

    # TODO: InternalExp at grpc__check_successor_list_length
    def grpc__check_successor_list_length(self) -> PResult[bool]:
        return self.existing_node.stabilizer.check_successor_list_length()

    # TODO: 実システムでは、ChordNodeオブジェクトが生成されたあとはこのrpcでチェック可能とする
    #       ただし初回の呼び出しはget_node_by_addressの中で行われ、そこでのチェックを通った場合のみ
    #       同メソッドは ChordNodeオブジェクトを返す設計とする（通信回数が増えてしまうがそこは許容する）
    def grpc__is_alive(self) -> bool:
        raise Exception("not implemented yet")

    # TODO: 実システムでだけ用いる。ノード情報を取得するAPI
    #       get_nobe_by_address内でgrpc__is_aliveでの生存チェックを通ったら
    #       このメソッドで暫定的に生成したChordNodeオブジェクトを構築するための情報
    #       を取得する. 内容としては NodeInfoオブジェクトのうち、successor_info_list
    #       のみ空リストとなっているものになる見込み
    def grpc__get_chord_node_info(self) -> 'NodeInfo':
        ret_info : NodeInfo = self.existing_node.node_info.get_partial_deepcopy()
        if self.existing_node.node_info.predecessor_info != None:
            ret_info.predecessor_info = cast('NodeInfo', self.existing_node.node_info.predecessor_info).get_partial_deepcopy()
        return ret_info
*/
use std::sync::{Arc, Mutex};
use std::cell::{RefCell, Ref, RefMut};
use parking_lot::{ReentrantMutex, const_reentrant_mutex};
use rocket_contrib::json::Json;

use crate::gval;
use crate::chord_node;
use crate::node_info;
use crate::chord_util;
use crate::data_store;
use crate::router;
use crate::stabilizer;
//use crate::taskqueue;

//type ArRmRs<T> = Arc<ReentrantMutex<RefCell<T>>>;
type ArMu<T> = Arc<Mutex<T>>;


/*
//TODO: (rust) ダミー実装なので委譲処理が必要になったタイミングで対応すること
pub fn grpc__delegate_my_tantou_data(predecessor: ArRmRs<chord_node::ChordNode>, node_id : u32) -> Vec<chord_util::KeyValue>{
    return vec![];
    //return self.existing_node.data_store.delegate_my_tantou_data(node_id)
}
*/

/*
def grpc__delegate_my_tantou_data(self, node_id : int) -> List[KeyValue]:
    return self.existing_node.data_store.delegate_my_tantou_data(node_id)
*/

/*
pub fn grpc__pass_successor_list(self_node: ArRmRs<chord_node::ChordNode>) -> Vec<node_info::NodeInfo> {
    return stabilizer::pass_successor_list(self_node);
}
*/

/*
def grpc__pass_successor_list(self) -> List['NodeInfo']:
    return self.existing_node.stabilizer.pass_successor_list()
*/

#[get("/")]
fn index() -> &'static str {
    "Hello, world!"
}

pub fn rest_api_server_start(){
    rocket::ignite()
        .mount("/", routes![index])
        .launch();
}

// TODO: (rustr) comment-outed at spiking RPC impl
/*
pub fn grpc__check_predecessor(self_node: ArMu<node_info::NodeInfo>, caller_node_ni: &node_info::NodeInfo) -> Result<bool, chord_util::GeneralError> {
    return stabilizer::check_predecessor(self_node, caller_node_ni);
}

pub fn grpc__set_routing_infos_force(self_node: ArMu<node_info::NodeInfo>, predecessor_info: node_info::NodeInfo, successor_info_0: node_info::NodeInfo , ftable_enry_0: node_info::NodeInfo){
    return stabilizer::set_routing_infos_force(Arc::clone(&self_node), predecessor_info, successor_info_0, ftable_enry_0);
}

// id（int）で識別されるデータを担当するノードの名前解決を行う
// Attention: 適切な担当ノードを得ることができなかった場合、FindNodeFailedExceptionがraiseされる
//            finger_tableに値が埋められた NodeInfoへの参照を渡すこと
// TODO: AppropriateExp, DownedExp, InternalExp at find_successor
pub fn grpc__find_successor(exnode_ni_ref: &node_info::NodeInfo, id : u32) -> Result<&node_info::NodeInfo, chord_util::GeneralError> {
    return router::find_successor(exnode_ni_ref, id);
}

// Attention: finger_tableに値が埋められた NodeInfoへの参照を渡すこと
pub fn grpc__closest_preceding_finger(exnode_ni_ref: &node_info::NodeInfo, id : u32) -> &node_info::NodeInfo {
    return router::closest_preceding_finger(exnode_ni_ref, id);
}

pub fn grpc__stabilize_successor_inner(self_node: ArMu<node_info::NodeInfo>) -> Result<Option<node_info::NodeInfo>, chord_util::GeneralError>{
    return stabilizer::stabilize_successor_inner(self_node);
}
*/
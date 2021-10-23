/*
# coding:utf-8

from .chord_util import ChordUtil, InternalControlFlowException,\
    NodeIsDownedExceptiopn, DataIdAndValue, KeyValue, PResult

class Endpoints:

    def __init__(self, existing_node : 'ChordNode'):
        self.existing_node = existing_node

    def rrpc__global_put(self, data_id : int, value_str : str) -> bool:
        return self.existing_node.global_put(data_id, value_str)

    def rrpc__put(self, data_id : int, value_str : str) -> bool:
        return self.existing_node.put(data_id, value_str)

    def rrpc__global_get(self, data_id : int) -> str:
        return self.existing_node.global_get(data_id)

    def rrpc__get(self, data_id : int, for_recovery = False) -> str:
        return self.existing_node.get(data_id, for_recovery)

    def rrpc__global_delete(self, data_id : int) -> bool:
        return self.existing_node.global_delete(data_id)

    # TODO: AppropriateExp, DownedExp, InternalExp at grpc__find_successor
    def rrpc__find_successor(self, id : int) -> PResult[Optional['ChordNode']]:
        return self.existing_node.router.find_successor(id)

    def rrpc__closest_preceding_finger(self, id : int) -> 'ChordNode':
        return self.existing_node.router.closest_preceding_finger(id)

    def rrpc__set_routing_infos_force(self, predecessor_info : 'NodeInfo', successor_info_0 : 'NodeInfo', ftable_enry_0 : 'NodeInfo'):
        return self.existing_node.stabilizer.set_routing_infos_force(predecessor_info, successor_info_0, ftable_enry_0)

    # TODO: InternalExp at grpc__check_predecessor
    def rrpc__check_predecessor(self, node_info : 'NodeInfo') -> PResult[bool]:
        return self.existing_node.stabilizer.check_predecessor(node_info)

    # TODO: 実システムでだけ用いる。ノード情報を取得するAPI
    #       get_nobe_by_address内でgrpc__is_aliveでの生存チェックを通ったら
    #       このメソッドで暫定的に生成したChordNodeオブジェクトを構築するための情報
    #       を取得する. 内容としては NodeInfoオブジェクトのうち、successor_info_list
    #       のみ空リストとなっているものになる見込み
    def rrpc__get_chord_node_info(self) -> 'NodeInfo':
        ret_info : NodeInfo = self.existing_node.node_info.get_partial_deepcopy()
        if self.existing_node.node_info.predecessor_info != None:
            ret_info.predecessor_info = cast('NodeInfo', self.existing_node.node_info.predecessor_info).get_partial_deepcopy()
        return ret_info
*/
use std::sync::{Arc, Mutex};
use std::cell::{RefCell, Ref, RefMut};
use parking_lot::{ReentrantMutex, const_reentrant_mutex};
use rocket_contrib::json::Json;
use rocket::State;
use rocket::config::{Config, Environment};

use chord_util::GeneralError;

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

#[get("/")]
//fn index() -> &'static str {
fn index() -> Json<node_info::NodeInfo> {
    let mut node_info = node_info::NodeInfo::new();
    
    node_info.node_id = 100;
    node_info.address_str = "kanbayashi".to_string();
    node_info.born_id = 77;
    node_info.successor_info_list = vec![];
    node_info.successor_info_list.push(node_info.clone());    
    node_info.predecessor_info = vec![];
    node_info.predecessor_info.push(node_info::partial_clone_from_ref_strong(&node_info));
    //node_info.finger_table = vec![];

    //"Hello, world!"
    Json(node_info)
}

#[get("/get-param-test?<param1>&<param2>")]
fn get_param_test(param1: String, param2: String) -> Json<node_info::NodeInfo> {
    let mut node_info = node_info::NodeInfo::new();

    println!("{:?}", param1);
    println!("{:?}", param2);
    
    node_info.node_id = 100;
    node_info.address_str = "kanbayashi".to_string();
    node_info.born_id = 77;
    node_info.successor_info_list = vec![];
    node_info.successor_info_list.push(node_info.clone());    
    node_info.predecessor_info = vec![];
    node_info.predecessor_info.push(node_info::partial_clone_from_ref_strong(&node_info));
    //node_info.finger_table = vec![];

    //"Hello, world!"
    Json(node_info)
}

#[post("/deserialize", data = "<node_info>")]
pub fn deserialize_test(self_ninfo: State<ArMu<node_info::NodeInfo>>, data_store: State<ArMu<data_store::DataStore>>, node_info: Json<node_info::NodeInfo>) -> String {
    // TODO: (rustr) 複数の引数をとるようなことがしたければ、それらを含むStructを定義するしか無さそう
    println!("{:?}", self_ninfo.lock().unwrap());
    println!("{:?}", node_info.address_str);
    println!("{:?}", node_info);
    format!("Accepted post request! {:?}", node_info.0)
}

pub fn rest_api_server_start(self_ninfo: ArMu<node_info::NodeInfo>, data_store: ArMu<data_store::DataStore>, port_num: i32){
    // TODO: (rustr) 起動時のポート指定
    let config = Config::build(Environment::Production)
    .address("127.0.0.1")
    .port(port_num as u16)
    .finalize()
    .unwrap();

    let app = rocket::custom(config);
    
    app.manage(self_ninfo)
       .manage(data_store)
       .mount("/", routes![index, get_param_test, deserialize_test])
       .launch();
/*    
    rocket::ignite()
        .manage(self_ninfo)
        .manage(data_store)
        .mount("/", routes![index, get_param_test, deserialize_test])
        .launch();
*/
}

// TODO: (rustr) 分散KVS化する際は、putのレプリカ配るだけ版みたいなものを実装する必要あり
//               実際に処理を行う側は正規のputかレプリカの配布かを判別できるフラグを追加する形で
//               1つのメソッドにまとめてしまって良いかと思う

// TODO: ひとまずRPC化して結合するまでは、grpc__xxx は NodeInfoの完全な実体（完全なClone）を受けて
//       それらが内部で読んでいる関数には受けた実体を ArMu_new!(xxx) でラップして渡す、とすればいい気がする・・・

pub fn rrpc__check_predecessor(self_node: &node_info::NodeInfo, caller_node_ni: &node_info::NodeInfo) -> Result<bool, chord_util::GeneralError> {
    //TODO: (rustr) ひとまずダミーを渡しておく
    let dummy_self_node = ArMu_new!(node_info::NodeInfo::new());
    return stabilizer::check_predecessor(dummy_self_node, node_info::partial_clone_from_ref_strong(caller_node_ni));
}

pub fn rrpc__set_routing_infos_force(self_node: &node_info::NodeInfo, predecessor_info: node_info::NodeInfo, successor_info_0: node_info::NodeInfo , ftable_enry_0: node_info::NodeInfo){
    //TODO: (rustr) ひとまずダミーを渡しておく
    let dummy_self_node = ArMu_new!(node_info::NodeInfo::new());    
    return stabilizer::set_routing_infos_force(dummy_self_node, predecessor_info, successor_info_0, ftable_enry_0);
}

// id（int）で識別されるデータを担当するノードの名前解決を行う
// Attention: 適切な担当ノードを得ることができなかった場合、FindNodeFailedExceptionがraiseされる
//            finger_tableに値が埋められた NodeInfoへの参照を渡すこと
// TODO: AppropriateExp, DownedExp, InternalExp at find_successor
//pub fn rrpc__find_successor(self_node: &node_info::NodeInfo, id : u32) -> Result<node_info::NodeInfo, chord_util::GeneralError> {
pub fn rrpc__find_successor(self_node: &node_info::NodeInfo, id : u32) -> node_info::NodeInfo {
    return router::find_successor(ArMu_new!(node_info::partial_clone_from_ref_strong(self_node)), id).unwrap();
}

// Attention: finger_tableに値が埋められた NodeInfoへの参照を渡すこと
// pub fn grpc__closest_preceding_finger(self_node: ArMu<node_info::NodeInfo>, id : u32) -> node_info::NodeInfo {
//     return router::closest_preceding_finger(Arc::clone(&self_node), id);
// }
pub fn rrpc__closest_preceding_finger(self_node: &node_info::NodeInfo, id : u32) -> node_info::NodeInfo {
    return router::closest_preceding_finger(ArMu_new!(node_info::partial_clone_from_ref_strong(self_node)), id);
}

pub fn rrpc__get_node_info(address : &String) -> Result<node_info::NodeInfo, GeneralError> {
    // TODO: (rustr) ひとまずダミーを渡しておく
    return chord_util::get_node_info(ArMu_new!(node_info::NodeInfo::new()));
}
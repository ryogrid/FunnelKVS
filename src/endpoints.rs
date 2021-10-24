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
use reqwest::Error;
use serde::{Serialize, Deserialize};

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

// urlは "http://から始まるものにすること"
fn http_get_request(url_str: &str) -> Result<String, chord_util::GeneralError> {
    let resp = match reqwest::blocking::get(url_str){
        Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR)) },
        Ok(response) => response
    };

    let ret = match resp.text(){
        Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR)) },
        Ok(text) => text
    };

    return Ok(ret);
}

// urlは "http://から始まるものにすること"
// json_str は JSONの文字列表現をそのまま渡せばよい
fn http_post_request(url_str: &str, json_str: String) -> Result<String, chord_util::GeneralError> {
    let client = reqwest::blocking::Client::new();
    let resp = match client.post(url_str).body(json_str).send(){
        Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR)) },
        Ok(response) => response        
    };

    let ret = match resp.text(){
        Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR)) },
        Ok(text) => text
    };

    return Ok(ret);
}

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

#[get("/result-type")]
//fn index() -> &'static str {
fn result_type() -> Json<Result<node_info::NodeInfo, chord_util::GeneralError>> {
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
    Json(Ok(node_info))
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
pub fn deserialize_test(self_node: State<ArMu<node_info::NodeInfo>>, data_store: State<ArMu<data_store::DataStore>>, node_info: Json<node_info::NodeInfo>) -> String {
    // TODO: (rustr) 複数の引数をとるようなことがしたければ、それらを含むStructを定義するしか無さそう
    println!("{:?}", self_node.lock().unwrap());
    println!("{:?}", node_info.address_str);
    println!("{:?}", node_info);
    format!("Accepted post request! {:?}", node_info.0)
}

// TODO: (rustr) 分散KVS化する際は、putのレプリカ配るだけ版みたいなものを実装する必要あり
//               実際に処理を行う側は正規のputかレプリカの配布かを判別できるフラグを追加する形で
//               1つのメソッドにまとめてしまって良いかと思う

// TODO: ひとまずRPC化して結合するまでは、grpc__xxx は NodeInfoの完全な実体（完全なClone）を受けて
//       それらが内部で読んでいる関数には受けた実体を ArMu_new!(xxx) でラップして渡す、とすればいい気がする・・・

pub fn rrpc_call__check_predecessor(self_node: &node_info::NodeInfo, caller_node_ni: &node_info::NodeInfo) -> Result<bool, chord_util::GeneralError> {
    //TODO: (rustr) 通信エラーなどの場合のハンドリングは後で
    let req_rslt = http_post_request(
        &("http://".to_string() + self_node.address_str.as_str() + "/check_predecessor"),
        serde_json::to_string(caller_node_ni).unwrap());
    
    return Ok(true);
}

#[post("/check_predecessor", data = "<caller_node_ni>")]
pub fn rrpc__check_predecessor(self_node: State<ArMu<node_info::NodeInfo>>, caller_node_ni: Json<node_info::NodeInfo>) -> Json<Result<bool, chord_util::GeneralError>> {
    return Json(stabilizer::check_predecessor(Arc::clone(&self_node), caller_node_ni.0));
}

pub fn rrpc_call__set_routing_infos_force(self_node: &node_info::NodeInfo, predecessor_info: node_info::NodeInfo, successor_info_0: node_info::NodeInfo , ftable_enry_0: node_info::NodeInfo) -> Result<bool, chord_util::GeneralError> {
    //TODO: (rustr) 通信エラーなどの場合のハンドリングは後で
    let rpc_arg = SetRoutingInfosForce::new(predecessor_info, successor_info_0, ftable_enry_0);

    let req_rslt = http_post_request(
        &("http://".to_string() + self_node.address_str.as_str() + "/set_routing_infos_force"),
        serde_json::to_string(&rpc_arg).unwrap());

    return Ok(true);
}

#[post("/set_routing_infos_force", data = "<rpc_args>")]
pub fn rrpc__set_routing_infos_force(self_node: State<ArMu<node_info::NodeInfo>>, rpc_args: Json<SetRoutingInfosForce>){
    let args = rpc_args.0;
    return stabilizer::set_routing_infos_force(Arc::clone(&self_node), args.predecessor_info, args.successor_info_0, args.ftable_enry_0);
}

pub fn rrpc_call__find_successor(self_node: &node_info::NodeInfo, id : u32) -> Result<node_info::NodeInfo, chord_util::GeneralError> {
    //TODO: (rustr) 通信エラーなどの場合のハンドリングは後で
    let req_rslt = http_post_request(
        &("http://".to_string() + self_node.address_str.as_str() + "/find_successor"),
        serde_json::to_string(&id).unwrap());
    
    let ret_ninfo = serde_json::from_str::<node_info::NodeInfo>(&req_rslt.unwrap()).unwrap();
    println!("find_successor: {:?}", ret_ninfo);
    return Ok(ret_ninfo);
}

// id（int）で識別されるデータを担当するノードの名前解決を行う
// Attention: 適切な担当ノードを得ることができなかった場合、FindNodeFailedExceptionがraiseされる
//            finger_tableに値が埋められた NodeInfoへの参照を渡すこと
// TODO: AppropriateExp, DownedExp, InternalExp at find_successor
//pub fn rrpc__find_successor(self_node: &node_info::NodeInfo, id : u32) -> Result<node_info::NodeInfo, chord_util::GeneralError> {
#[post("/find_successor", data = "<id>")]
pub fn rrpc__find_successor(self_node: State<ArMu<node_info::NodeInfo>>, id : Json<u32>) -> Json<Result<node_info::NodeInfo, chord_util::GeneralError>> {
    return Json(router::find_successor(Arc::clone(&self_node), id.0));
}

// Attention: finger_tableに値が埋められた NodeInfoへの参照を渡すこと
// pub fn grpc__closest_preceding_finger(self_node: ArMu<node_info::NodeInfo>, id : u32) -> node_info::NodeInfo {
//     return router::closest_preceding_finger(Arc::clone(&self_node), id);
// }
pub fn rrpc_call__closest_preceding_finger(self_node: &node_info::NodeInfo, id : u32) -> Result<node_info::NodeInfo, chord_util::GeneralError> {
    //TODO: (rustr) 通信エラーなどの場合のハンドリングは後で
    let req_rslt = http_post_request(
        &("http://".to_string() + self_node.address_str.as_str() + "/closest_preceding_finger"),
        serde_json::to_string(&id).unwrap());
    
    let ret_ninfo = serde_json::from_str::<node_info::NodeInfo>(&req_rslt.unwrap()).unwrap();
    println!("closest_preceding_finger: {:?}", ret_ninfo);
    return Ok(ret_ninfo);
}

#[post("/closest_preceding_finger", data = "<id>")]
pub fn rrpc__closest_preceding_finger(self_node: State<ArMu<node_info::NodeInfo>>, id : Json<u32>) -> Json<node_info::NodeInfo> {
    return Json(router::closest_preceding_finger(Arc::clone(&self_node), id.0));
}

pub fn rrpc_call__get_node_info(address : &String) -> Result<node_info::NodeInfo, GeneralError> {
    //TODO: (rustr) 通信エラーなどの場合のハンドリングは後で
    println!("get_node_info: {:?}", *address);
    let req_rslt = http_get_request(&("http://".to_string() + address.as_str() + "/get_node_info"));
    let ret_ninfo = serde_json::from_str::<node_info::NodeInfo>(&req_rslt.unwrap()).unwrap();
    println!("get_node_info: {:?}", ret_ninfo);
    return Ok(ret_ninfo);
}

#[get("/get_node_info")]
pub fn rrpc__get_node_info(self_node: State<ArMu<node_info::NodeInfo>>) -> Json<node_info::NodeInfo> {
    return Json(chord_util::get_node_info(Arc::clone(&self_node)));
}

// ブラウザからアドレス解決を試すためのエンドポイント
// 与えられた0から100の整数の100分の1をID空間のサイズ（最大値）にかけた
// 値をIDとして、find_successorした結果を返す
// 問い合わせはまず自身に対してかける
#[get("/resolve_id_val?<percentage>")]
pub fn rrpc__resolve_id_val(percentage : String) -> Json<node_info::NodeInfo> {
    let percentage_num: f32 = percentage.parse().unwrap();
    let id = ((percentage_num / 100.0) as f64) * (gval::ID_MAX as f64);
    Json(router::find_successor(ArMu_new!(node_info::NodeInfo::new()), id as u32).unwrap())
}

pub fn rest_api_server_start(self_node: ArMu<node_info::NodeInfo>, data_store: ArMu<data_store::DataStore>, bind_addr: String, bind_port_num: i32){
    let config = Config::build(Environment::Production)
    .address(bind_addr)
    .port(bind_port_num as u16)
    .finalize()
    .unwrap();

    let app = rocket::custom(config);
    
    app.manage(self_node)
       .manage(data_store)
       .mount(
           "/", 
            routes![
                index,
                get_param_test,
                deserialize_test,
                result_type,
                rrpc__check_predecessor,
                rrpc__set_routing_infos_force,
                rrpc__find_successor,
                rrpc__get_node_info,
                rrpc__resolve_id_val
            ]
        )
       .launch();
}


#[derive(Serialize, Deserialize)]
#[derive(Debug, Clone)]
pub struct SetRoutingInfosForce{
    predecessor_info: node_info::NodeInfo,
    successor_info_0: node_info::NodeInfo,
    ftable_enry_0: node_info::NodeInfo
}

impl SetRoutingInfosForce {
    pub fn new(
        predecessor_info: node_info::NodeInfo,
        successor_info_0: node_info::NodeInfo,
        ftable_enry_0: node_info::NodeInfo) -> SetRoutingInfosForce 
    {
        SetRoutingInfosForce {
            predecessor_info: predecessor_info, 
            successor_info_0: successor_info_0,
            ftable_enry_0: ftable_enry_0
        }
    }
}
use std::sync::{Arc, Mutex};
use std::cell::{RefCell, Ref, RefMut};
use std::time::Duration;
use std::collections::HashMap;

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

type ArMu<T> = Arc<Mutex<T>>;

// urlは "http://から始まるものにすること"
fn http_get_request(url_str: &str, address_str: &str, client_pool: ArMu<HashMap<String, ArMu<reqwest::blocking::Client>>>) -> Result<String, chord_util::GeneralError> {
/*
    let mut client_pool_ref = client_pool.lock().unwrap();
    let mut is_reused = false;
    let client_armu = match client_pool_ref.get(&address_str.to_string()){
        None => {            
            let new_client = match reqwest::blocking::Client::builder()
            .pool_max_idle_per_host(usize::MAX)
            .pool_idle_timeout(Duration::from_secs(10000))
            .timeout(Duration::from_secs(10000))
            .build(){
                Err(err) => {
                    chord_util::dprint(&("ERROR at http_get_request(1)".to_string() + url_str));
                    return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR));
                },
                Ok(got_client) => {
                    got_client
                }
            };
            let new_client_armu = ArMu_new!(new_client);
            // clientを使いまわすためにHashMapに格納しておく
            client_pool_ref.insert(address_str.to_string(), Arc::clone(&new_client_armu));
            new_client_armu
        }
        Some(client_armu) => {
            let tmp_client_armu = Arc::clone(&client_armu);
            is_reused = true;
            tmp_client_armu
        }
    };
    drop(client_pool_ref);

    let client = client_armu.lock().unwrap();

    if is_reused {
        println!("reused client: {:?}", *client);
    }
*/
    let client = match reqwest::blocking::Client::builder()
    .pool_max_idle_per_host(3)
    .pool_idle_timeout(None)
    .timeout(Duration::from_secs(10000))
    .build(){
        Err(err) => { 
            chord_util::dprint(&("ERROR at http_post_request(1)".to_string() + url_str));
            return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR));
        },
        Ok(got_client) => got_client
    };

    let resp = match client.get(url_str).send(){
        Err(err) => { 
            chord_util::dprint(&("ERROR at http_get_request(2)".to_string() + url_str));
            return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR));
        },
        Ok(response) => response
    };

    let ret = match resp.text(){
        Err(err) => {
            chord_util::dprint(&("ERROR at http_get_request(3)".to_string() + url_str));
            return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR));
        },
        Ok(text) => text
    };

    return Ok(ret);
}

// urlは "http://から始まるものにすること"
// json_str は JSONの文字列表現をそのまま渡せばよい
fn http_post_request(url_str: &str, address_str: &str, client_pool: ArMu<HashMap<String, ArMu<reqwest::blocking::Client>>>, json_str: String) -> Result<String, chord_util::GeneralError> {
/*
    let mut client_pool_ref = client_pool.lock().unwrap();
    let mut is_reused = false;
    let client_armu = match client_pool_ref.get(&address_str.to_string()){
        None => {            
            let new_client = match reqwest::blocking::Client::builder()
            .pool_max_idle_per_host(usize::MAX)
            .pool_idle_timeout(Duration::from_secs(10000))
            .timeout(Duration::from_secs(10000))
            .build(){
                Err(err) => {
                    chord_util::dprint(&("ERROR at http_post_request(1)".to_string() + url_str));
                    return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR));
                },
                Ok(got_client) => {
                    got_client
                }
            };
            let new_client_armu = ArMu_new!(new_client);
            // clientを使いまわすためにHashMapに格納しておく
            client_pool_ref.insert(address_str.to_string(), Arc::clone(&new_client_armu));
            new_client_armu
        }
        Some(client_armu) => {
            let tmp_client_armu = Arc::clone(&client_armu);
            is_reused = true;
            tmp_client_armu
        }
    };
    drop(client_pool_ref);

    let client = client_armu.lock().unwrap();

    if is_reused {
        println!("reused client: {:?}", *client);
    }
*/    

    let client = match reqwest::blocking::Client::builder()
    .pool_max_idle_per_host(3)
    .pool_idle_timeout(None)
    .timeout(Duration::from_secs(10000))
    .build(){
        Err(err) => { 
            chord_util::dprint(&("ERROR at http_post_request(1)".to_string() + url_str));
            return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR));
        },
        Ok(got_client) => got_client
    };

    let resp = match client.post(url_str).body(json_str).send(){
        Err(err) => {
            chord_util::dprint(&("ERROR at http_post_request(2)".to_string() + url_str));
            return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR));
        },
        Ok(response) => response        
    };

    let ret = match resp.text(){
        Err(err) => {
            chord_util::dprint(&("ERROR at http_post_request(3)".to_string() + url_str));
            return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR));
        },
        Ok(text) => text
    };

    return Ok(ret);
}

#[get("/")]
fn index() -> Json<node_info::NodeInfo> {
    let mut node_info = node_info::NodeInfo::new();
    
    node_info.node_id = 100;
    node_info.address_str = "kanbayashi".to_string();
    node_info.born_id = 77;
    node_info.successor_info_list = vec![];
    node_info.successor_info_list.push(node_info.clone());    
    node_info.predecessor_info = vec![];
    node_info.predecessor_info.push(node_info::partial_clone_from_ref_strong(&node_info));

    Json(node_info)
}

#[get("/result-type")]
fn result_type() -> Json<Result<node_info::NodeInfo, chord_util::GeneralError>> {
    let mut node_info = node_info::NodeInfo::new();
    
    node_info.node_id = 100;
    node_info.address_str = "kanbayashi".to_string();
    node_info.born_id = 77;
    node_info.successor_info_list = vec![];
    node_info.successor_info_list.push(node_info.clone());    
    node_info.predecessor_info = vec![];
    node_info.predecessor_info.push(node_info::partial_clone_from_ref_strong(&node_info));

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

    Json(node_info)
}

#[post("/deserialize", data = "<node_info>")]
pub fn deserialize_test(self_node: State<ArMu<node_info::NodeInfo>>, data_store: State<ArMu<data_store::DataStore>>, node_info: Json<node_info::NodeInfo>) -> String {
    println!("{:?}", self_node.lock().unwrap());
    println!("{:?}", node_info.address_str);
    println!("{:?}", node_info);
    format!("Accepted post request! {:?}", node_info.0)
}

pub fn rrpc_call__check_predecessor(self_node: &node_info::NodeInfo, caller_node_ni: &node_info::NodeInfo, client_pool: ArMu<HashMap<String, ArMu<reqwest::blocking::Client>>>) -> Result<bool, chord_util::GeneralError> {
    let req_rslt = http_post_request(
        &("http://".to_string() + self_node.address_str.as_str() + "/check_predecessor"), self_node.address_str.as_str(), Arc::clone(&client_pool),
        match serde_json::to_string(caller_node_ni){
            Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR)) },
            Ok(text) => text
        });
    
    match req_rslt {
        Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR)) },
        Ok(resp) => { return Ok(true) }
    };
}

#[post("/check_predecessor", data = "<caller_node_ni>")]
pub fn rrpc__check_predecessor(self_node: State<ArMu<node_info::NodeInfo>>, data_store: State<ArMu<data_store::DataStore>>, client_pool: State<ArMu<HashMap<String, ArMu<reqwest::blocking::Client>>>>, caller_node_ni: Json<node_info::NodeInfo>) -> Json<Result<bool, chord_util::GeneralError>> {
    return Json(stabilizer::check_predecessor(Arc::clone(&self_node), Arc::clone(&data_store), Arc::clone(&client_pool), caller_node_ni.0));
}

pub fn rrpc_call__set_routing_infos_force(self_node: &node_info::NodeInfo, predecessor_info: node_info::NodeInfo, successor_info_0: node_info::NodeInfo , ftable_enry_0: node_info::NodeInfo, client_pool: ArMu<HashMap<String, ArMu<reqwest::blocking::Client>>>) -> Result<bool, chord_util::GeneralError> {
    let rpc_arg = SetRoutingInfosForce::new(predecessor_info, successor_info_0, ftable_enry_0);

    let req_rslt = http_post_request(
        &("http://".to_string() + self_node.address_str.as_str() + "/set_routing_infos_force"), self_node.address_str.as_str(), Arc::clone(&client_pool),
        match serde_json::to_string(&rpc_arg){
            Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR)) },
            Ok(text) => text
        });

    return Ok(true);
}

#[post("/set_routing_infos_force", data = "<rpc_args>")]
pub fn rrpc__set_routing_infos_force(self_node: State<ArMu<node_info::NodeInfo>>, rpc_args: Json<SetRoutingInfosForce>, client_pool: State<ArMu<HashMap<String, ArMu<reqwest::blocking::Client>>>>){
    let args = rpc_args.0;
    return stabilizer::set_routing_infos_force(Arc::clone(&self_node), Arc::clone(&client_pool), args.predecessor_info, args.successor_info_0, args.ftable_enry_0);
}

pub fn rrpc_call__find_successor(self_node: &node_info::NodeInfo, client_pool: ArMu<HashMap<String, ArMu<reqwest::blocking::Client>>>, id : u32) -> Result<node_info::NodeInfo, chord_util::GeneralError> {
    let req_rslt = http_post_request(
        &("http://".to_string() + self_node.address_str.as_str() + "/find_successor"), self_node.address_str.as_str(), Arc::clone(&client_pool),
        match serde_json::to_string(&id){
            Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR)) },
            Ok(text) => text
        });
    
    let req_rslt_ref = &(match req_rslt{
        Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR)) },
        Ok(ninfo) => ninfo
    });

    let ret_ninfo = match serde_json::from_str::<Result<node_info::NodeInfo, chord_util::GeneralError>>(req_rslt_ref){
        Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR)) },
        Ok(ninfo) => ninfo
    };

    return ret_ninfo;
}

// idで識別されるデータを担当するノードの名前解決を行う
#[post("/find_successor", data = "<id>")]
pub fn rrpc__find_successor(self_node: State<ArMu<node_info::NodeInfo>>, client_pool: State<ArMu<HashMap<String, ArMu<reqwest::blocking::Client>>>>, id : Json<u32>) -> Json<Result<node_info::NodeInfo, chord_util::GeneralError>> {
    return Json(router::find_successor(Arc::clone(&self_node), Arc::clone(&client_pool), id.0));
}

pub fn rrpc_call__closest_preceding_finger(self_node: &node_info::NodeInfo, client_pool: ArMu<HashMap<String, ArMu<reqwest::blocking::Client>>>, id : u32) -> Result<node_info::NodeInfo, chord_util::GeneralError> {
    let req_rslt = http_post_request(
        &("http://".to_string() + self_node.address_str.as_str() + "/closest_preceding_finger"), self_node.address_str.as_str(), Arc::clone(&client_pool),
        match serde_json::to_string(&id){
            Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR)) },
            Ok(text) => text
        });
    
    let res_text = match req_rslt {
        Err(err) => {
            return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR));
        }
        Ok(text) => {text}
    };

    let ret_ninfo = match match serde_json::from_str::<Result<node_info::NodeInfo, chord_util::GeneralError>>(&res_text){
        Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR))},
        Ok(result_ninfo) => result_ninfo
    }{
        Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR))},
        Ok(ninfo) => ninfo
    };

    return Ok(ret_ninfo);
}

#[post("/closest_preceding_finger", data = "<id>")]
pub fn rrpc__closest_preceding_finger(self_node: State<ArMu<node_info::NodeInfo>>, client_pool: State<ArMu<HashMap<String, ArMu<reqwest::blocking::Client>>>>, id : Json<u32>) -> Json<Result<node_info::NodeInfo, chord_util::GeneralError>> {
    return Json(router::closest_preceding_finger(Arc::clone(&self_node), Arc::clone(&client_pool), id.0));
}

pub fn rrpc_call__global_put(self_node: &node_info::NodeInfo, client_pool: ArMu<HashMap<String, ArMu<reqwest::blocking::Client>>>, key_str: String, val_str: String) -> Result<bool, chord_util::GeneralError> {
    let rpc_arg = GlobalPut::new(key_str, val_str);

    let req_rslt = http_post_request(
        &("http://".to_string() + self_node.address_str.as_str() + "/global_put"), self_node.address_str.as_str(), Arc::clone(&client_pool),
        match serde_json::to_string(&rpc_arg){
            Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR)) },
            Ok(text) => text
        });
    
    let res_text = match req_rslt {
        Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR))},
        Ok(text) => text
    };

    match match serde_json::from_str::<Result<bool, chord_util::GeneralError>>(&res_text){
        Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR))},
        Ok(result) => result
    }{
        Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR))},
        Ok(is_exist) => { return Ok(is_exist); }
    }
}

#[post("/global_put", data = "<rpc_args>")]
pub fn rrpc__global_put(self_node: State<ArMu<node_info::NodeInfo>>, data_store: State<ArMu<data_store::DataStore>>, client_pool: State<ArMu<HashMap<String, ArMu<reqwest::blocking::Client>>>>, rpc_args: Json<GlobalPut>) -> Json<Result<bool, chord_util::GeneralError>> {
    return Json(chord_node::global_put(Arc::clone(&self_node), Arc::clone(&data_store), Arc::clone(&client_pool), rpc_args.0.key_str, rpc_args.0.val_str));
}

pub fn rrpc_call__put(self_node: &node_info::NodeInfo, client_pool: ArMu<HashMap<String, ArMu<reqwest::blocking::Client>>>, key_id: u32, val_str: String) -> Result<bool, chord_util::GeneralError> {
    let rpc_arg = Put::new(key_id, val_str);

    let req_rslt = http_post_request(
        &("http://".to_string() + self_node.address_str.as_str() + "/put"), self_node.address_str.as_str(), Arc::clone(&client_pool),
        match serde_json::to_string(&rpc_arg){
            Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR)) },
            Ok(text) => text
        });
    
    let res_text = match req_rslt {
        Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR))},
        Ok(text) => text
    };

    match match serde_json::from_str::<Result<bool, chord_util::GeneralError>>(&res_text){
        Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR))},
        Ok(result) => result
    }{
        Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR))},
        Ok(is_exist) => { return Ok(is_exist) }
    }
}

#[post("/put", data = "<rpc_args>")]
pub fn rrpc__put(self_node: State<ArMu<node_info::NodeInfo>>, data_store: State<ArMu<data_store::DataStore>>, client_pool: State<ArMu<HashMap<String, ArMu<reqwest::blocking::Client>>>>, rpc_args: Json<Put>) -> Json<Result<bool, chord_util::GeneralError>> {
    return Json(chord_node::put(Arc::clone(&self_node), Arc::clone(&data_store), Arc::clone(&client_pool), rpc_args.0.key_id, rpc_args.0.val_str));
}

pub fn rrpc_call__global_get(self_node: &node_info::NodeInfo, client_pool: ArMu<HashMap<String, ArMu<reqwest::blocking::Client>>>, key_str: String) -> Result<chord_util::DataIdAndValue, chord_util::GeneralError> {
    let req_rslt = http_post_request(
        &("http://".to_string() + self_node.address_str.as_str() + "/global_get"), self_node.address_str.as_str(), Arc::clone(&client_pool),
        match serde_json::to_string(&key_str){
            Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR)) },
            Ok(text) => text
        });
    
    let res_text = match req_rslt {
        Err(err) => {
            return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR));
        }
        Ok(text) => {text}
    };

    let ret_iv = match match serde_json::from_str::<Result<chord_util::DataIdAndValue, chord_util::GeneralError>>(&res_text){
        Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR))},
        Ok(result_iv) => result_iv
    }{
        Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR))},
        Ok(data_iv) => data_iv
    };

    return Ok(ret_iv);
}

#[post("/global_get", data = "<key_str>")]
pub fn rrpc__global_get(self_node: State<ArMu<node_info::NodeInfo>>, data_store: State<ArMu<data_store::DataStore>>, client_pool: State<ArMu<HashMap<String, ArMu<reqwest::blocking::Client>>>>, key_str: Json<String>) -> Json<Result<chord_util::DataIdAndValue, chord_util::GeneralError>> {
    return Json(chord_node::global_get(Arc::clone(&self_node), Arc::clone(&data_store), Arc::clone(&client_pool), key_str.0));
}

pub fn rrpc_call__get(self_node: &node_info::NodeInfo, client_pool: ArMu<HashMap<String, ArMu<reqwest::blocking::Client>>>, key_id: u32) -> Result<chord_util::DataIdAndValue, chord_util::GeneralError> {
    let req_rslt = http_post_request(
        &("http://".to_string() + self_node.address_str.as_str() + "/get"), self_node.address_str.as_str(), Arc::clone(&client_pool),
        match serde_json::to_string(&key_id){
            Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR)) },
            Ok(text) => text
        });
    
    let res_text = match req_rslt {
        Err(err) => {
            return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR));
        }
        Ok(text) => {text}
    };

    let ret_iv = match match serde_json::from_str::<Result<chord_util::DataIdAndValue, chord_util::GeneralError>>(&res_text){
        Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR))},
        Ok(result_iv) => result_iv
    }{
        Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR))},
        Ok(data_iv) => data_iv
    };

    return Ok(ret_iv);
}

#[post("/get", data = "<key_id>")]
pub fn rrpc__get(self_node: State<ArMu<node_info::NodeInfo>>, data_store: State<ArMu<data_store::DataStore>>, client_pool: State<ArMu<HashMap<String, ArMu<reqwest::blocking::Client>>>>, key_id: Json<u32>) -> Json<Result<chord_util::DataIdAndValue, chord_util::GeneralError>> {
    return Json(chord_node::get(Arc::clone(&self_node), Arc::clone(&data_store), key_id.0));
}

pub fn rrpc_call__pass_datas(self_node: &node_info::NodeInfo, client_pool: ArMu<HashMap<String, ArMu<reqwest::blocking::Client>>>, pass_datas: Vec<chord_util::DataIdAndValue>) -> Result<bool, chord_util::GeneralError> {
    let req_rslt = http_post_request(
        &("http://".to_string() + self_node.address_str.as_str() + "/pass_datas"), self_node.address_str.as_str(), Arc::clone(&client_pool),
        match serde_json::to_string(&pass_datas){
            Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR)) },
            Ok(text) => text
        });
    
    let res_text = match req_rslt {
        Err(err) => {
            return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR));
        }
        Ok(text) => {text}
    };

    let ret_bool = match match serde_json::from_str::<Result<bool, chord_util::GeneralError>>(&res_text){
        Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR))},
        Ok(result) => result
    }{
        Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR))},
        Ok(is_successed) => is_successed
    };

    return Ok(ret_bool);
}

#[post("/pass_datas", data = "<pass_datas>")]
pub fn rrpc__pass_datas(self_node: State<ArMu<node_info::NodeInfo>>, data_store: State<ArMu<data_store::DataStore>>, client_pool: State<ArMu<HashMap<String, ArMu<reqwest::blocking::Client>>>>, pass_datas: Json<Vec<chord_util::DataIdAndValue>>) -> Json<Result<bool, chord_util::GeneralError>> {
    return Json(stabilizer::pass_datas(Arc::clone(&self_node), Arc::clone(&data_store), Arc::clone(&client_pool), pass_datas.0));
}

pub fn rrpc_call__global_delete(self_node: &node_info::NodeInfo, client_pool: ArMu<HashMap<String, ArMu<reqwest::blocking::Client>>>, key_str: String) -> Result<bool, chord_util::GeneralError> {
    let req_rslt = http_post_request(
        &("http://".to_string() + self_node.address_str.as_str() + "/global_delete"), self_node.address_str.as_str(), Arc::clone(&client_pool),
        match serde_json::to_string(&key_str){
            Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR)) },
            Ok(text) => text
        });
    
    let res_text = match req_rslt {
        Err(err) => {
            return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR));
        }
        Ok(text) => {text}
    };

    let is_exist = match match serde_json::from_str::<Result<bool, chord_util::GeneralError>>(&res_text){
        Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR))},
        Ok(result_bool) => result_bool
    }{
        Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR))},
        Ok(wrapped_bool) => wrapped_bool
    };

    return Ok(is_exist);
}

#[post("/global_delete", data = "<key_str>")]
pub fn rrpc__global_delete(self_node: State<ArMu<node_info::NodeInfo>>, data_store: State<ArMu<data_store::DataStore>>, client_pool: State<ArMu<HashMap<String, ArMu<reqwest::blocking::Client>>>>, key_str: Json<String>) -> Json<Result<bool, chord_util::GeneralError>> {
    return Json(chord_node::global_delete(Arc::clone(&self_node), Arc::clone(&data_store), Arc::clone(&client_pool), key_str.0));
}

pub fn rrpc_call__get_node_info(address : &String, client_pool: ArMu<HashMap<String, ArMu<reqwest::blocking::Client>>>) -> Result<node_info::NodeInfo, GeneralError> {
    let req_rslt = http_get_request(&("http://".to_string() + address.as_str() + "/get_node_info"), address.as_str(), Arc::clone(&client_pool));
    let ret_ninfo = match serde_json::from_str::<node_info::NodeInfo>(&(
        match req_rslt{
            Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR)) },
            Ok(text) => text
        }
    )){
        Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR)) },
        Ok(ninfo) => ninfo
    };

    return Ok(ret_ninfo);
}

#[get("/get_node_info")]
pub fn rrpc__get_node_info(self_node: State<ArMu<node_info::NodeInfo>>, client_pool: State<ArMu<HashMap<String, ArMu<reqwest::blocking::Client>>>>) -> Json<node_info::NodeInfo> {
    return Json(chord_util::get_node_info(Arc::clone(&self_node), Arc::clone(&client_pool)));
}

// ブラウザから試すためのエンドポイント
#[get("/global_put_simple?<key>&<val>")]
pub fn rrpc__global_put_simple(self_node: State<ArMu<node_info::NodeInfo>>, data_store: State<ArMu<data_store::DataStore>>, client_pool: State<ArMu<HashMap<String, ArMu<reqwest::blocking::Client>>>>, key: String, val: String) -> Json<Result<bool, chord_util::GeneralError>> {
    return Json(chord_node::global_put(Arc::clone(&self_node), Arc::clone(&data_store), Arc::clone(&client_pool), key, val));
}

// ブラウザから試すためのエンドポイント
#[get("/global_get_simple?<key>")]
pub fn rrpc__global_get_simple(self_node: State<ArMu<node_info::NodeInfo>>, data_store: State<ArMu<data_store::DataStore>>, client_pool: State<ArMu<HashMap<String, ArMu<reqwest::blocking::Client>>>>, key: String) -> Json<Result<chord_util::DataIdAndValue, chord_util::GeneralError>> {
    return Json(chord_node::global_get(Arc::clone(&self_node), Arc::clone(&data_store), Arc::clone(&client_pool), key));
}

// ブラウザから試すためのエンドポイント
#[get("/global_delete_simple?<key>")]
pub fn rrpc__global_delete_simple(self_node: State<ArMu<node_info::NodeInfo>>, data_store: State<ArMu<data_store::DataStore>>, client_pool: State<ArMu<HashMap<String, ArMu<reqwest::blocking::Client>>>>, key: String) -> Json<Result<bool, chord_util::GeneralError>> {
    return Json(chord_node::global_delete(Arc::clone(&self_node), Arc::clone(&data_store), Arc::clone(&client_pool), key));
}

// ブラウザからアドレス解決を試すためのエンドポイント
// 与えられた0から100の整数の100分の1をID空間のサイズ（最大値）にかけた
// 値をIDとして、find_successorした結果を返す
// 問い合わせはまず自身に対してかける
#[get("/resolve_id_val?<percentage>")]
pub fn rrpc__resolve_id_val(self_node: State<ArMu<node_info::NodeInfo>>, client_pool: State<ArMu<HashMap<String, ArMu<reqwest::blocking::Client>>>>, percentage : String) -> Json<node_info::NodeInfo> {
    let percentage_num: f32 = percentage.parse().unwrap();
    let id = ((percentage_num / 100.0) as f64) * (gval::ID_MAX as f64);
    Json(router::find_successor(Arc::clone(&self_node), Arc::clone(&client_pool), id as u32).unwrap())
}

pub fn rest_api_server_start(self_node: ArMu<node_info::NodeInfo>, data_store: ArMu<data_store::DataStore>, client_pool: ArMu<HashMap<String, ArMu<reqwest::blocking::Client>>>, bind_addr: String, bind_port_num: i32){
    let config = Config::build(Environment::Production)
    .address(bind_addr)
    .port(bind_port_num as u16)
    .workers(30)
    .keep_alive(10000)
    .read_timeout(10000)
    .write_timeout(10000)
    .limits(rocket::config::Limits::new().limit("forms", u64::MAX).limit("json", u64::MAX))
    .finalize()
    .unwrap();

    let app = rocket::custom(config);
    
    app.manage(self_node)
       .manage(data_store)
       .manage(client_pool)
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
                rrpc__closest_preceding_finger,                
                rrpc__get_node_info,
                rrpc__resolve_id_val,
                rrpc__global_put,
                rrpc__put,
                rrpc__global_get,
                rrpc__get,
                rrpc__global_delete,
                rrpc__global_put_simple,
                rrpc__global_get_simple,
                rrpc__global_delete_simple
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

#[derive(Serialize, Deserialize)]
#[derive(Debug, Clone)]
pub struct GlobalPut {
    key_str: String,
    val_str: String
}

impl GlobalPut {
    pub fn new(
        key_str: String,
        val_str: String) -> GlobalPut
    {
        GlobalPut {
            key_str: key_str, 
            val_str: val_str
        }
    }
}

#[derive(Serialize, Deserialize)]
#[derive(Debug, Clone)]
pub struct Put {
    key_id: u32,
    val_str: String
}

impl Put {
    pub fn new(
        key_id: u32,
        val_str: String) -> Put
    {
        Put {
            key_id: key_id, 
            val_str: val_str
        }
    }
}
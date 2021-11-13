use std::sync::{Arc, Mutex};
use std::cell::{RefCell, Ref, RefMut};
use std::time::Duration;
use std::collections::HashMap;
use std::net::SocketAddr;

use rocket_contrib::json::Json;
use rocket::State;
use rocket::config::{Config, Environment};
use reqwest::Error;
use serde::{Serialize, Deserialize};
use tokio::runtime::Runtime;

use chord_util::GeneralError;

use tonic::{transport::Server, Request, Response, Status};
use prost_types::Any;
use prost::Message;
use crate::rustdkvs::rust_dkvs_server::{RustDkvs, RustDkvsServer};
use crate::rustdkvs::rust_dkvs_client::RustDkvsClient;

use crate::rustdkvs::{NodeInfo, Uint32, RString, Void, Bool, DataIdAndValue, PassDatas, NodeInfoSummary};

use crate::gval;
use crate::chord_node;
use crate::node_info;
use crate::chord_util;
use crate::data_store;
use crate::router;
use crate::stabilizer;

type ArMu<T> = Arc<Mutex<T>>;

//#[derive(Debug, Default)]
#[derive(Debug)]
pub struct MyRustDKVS {
    self_node: ArMu<node_info::NodeInfo>,
    data_store: ArMu<data_store::DataStore>,
    client_pool: ArMu<HashMap<String, ArMu<reqwest::Client>>>
}

pub async fn get_grpc_client(address: &String) -> Result<crate::rustdkvs::rust_dkvs_client::RustDkvsClient<tonic::transport::Channel>, chord_util::GeneralError> {
    let ret = match RustDkvsClient::connect("http://".to_string() + address.as_str()).await {
        Ok(client) => { Ok(client) }
        Err(err) => async {
            let mut retry_cnt = 0; 
            let ret_client;
            loop {
                let tmp_client = RustDkvsClient::connect("http://".to_string() + address.as_str()).await;
                if tmp_client.is_ok() {
                    ret_client = tmp_client.unwrap();
                    break;
                }else{
                    println!{"Socket Error Occured: {:?}", err};
                    retry_cnt += 1;
                    if retry_cnt == 10 {
                        return Err(chord_util::GeneralError::new("socket error retry 3count reached".to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR));
                    }
                    std::thread::sleep(std::time::Duration::from_millis(1000 as u64));
                    continue;
                }
            }
            Ok(ret_client)
        }.await
    }; //?;
    return ret;
}


// urlは "http://から始まるものにすること"
async fn http_get_request(url_str: &str, address_str: &str, client_pool: ArMu<HashMap<String, ArMu<reqwest::Client>>>) -> Result<String, chord_util::GeneralError> {
/*
    let mut client_pool_ref = client_pool.lock().unwrap();
    let mut is_reused = false;
    let client_armu = match client_pool_ref.get(&address_str.to_string()){
        None => {            
            let new_client = match reqwest::Client::builder()
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
    let client = match reqwest::Client::builder()
    .pool_max_idle_per_host(256)
    .pool_idle_timeout(Duration::from_secs(10000))
    .timeout(Duration::from_secs(10000))
    .build(){
        Err(err) => { 
            chord_util::dprint(&("ERROR at http_post_request(1)".to_string() + url_str));
            return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR));
        },
        Ok(got_client) => got_client
    };

    let resp = match client.get(url_str).header(reqwest::header::CONTENT_TYPE, "application/json").send().await {
        Err(err) => { 
            chord_util::dprint(&("ERROR at http_get_request(2)".to_string() + url_str));
            return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR));
        },
        Ok(response) => response
    };

    let ret = match resp.text().await {
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
async fn http_post_request(url_str: &str, address_str: &str, client_pool: ArMu<HashMap<String, ArMu<reqwest::Client>>>, json_str: String) -> Result<String, chord_util::GeneralError> {
/*
    let mut client_pool_ref = client_pool.lock().unwrap();
    let mut is_reused = false;
    let client_armu = match client_pool_ref.get(&address_str.to_string()){
        None => {            
            let new_client = match reqwest::Client::builder()
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

    let client = match reqwest::Client::builder()
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

    let resp = match client.post(url_str).header(reqwest::header::CONTENT_TYPE, "application/json").body(json_str).send().await {
        Err(err) => {
            chord_util::dprint(&("ERROR at http_post_request(2)".to_string() + url_str));
            return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR));
        },
        Ok(response) => response        
    };

    let ret = match resp.text().await {
        Err(err) => {
            chord_util::dprint(&("ERROR at http_post_request(3)".to_string() + url_str));
            return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR));
        },
        Ok(text) => text
    };

    return Ok(ret);
}

pub async fn rrpc_call__check_predecessor(self_node: &node_info::NodeInfo, caller_node_ni: &node_info::NodeInfo, client_pool: ArMu<HashMap<String, ArMu<reqwest::Client>>>, caller_id: u32) -> Result<bool, chord_util::GeneralError> {
    // 呼び出し元で対処しているため、ここでの自ノードへの呼出しへの対処は不要
    
    let mut client = get_grpc_client(&self_node.address_str).await?;

    //let request = tonic::Request::new(conv_node_info_to_grpc_one((*caller_node_ni).clone()));
    let request = tonic::Request::new(crate::rustdkvs::NodeInfoSummary { node_id: caller_node_ni.node_id, succ0_id: caller_node_ni.node_id, address_str: caller_node_ni.address_str.clone()});
    
    let response = client.grpc_check_predecessor(request).await;
    //println!("RESPONSE={:?}", response);
    return Ok(response.unwrap().into_inner().val);
}

// pub async fn rrpc_call__check_predecessor(self_node: &node_info::NodeInfo, caller_node_ni: &node_info::NodeInfo, client_pool: ArMu<HashMap<String, ArMu<reqwest::Client>>>) -> Result<bool, chord_util::GeneralError> {
//     let tmp_url_str_ref = &("http://".to_string() + self_node.address_str.as_str() + "/check_predecessor");
//     let req_rslt = http_post_request(tmp_url_str_ref
//         , self_node.address_str.as_str(), Arc::clone(&client_pool),
//         match serde_json::to_string(caller_node_ni){
//             Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR)) },
//             Ok(text) => text
//         });
    
//     match req_rslt.await {
//         Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR)) },
//         Ok(resp) => { return Ok(true) }
//     };        
// }

pub async fn rrpc_call__set_routing_infos_force(self_node: &node_info::NodeInfo, predecessor_info: node_info::NodeInfo, successor_info_0: node_info::NodeInfo , ftable_enry_0: node_info::NodeInfo, client_pool: ArMu<HashMap<String, ArMu<reqwest::Client>>>) -> Result<bool, chord_util::GeneralError> {
    let mut client = get_grpc_client(&self_node.address_str).await?;

    let request = tonic::Request::new(
        crate::rustdkvs::SetRoutingInfosForce {
            predecessor_info: Some(conv_node_info_to_grpc_one(predecessor_info)),
            successor_info_0: Some(conv_node_info_to_grpc_one(successor_info_0)),
            ftable_enry_0: Some(conv_node_info_to_grpc_one(ftable_enry_0))
        } 
    );
    
    let response = client.grpc_set_routing_infos_force(request).await;
    //println!("RESPONSE={:?}", response);
    return Ok(response.unwrap().into_inner().val);
}

// pub async fn rrpc_call__set_routing_infos_force(self_node: &node_info::NodeInfo, predecessor_info: node_info::NodeInfo, successor_info_0: node_info::NodeInfo , ftable_enry_0: node_info::NodeInfo, client_pool: ArMu<HashMap<String, ArMu<reqwest::Client>>>) -> Result<bool, chord_util::GeneralError> {
//     let rpc_arg = SetRoutingInfosForce::new(predecessor_info, successor_info_0, ftable_enry_0);
//     let tmp_url_str_ref = &("http://".to_string() + self_node.address_str.as_str() + "/set_routing_infos_force");
//     let req_rslt = http_post_request(
//         tmp_url_str_ref, self_node.address_str.as_str(), Arc::clone(&client_pool),
//         match serde_json::to_string(&rpc_arg){
//             Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR)) },
//             Ok(text) => text
//         });

//     return Ok(true);
// }

pub async fn rrpc_call__find_successor(self_node: &node_info::NodeInfo, client_pool: ArMu<HashMap<String, ArMu<reqwest::Client>>>, id : u32, caller_id: u32) -> Result<node_info::NodeInfoSummary, chord_util::GeneralError> {
    if self_node.node_id == caller_id {
        return router::find_successor(ArMu_new!(node_info::partial_clone_from_ref_strong(self_node)), client_pool, id).await;
    }
    
    let mut client = get_grpc_client(&self_node.address_str).await?;

    let request = tonic::Request::new(Uint32 { val: id});
    
    let response = client.grpc_find_successor(request).await.unwrap().into_inner(); //?;
    //println!("RESPONSE={:?}", response);
    //return Ok(node_info::gen_summary_node_info(&conv_node_info_to_normal_one(response.unwrap().into_inner())));
    return Ok(node_info::NodeInfoSummary { node_id: response.node_id, succ0_id: response.node_id, address_str: response.address_str.clone()});
}

// pub async fn rrpc_call__find_successor(self_node: &node_info::NodeInfo, client_pool: ArMu<HashMap<String, ArMu<reqwest::Client>>>, id : u32) -> Result<node_info::NodeInfo, chord_util::GeneralError> {
//     let tmp_url_str_ref = &("http://".to_string() + self_node.address_str.as_str() + "/find_successor");
//     let req_rslt = http_post_request(
//         tmp_url_str_ref, self_node.address_str.as_str(), Arc::clone(&client_pool),
//         match serde_json::to_string(&id){
//             Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR)) },
//             Ok(text) => text
//         });
    
//     let req_rslt_ref = &(match req_rslt.await {
//         Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR)) },
//         Ok(ninfo) => ninfo
//     });

//     let ret_ninfo = match serde_json::from_str::<Result<node_info::NodeInfo, chord_util::GeneralError>>(req_rslt_ref){
//         Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR)) },
//         Ok(ninfo) => ninfo
//     };

//     return ret_ninfo;
// }

pub async fn rrpc_call__closest_preceding_finger(self_node: &node_info::NodeInfo, client_pool: ArMu<HashMap<String, ArMu<reqwest::Client>>>, id : u32, caller_id: u32) -> Result<node_info::NodeInfoSummary, chord_util::GeneralError> {
    if self_node.node_id == caller_id {
        return router::closest_preceding_finger(ArMu_new!(node_info::partial_clone_from_ref_strong(self_node)), client_pool, id).await;
    }
    
    let mut client = get_grpc_client(&self_node.address_str).await?;

    let request = tonic::Request::new(Uint32 { val: id});
    
    let response = client.grpc_closest_preceding_finger(request).await.unwrap().into_inner(); //?;
    //println!("RESPONSE={:?}", response);
    //return Ok(node_info::gen_summary_node_info(&conv_node_info_to_normal_one(response.unwrap().into_inner())));
    return Ok(node_info::NodeInfoSummary { node_id: response.node_id, succ0_id: response.succ0_id, address_str: response.address_str.clone()});
}

// pub async fn rrpc_call__closest_preceding_finger(self_node: &node_info::NodeInfo, client_pool: ArMu<HashMap<String, ArMu<reqwest::Client>>>, id : u32) -> Result<node_info::NodeInfo, chord_util::GeneralError> {
//     let tmp_url_str_ref = &("http://".to_string() + self_node.address_str.as_str() + "/closest_preceding_finger");
//     let req_rslt = http_post_request(
//         tmp_url_str_ref, self_node.address_str.as_str(), Arc::clone(&client_pool),
//         match serde_json::to_string(&id){
//             Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR)) },
//             Ok(text) => text
//         });
    
//     let res_text = match req_rslt.await {
//         Err(err) => {
//             return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR));
//         }
//         Ok(text) => {text}
//     };

//     let ret_ninfo = match match serde_json::from_str::<Result<node_info::NodeInfo, chord_util::GeneralError>>(&res_text){
//         Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR))},
//         Ok(result_ninfo) => result_ninfo
//     }{
//         Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR))},
//         Ok(ninfo) => ninfo
//     };

//     return Ok(ret_ninfo);
// }

// global_put の grpc は呼び出す箇所がないので実装不要

// pub async fn rrpc_call__global_put(self_node: &node_info::NodeInfo, client_pool: ArMu<HashMap<String, ArMu<reqwest::Client>>>, key_str: String, val_str: String) -> Result<bool, chord_util::GeneralError> {
//     let rpc_arg = GlobalPut::new(key_str, val_str);
//     let tmp_url_str_ref = &("http://".to_string() + self_node.address_str.as_str() + "/global_put");
//     let req_rslt = http_post_request(
//         tmp_url_str_ref, self_node.address_str.as_str(), Arc::clone(&client_pool),
//         match serde_json::to_string(&rpc_arg){
//             Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR)) },
//             Ok(text) => text
//         });
    
//     let res_text = match req_rslt.await {
//         Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR))},
//         Ok(text) => text
//     };

//     match match serde_json::from_str::<Result<bool, chord_util::GeneralError>>(&res_text){
//         Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR))},
//         Ok(result) => result
//     }{
//         Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR))},
//         Ok(is_exist) => { return Ok(is_exist); }
//     }
// }

pub async fn rrpc_call__put(self_node: &node_info::NodeInfo, data_store: ArMu<data_store::DataStore>, client_pool: ArMu<HashMap<String, ArMu<reqwest::Client>>>, key_id: u32, val_str: String, caller_id: u32) -> Result<bool, chord_util::GeneralError> {
    if self_node.node_id == caller_id {
        return chord_node::put(ArMu_new!(node_info::partial_clone_from_ref_strong(self_node)), Arc::clone(&data_store), client_pool, key_id, val_str);
    }
    
    let mut client = get_grpc_client(&self_node.address_str).await?;

    let request = tonic::Request::new(
        crate::rustdkvs::Put { 
            key_id: key_id,
            val_str: val_str
        }
    );
    
    let response = client.grpc_put(request).await; //?;
    //println!("RESPONSE={:?}", response);
    return Ok(response.unwrap().into_inner().val);
}

// pub async fn rrpc_call__put(self_node: &node_info::NodeInfo, client_pool: ArMu<HashMap<String, ArMu<reqwest::Client>>>, key_id: u32, val_str: String) -> Result<bool, chord_util::GeneralError> {
//     let rpc_arg = Put::new(key_id, val_str);
//     let tmp_url_str_ref = &("http://".to_string() + self_node.address_str.as_str() + "/put");
//     let req_rslt = http_post_request(
//         tmp_url_str_ref, self_node.address_str.as_str(), Arc::clone(&client_pool),
//         match serde_json::to_string(&rpc_arg){
//             Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR)) },
//             Ok(text) => text
//         });
    
//     let res_text = match req_rslt.await {
//         Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR))},
//         Ok(text) => text
//     };

//     match match serde_json::from_str::<Result<bool, chord_util::GeneralError>>(&res_text){
//         Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR))},
//         Ok(result) => result
//     }{
//         Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR))},
//         Ok(is_exist) => { return Ok(is_exist) }
//     }
// }

// global_get の grpc は呼び出し箇所がないため実装不要

// pub async fn rrpc_call__global_get(self_node: &node_info::NodeInfo, client_pool: ArMu<HashMap<String, ArMu<reqwest::Client>>>, key_str: String) -> Result<chord_util::DataIdAndValue, chord_util::GeneralError> {
//     let tmp_url_str_ref = &("http://".to_string() + self_node.address_str.as_str() + "/global_get");
//     let req_rslt = http_post_request(
//         tmp_url_str_ref, self_node.address_str.as_str(), Arc::clone(&client_pool),
//         match serde_json::to_string(&key_str){
//             Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR)) },
//             Ok(text) => text
//         });
    
//     let res_text = match req_rslt.await {
//         Err(err) => {
//             return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR));
//         }
//         Ok(text) => {text}
//     };

//     let ret_iv = match match serde_json::from_str::<Result<chord_util::DataIdAndValue, chord_util::GeneralError>>(&res_text){
//         Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR))},
//         Ok(result_iv) => result_iv
//     }{
//         Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR))},
//         Ok(data_iv) => data_iv
//     };

//     return Ok(ret_iv);
// }

pub async fn rrpc_call__get(self_node: &node_info::NodeInfo, data_store: ArMu<data_store::DataStore>, client_pool: ArMu<HashMap<String, ArMu<reqwest::Client>>>, key_id: u32, caller_id: u32) -> Result<chord_util::DataIdAndValue, chord_util::GeneralError> {
    if self_node.node_id == caller_id {

        return chord_node::get(ArMu_new!(node_info::partial_clone_from_ref_strong(self_node)), Arc::clone(&data_store), key_id);
    }
    let mut client = get_grpc_client(&self_node.address_str).await?;

    let request = tonic::Request::new(Uint32 { val: key_id});
    
    let response = client.grpc_get(request).await; //?;
    //println!("RESPONSE={:?}", response);
    return Ok(conv_iv_to_normal_one(response.unwrap().into_inner()));
}

// pub async fn rrpc_call__get(self_node: &node_info::NodeInfo, client_pool: ArMu<HashMap<String, ArMu<reqwest::Client>>>, key_id: u32) -> Result<chord_util::DataIdAndValue, chord_util::GeneralError> {
//     let tmp_url_str_ref = &("http://".to_string() + self_node.address_str.as_str() + "/get");
//     let req_rslt = http_post_request(
//         tmp_url_str_ref, self_node.address_str.as_str(), Arc::clone(&client_pool),
//         match serde_json::to_string(&key_id){
//             Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR)) },
//             Ok(text) => text
//         });
    
//     let res_text = match req_rslt.await {
//         Err(err) => {
//             return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR));
//         }
//         Ok(text) => {text}
//     };

//     let ret_iv = match match serde_json::from_str::<Result<chord_util::DataIdAndValue, chord_util::GeneralError>>(&res_text){
//         Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR))},
//         Ok(result_iv) => result_iv
//     }{
//         Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR))},
//         Ok(data_iv) => data_iv
//     };

//     return Ok(ret_iv);
// }

pub async fn rrpc_call__pass_datas(self_node: &node_info::NodeInfo, client_pool: ArMu<HashMap<String, ArMu<reqwest::Client>>>, pass_datas: Vec<chord_util::DataIdAndValue>, caller_id: u32) -> Result<bool, chord_util::GeneralError> {
    // 呼び出し元で対処しているため、ここでの自ノードへの呼び出しへの対処は不要
    let mut client = get_grpc_client(&self_node.address_str).await?;

    let request = tonic::Request::new(
        crate::rustdkvs::PassDatas {
            vals: conv_iv_vec_to_grpc_one(pass_datas)
        } 
    );
    
    let response = client.grpc_pass_datas(request).await; //?;
    //println!("RESPONSE={:?}", response);
    return Ok(response.unwrap().into_inner().val);
}

// pub async fn rrpc_call__pass_datas(self_node: &node_info::NodeInfo, client_pool: ArMu<HashMap<String, ArMu<reqwest::Client>>>, pass_datas: Vec<chord_util::DataIdAndValue>) -> Result<bool, chord_util::GeneralError> {
//     let tmp_url_str_ref = &("http://".to_string() + self_node.address_str.as_str() + "/pass_datas");
//     let req_rslt = http_post_request(
//         tmp_url_str_ref, self_node.address_str.as_str(), Arc::clone(&client_pool),
//         match serde_json::to_string(&pass_datas){
//             Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR)) },
//             Ok(text) => text
//         });
    
//     let res_text = match req_rslt.await {
//         Err(err) => {
//             return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR));
//         }
//         Ok(text) => {text}
//     };

//     let ret_bool = match match serde_json::from_str::<Result<bool, chord_util::GeneralError>>(&res_text){
//         Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR))},
//         Ok(result) => result
//     }{
//         Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR))},
//         Ok(is_successed) => is_successed
//     };

//     return Ok(ret_bool);
// }

// global_delete の grpc は呼び出し箇所がないため実装不要

// pub async fn rrpc_call__global_delete(self_node: &node_info::NodeInfo, client_pool: ArMu<HashMap<String, ArMu<reqwest::Client>>>, key_str: String) -> Result<bool, chord_util::GeneralError> {
//     let tmp_url_str_ref = &("http://".to_string() + self_node.address_str.as_str() + "/global_delete");
//     let req_rslt = http_post_request(
//         tmp_url_str_ref, self_node.address_str.as_str(), Arc::clone(&client_pool),
//         match serde_json::to_string(&key_str){
//             Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR)) },
//             Ok(text) => text
//         });
    
//     let res_text = match req_rslt.await {
//         Err(err) => {
//             return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR));
//         }
//         Ok(text) => {text}
//     };

//     let is_exist = match match serde_json::from_str::<Result<bool, chord_util::GeneralError>>(&res_text){
//         Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR))},
//         Ok(result_bool) => result_bool
//     }{
//         Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR))},
//         Ok(wrapped_bool) => wrapped_bool
//     };

//     return Ok(is_exist);
// }

// pub async fn grpc_call_test_get_node_info(address : &String) -> node_info::NodeInfo { //, client_pool: ArMu<HashMap<String, ArMu<reqwest::Client>>>){
//     //let mut client = RustDkvsClient::connect("http://".to_string() + address.as_str()).await.unwrap(); //?;
//     let mut client = get_grpc_client(address).await?;

//     let request = tonic::Request::new(RString {
//         val: "it is sunny!".into()
//     });
    
//     let response = client.grpc_test_get_node_info(request).await; //?;
//     //println!("RESPONSE={:?}", response);
//     return conv_node_info_to_normal_one(response.unwrap().into_inner());
// }

pub async fn rrpc_call__get_node_info(self_node: &node_info::NodeInfoSummary, client_pool: ArMu<HashMap<String, ArMu<reqwest::Client>>>, caller_id: u32, caller_ni: &node_info::NodeInfo) -> Result<node_info::NodeInfo, GeneralError> {
    if self_node.node_id == caller_id {
        return Ok(chord_util::get_node_info(ArMu_new!(node_info::partial_clone_from_ref_strong(caller_ni)), client_pool));
    }
    let mut client = get_grpc_client(&self_node.address_str).await?;

    let request = tonic::Request::new(Void {
        val: 0
    });
    
    let response = client.grpc_get_node_info(request).await; //?;
    //println!("RESPONSE={:?}", response);
    let ret_ni = response.unwrap().into_inner();
    let ret_ni_conved = conv_node_info_to_normal_one(ret_ni);

    return Ok(ret_ni_conved);
}

// pub async fn rrpc_call__get_node_info(address : &String, client_pool: ArMu<HashMap<String, ArMu<reqwest::Client>>>) -> Result<node_info::NodeInfo, GeneralError> {
//     let tmp_url_str_ref = &("http://".to_string() + address.as_str() + "/get_node_info");
//     let req_rslt = http_get_request(tmp_url_str_ref, address.as_str(), Arc::clone(&client_pool));
//     let ret_ninfo = match serde_json::from_str::<node_info::NodeInfo>(&(
//         match req_rslt.await {
//             Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR)) },
//             Ok(text) => text
//         }
//     )){
//         Err(err) => { return Err(chord_util::GeneralError::new(err.to_string(), chord_util::ERR_CODE_HTTP_REQUEST_ERR)) },
//         Ok(ninfo) => ninfo
//     };

//     return Ok(ret_ninfo);
// }

#[tonic::async_trait]
impl RustDkvs for MyRustDKVS {
    async fn grpc_test_get_node_info(
        &self,
        request: Request<RString>, // Accept request of type RString
    ) -> Result<Response<NodeInfo>, Status> { // Return an instance of type rustdkvs::NodeInfo
        //println!("Got a request: {:?}", request);

        //chord_util::get_node_info(Arc::clone(&self_node), Arc::clone(&client_pool));

        let empty_ni = node_info::NodeInfo::new();

        // We must use .into_inner() as the fields of gRPC requests and responses are private
        let reply = conv_node_info_to_grpc_one(empty_ni.clone());

        Ok(Response::new(reply)) // Send back our formatted message
    }

    async fn grpc_check_predecessor(
        &self,
        request: Request<crate::rustdkvs::NodeInfoSummary>,
    ) -> Result<Response<Bool>, Status> {
        //println!("Got a request: {:?}", request);

        let req_tmp = request.into_inner();
        let reply_tmp = stabilizer::check_predecessor(Arc::clone(&self.self_node), Arc::clone(&self.data_store), Arc::clone(&self.client_pool), node_info::NodeInfoSummary { node_id: req_tmp.node_id, succ0_id: req_tmp.node_id, address_str: req_tmp.address_str.clone()}).await.unwrap();
        let reply = Bool { val: reply_tmp };

        Ok(Response::new(reply))
    }

    async fn grpc_set_routing_infos_force(
        &self,
        request: Request<crate::rustdkvs::SetRoutingInfosForce>,
    ) -> Result<Response<Bool>, Status> {
        println!("Got a request: {:?}", request);
        let srif_obj = request.into_inner();
        stabilizer::set_routing_infos_force(Arc::clone(&self.self_node), Arc::clone(&self.client_pool), conv_node_info_to_normal_one(srif_obj.predecessor_info.unwrap()), conv_node_info_to_normal_one(srif_obj.successor_info_0.unwrap()), conv_node_info_to_normal_one(srif_obj.ftable_enry_0.unwrap()));
        let reply = Bool { val: true };
        Ok(Response::new(reply))
    }
    
    async fn grpc_find_successor(
        &self,
        request: Request<Uint32>,
    ) -> Result<Response<crate::rustdkvs::NodeInfoSummary>, Status> {
        println!("Got a request: {:?}", request);

        let reply_tmp = router::find_successor(Arc::clone(&self.self_node), Arc::clone(&self.client_pool), request.into_inner().val).await.unwrap();
        let reply = crate::rustdkvs::NodeInfoSummary { node_id: reply_tmp.node_id, succ0_id: reply_tmp.succ0_id, address_str: reply_tmp.address_str.clone() };
        //let reply = node_info::gen_summary_node_info() conv_node_info_to_grpc_one(node_info::gen_node_info_from_summary(&reply_tmp.unwrap()));
        Ok(Response::new(reply))
    }

    async fn grpc_closest_preceding_finger(
        &self,
        request: Request<Uint32>,
    ) -> Result<Response<crate::rustdkvs::NodeInfoSummary>, Status> {
        //println!("Got a request: {:?}", request);

        let reply_tmp = router::closest_preceding_finger(Arc::clone(&self.self_node), Arc::clone(&self.client_pool), request.into_inner().val).await.unwrap();
        let reply = crate::rustdkvs::NodeInfoSummary { node_id: reply_tmp.node_id, succ0_id: reply_tmp.succ0_id, address_str: reply_tmp.address_str.clone() };
        //let reply = conv_node_info_to_grpc_one(reply_tmp.unwrap());
        Ok(Response::new(reply))
    }
    
    async fn grpc_global_put(
        &self,
        request: Request<crate::rustdkvs::GlobalPut>,
    ) -> Result<Response<Bool>, Status> {
        //println!("Got a request: {:?}", request);

        let gp_val = request.into_inner();
        let reply_tmp = chord_node::global_put(Arc::clone(&self.self_node), Arc::clone(&self.data_store), Arc::clone(&self.client_pool), gp_val.key_str, gp_val.val_str).await;
        let reply = Bool { val: reply_tmp.unwrap() };
        Ok(Response::new(reply))
    }

    async fn grpc_put(
        &self,
        request: Request<crate::rustdkvs::Put>,
    ) -> Result<Response<Bool>, Status> {
        //println!("Got a request: {:?}", request);

        let gp_val = request.into_inner();
        let reply_tmp = chord_node::put(Arc::clone(&self.self_node), Arc::clone(&self.data_store), Arc::clone(&self.client_pool), gp_val.key_id, gp_val.val_str);
        let reply = Bool { val: reply_tmp.unwrap() };
        Ok(Response::new(reply))
    }

    async fn grpc_global_get(
        &self,
        request: Request<RString>,
    ) -> Result<Response<crate::rustdkvs::DataIdAndValue>, Status> {
        //println!("Got a request: {:?}", request);

        let reply_tmp = chord_node::global_get(Arc::clone(&self.self_node), Arc::clone(&self.data_store), Arc::clone(&self.client_pool), request.into_inner().val).await;
        let reply = conv_iv_to_grpc_one(reply_tmp.unwrap());
        Ok(Response::new(reply))
    }

    async fn grpc_get(
        &self,
        request: Request<Uint32>,
    ) -> Result<Response<crate::rustdkvs::DataIdAndValue>, Status> {
        //println!("Got a request: {:?}", request);

        let reply_tmp = chord_node::get(Arc::clone(&self.self_node), Arc::clone(&self.data_store), request.into_inner().val);
        let reply = conv_iv_to_grpc_one(reply_tmp.unwrap());
        Ok(Response::new(reply))
    }

    async fn grpc_pass_datas(
        &self,
        request: Request<PassDatas>,
    ) -> Result<Response<Bool>, Status> {
        //println!("Got a request: {:?}", request);

        let iv_vec_tmp = request.into_inner();
        let reply_tmp = stabilizer::pass_datas(Arc::clone(&self.self_node), Arc::clone(&self.data_store), Arc::clone(&self.client_pool), conv_iv_vec_to_normal_one(iv_vec_tmp.vals));
        let reply = Bool { val: reply_tmp.unwrap() };
        Ok(Response::new(reply))
    }

    async fn grpc_get_node_info(
        &self,
        request: Request<Void>, // Accept request of type RString
    ) -> Result<Response<NodeInfo>, Status> { // Return an instance of type rustdkvs::NodeInfo
        println!("Got a request: {:?}", request);

        let mut reply = conv_node_info_to_grpc_one(chord_util::get_node_info(Arc::clone(&self.self_node), Arc::clone(&self.client_pool)));
        reply.finger_table = vec![];

        Ok(Response::new(reply))
    }    


}

// #[get("/get_node_info")]
// pub fn rrpc__get_node_info(self_node: State<ArMu<node_info::NodeInfo>>, client_pool: State<ArMu<HashMap<String, ArMu<reqwest::Client>>>>) -> Json<node_info::NodeInfo> {
//     return Json(chord_util::get_node_info(Arc::clone(&self_node), Arc::clone(&client_pool)));
// }

// // ブラウザから試すためのエンドポイント
// #[get("/global_put_simple?<key>&<val>")]
// pub fn rrpc__global_put_simple(self_node: State<ArMu<node_info::NodeInfo>>, data_store: State<ArMu<data_store::DataStore>>, client_pool: State<ArMu<HashMap<String, ArMu<reqwest::Client>>>>, key: String, val: String) -> Json<Result<bool, chord_util::GeneralError>> {
//     let rt = Runtime::new().unwrap();

//     let handle = rt.spawn({
//         chord_node::global_put(Arc::clone(&self_node), Arc::clone(&data_store), Arc::clone(&client_pool), key, val)
//     });
    
//     return Json(rt.block_on(handle).unwrap());
// }

// // ブラウザから試すためのエンドポイント
// #[get("/global_get_simple?<key>")]
// pub fn rrpc__global_get_simple(self_node: State<ArMu<node_info::NodeInfo>>, data_store: State<ArMu<data_store::DataStore>>, client_pool: State<ArMu<HashMap<String, ArMu<reqwest::Client>>>>, key: String) -> Json<Result<chord_util::DataIdAndValue, chord_util::GeneralError>> {
//     let rt = Runtime::new().unwrap();

//     let handle = rt.spawn({
//         chord_node::global_get(Arc::clone(&self_node), Arc::clone(&data_store), Arc::clone(&client_pool), key)
//     });
    
//     return Json(rt.block_on(handle).unwrap());
// }

// // ブラウザから試すためのエンドポイント
// #[get("/global_delete_simple?<key>")]
// pub fn rrpc__global_delete_simple(self_node: State<ArMu<node_info::NodeInfo>>, data_store: State<ArMu<data_store::DataStore>>, client_pool: State<ArMu<HashMap<String, ArMu<reqwest::Client>>>>, key: String) -> Json<Result<bool, chord_util::GeneralError>> {
//     let rt = Runtime::new().unwrap();

//     let handle = rt.spawn({
//         chord_node::global_delete(Arc::clone(&self_node), Arc::clone(&data_store), Arc::clone(&client_pool), key)
//     });
    
//     return Json(rt.block_on(handle).unwrap());
// }

// // ブラウザからアドレス解決を試すためのエンドポイント
// // 与えられた0から100の整数の100分の1をID空間のサイズ（最大値）にかけた
// // 値をIDとして、find_successorした結果を返す
// // 問い合わせはまず自身に対してかける
// #[get("/resolve_id_val?<percentage>")]
// pub fn rrpc__resolve_id_val(self_node: State<ArMu<node_info::NodeInfo>>, client_pool: State<ArMu<HashMap<String, ArMu<reqwest::Client>>>>, percentage : String) -> Json<node_info::NodeInfo> {
//     let percentage_num: f32 = percentage.parse().unwrap();
//     let id = ((percentage_num / 100.0) as f64) * (gval::ID_MAX as f64);

//     let rt = Runtime::new().unwrap();

//     let handle = rt.spawn({
//         router::find_successor(Arc::clone(&self_node), Arc::clone(&client_pool), id as u32)
//     });
    
//     return Json(rt.block_on(handle).unwrap().unwrap());
// }

pub async fn grpc_api_server_start(self_node: ArMu<node_info::NodeInfo>, data_store: ArMu<data_store::DataStore>, client_pool: ArMu<HashMap<String, ArMu<reqwest::Client>>>, bind_addr: String, bind_port_num: i32) {
    let addr_port: SocketAddr = (bind_addr + ":" + bind_port_num.to_string().as_str()).parse().unwrap();
    //let rdkvs_serv = MyRustDKVS::default();
    let rdkvs_serv = MyRustDKVS { self_node: self_node, data_store: data_store, client_pool: client_pool};

    Server::builder()
    .concurrency_limit_per_connection(1000)
    //.http2_keepalive_timeout(Some(Duration::from_secs(1000000)))
    //.tcp_keepalive(Some(Duration::from_secs(1000000)))
    .timeout(Duration::from_secs(10000))
    .add_service(RustDkvsServer::new(rdkvs_serv))    
    .serve(addr_port)
    .await;
}

pub fn conv_node_info_to_grpc_one(node_info: node_info::NodeInfo) -> crate::rustdkvs::NodeInfo {
    return crate::rustdkvs::NodeInfo {
        //message : format!("Hello {}!", request.into_inner().name).into(),
        node_id : node_info.node_id,
        address_str: node_info.address_str,
        born_id : node_info.born_id,
        successor_info_list: conv_node_info_vec_to_grpc_one(node_info.successor_info_list),
        predecessor_info: conv_node_info_vec_to_grpc_one(node_info.predecessor_info),
        finger_table: conv_node_info_opvec_to_grpc_one(node_info.finger_table)
     };
}

pub fn conv_node_info_vec_to_grpc_one(ni_vec: Vec<node_info::NodeInfo>) -> Vec<crate::rustdkvs::NodeInfo> {
    let mut ret_vec: Vec<crate::rustdkvs::NodeInfo> = vec![];
    for ninfo in ni_vec {
        ret_vec.push(conv_node_info_to_grpc_one(ninfo));
    }
    return ret_vec;
}

pub fn conv_node_info_opvec_to_grpc_one(ni_opvec: Vec<Option<node_info::NodeInfo>>) -> Vec<crate::rustdkvs::NodeInfo> {
    let mut ret_vec: Vec<crate::rustdkvs::NodeInfo> = vec![];
    //born_id = -1 な ノードは None として扱うように受け側では逆変換する規約とする
    for ninfo in ni_opvec {
        match ninfo {
            None => {
                // // newした時点での born_id の初期値は -1 である
                // let none_dummy = crate::rustdkvs::NodeInfo { 
                //     node_id: 0,
                //     address_str: "".to_string(),
                //     born_id: -1,
                //     predecessor_info: vec![],
                //     successor_info_list: vec![],
                //     finger_table: vec![]
                // };
                // ret_vec.push(none_dummy);
            }
            Some(ninfo_wrapped) => { 
                // let any: Any;
                // ret_vec.push(conv_node_info_to_grpc_one(ninfo_wrapped));
            }
        }
    }
    return ret_vec;
}

pub fn conv_node_info_to_normal_one(node_info: crate::rustdkvs::NodeInfo) -> node_info::NodeInfo {
    return node_info::NodeInfo {
        //message : format!("Hello {}!", request.into_inner().name).into(),
        node_id : node_info.node_id,
        address_str: node_info.address_str,
        born_id : node_info.born_id,
        successor_info_list: conv_node_info_vec_to_normal_one(node_info.successor_info_list),
        predecessor_info: conv_node_info_vec_to_normal_one(node_info.predecessor_info),
        finger_table: conv_node_info_opvec_to_normal_one(node_info.finger_table)
     };
}

pub fn conv_node_info_vec_to_normal_one(ni_vec: Vec<crate::rustdkvs::NodeInfo>) -> Vec<node_info::NodeInfo> {
    let mut ret_vec: Vec<node_info::NodeInfo> = vec![];
    for ninfo in ni_vec {
        ret_vec.push(conv_node_info_to_normal_one(ninfo));
    }
    return ret_vec;
}

pub fn conv_node_info_opvec_to_normal_one(ni_opvec: Vec<crate::rustdkvs::NodeInfo>) -> Vec<Option<node_info::NodeInfo>> {
    let mut ret_vec: Vec<Option<node_info::NodeInfo>> = vec![];
    // born_id = -1 な ノードは None として扱う
    for ninfo in ni_opvec {
        if ninfo.born_id == -1 {
            ret_vec.push(None);
        }else {
            let val = Some(conv_node_info_to_normal_one(ninfo));
            ret_vec.push(val);
        }
    }
    return ret_vec;
}

pub fn conv_iv_to_grpc_one(iv: chord_util::DataIdAndValue) -> crate::rustdkvs::DataIdAndValue {
    return crate::rustdkvs::DataIdAndValue { data_id: iv.data_id, val_str: iv.val_str};
}

pub fn conv_iv_vec_to_grpc_one(iv_vec: Vec<chord_util::DataIdAndValue>) -> Vec<crate::rustdkvs::DataIdAndValue> {
    let mut ret_vec: Vec<crate::rustdkvs::DataIdAndValue> = vec![];
    for iv in iv_vec {
        ret_vec.push(conv_iv_to_grpc_one(iv));
    }
    return ret_vec;
}

pub fn conv_iv_to_normal_one(iv: crate::rustdkvs::DataIdAndValue) -> chord_util::DataIdAndValue {
    return chord_util::DataIdAndValue { data_id: iv.data_id, val_str: iv.val_str};
}

pub fn conv_iv_vec_to_normal_one(iv_vec: Vec<crate::rustdkvs::DataIdAndValue>) -> Vec<chord_util::DataIdAndValue> {
    let mut ret_vec: Vec<chord_util::DataIdAndValue> = vec![];
    for iv in iv_vec {
        ret_vec.push(conv_iv_to_normal_one(iv));
    }
    return ret_vec;
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
//#![allow(dead_code)] 
// disables several lint warnings
#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(non_upper_case_globals)]
#![allow(unused_assignments)]
#![allow(dead_code)]
#![allow(non_snake_case)]
#![allow(unused_must_use)]

#![feature(proc_macro_hygiene)]
#![feature(decl_macro)]

#[macro_use]
extern crate rocket;

#[macro_use]
extern crate lazy_static;

//HTTPヘッダを生成する構造体を自動生成するためのマクロを使用可能とする
//認証などを行わないのであれば必要ないかも
#[macro_use]
extern crate hyper;

// utility macros

macro_rules! ArMu_new {
    ($wrapped:expr) => (
        Arc::new(Mutex::new($wrapped))
    );    
}

pub mod gval;
pub mod chord_node;
pub mod node_info;
pub mod chord_util;
pub mod stabilizer;
pub mod router;
pub mod data_store;
pub mod endpoints;

type ArMu<T> = Arc<Mutex<T>>;

use std::{borrow::{Borrow, BorrowMut}, io::Write, sync::Arc, thread};
use std::cell::{RefMut, RefCell, Ref};
use std::io::{stdout, stdin};
use std::sync::{Mutex, mpsc};
use std::sync::atomic::Ordering;
use std::env;
use std::collections::HashMap;
use std::fs::File;

use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::json;

use pprof::ProfilerGuard;
use pprof::protos::Message;

fn req_rest_api_test_inner_get() {
    let resp = reqwest::blocking::get("http://127.0.0.1:8000/").unwrap()
    .text();
    //.json::<HashMap<String, String>>().unwrap();
    println!("{:#?}", resp);
}

fn req_rest_api_test_inner_get_param_test() {
    let resp = reqwest::blocking::get("http://localhost:8000/get-param-test?param1=aaaaaa&param2=bbbbbb").unwrap()
    .text();
    //.json::<HashMap<String, String>>().unwrap();
    println!("{:#?}", resp);
}

fn req_rest_api_test_inner_post() {
    let text = r#"{"node_id":100,"address_str":"kanbayashi","born_id":77,"successor_info_list":[{"node_id":100,"address_str":"kanbayashi","born_id":77,"successor_info_list":[],"predecessor_info":[],"finger_table":[null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null]}],"predecessor_info":[{"node_id":100,"address_str":"kanbayashi","born_id":77,"successor_info_list":[{"node_id":100,"address_str":"kanbayashi","born_id":77,"successor_info_list":[],"predecessor_info":[],"finger_table":[null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null]}],"predecessor_info":[],"finger_table":[null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null]}],"finger_table":[null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null]}"#;
    let arg_node_info = serde_json::from_str::<node_info::NodeInfo>(text).unwrap();

    let client = reqwest::blocking::Client::new();
    let res = client.post("http://localhost:8000/deserialize")
    .body(serde_json::to_string(&arg_node_info).unwrap())
    .send().unwrap();

    println!("{:#?}", res.text());
}

fn req_rest_api_test() {    
    println!("client mode!\n");
    //req_rest_api_test_inner_post();
    //req_rest_api_test_inner_get_param_test();
    req_rest_api_test_inner_get();
}

fn main() {
    //引数処理
    let args: Vec<String> = env::args().collect();
    if args.len() == 2 {
        let num: i32 = args[1].parse().unwrap();
        if num == 2 { // REST client
            req_rest_api_test();
        }
    }else if args.len() > 2 {
        let guard = pprof::ProfilerGuard::new(100).unwrap();

        let born_id: i32 = args[1].parse().unwrap();
        let bind_addr: String = args[2].parse().unwrap();
        let bind_port_num: i32 = args[3].parse().unwrap();
        let tyukai_addr: String = args[4].parse().unwrap();
        let tyukai_port_num: i32 = args[5].parse().unwrap();
        let log_out_path: String = args[6].parse().unwrap();
        //TODO: (rustr) ログの出力先ディレクトリのパスも受けられるようにする
        //              ディレクトリがまだ存在しなければここの引数処理の中で作成してしまう

        //TODO: (rustr) ロガーライブラリは初期化時にディレクトリパスも含めて出力先を指定できるものを選びたい
        //              （つまり、ロガーライブラリの初期化もグローバルに一度やればOK、みたいなものであればここでやる）
        println!("born_id={:?}, bind_addr={:?}, bind_port_num={:?}, tyukai_addr={:?}, tyukai_port_num={:?}, log_out_path={:?}", &born_id, &bind_addr, &bind_port_num, &tyukai_addr, &tyukai_port_num, &log_out_path);

        let node_info = ArMu_new!(node_info::NodeInfo::new());
        let data_store = ArMu_new!(data_store::DataStore::new());

        let node_info_api_serv = Arc::clone(&node_info);
        let data_store_api_serv = Arc::clone(&data_store);
        let bind_addr_api_serv = bind_addr.clone();

        let node_info_arc_succ_th = Arc::clone(&node_info);
        let data_store_arc_succ_th = Arc::clone(&data_store);
    
        let node_info_arc_ftable_th = Arc::clone(&node_info);
        let data_store_arc_ftable_th = Arc::clone(&data_store);

        std::thread::spawn(move|| {
            endpoints::rest_api_server_start(Arc::clone(&node_info_api_serv), Arc::clone(&data_store_api_serv), bind_addr_api_serv, bind_port_num);
        });

        std::thread::sleep(std::time::Duration::from_millis(1500 as u64));

        // 仲介ノードを介してChordネットワークに参加する
        stabilizer::join(
            Arc::clone(&node_info),
            &(bind_addr.clone() + ":" + &bind_port_num.to_string()),
            &(tyukai_addr + ":" + &tyukai_port_num.to_string()),
            born_id
        );

        std::thread::sleep(std::time::Duration::from_millis(500 as u64));


        let mut counter = 0;
        let stabilize_succ_th_handle = std::thread::spawn(move|| loop{
            stabilizer::stabilize_successor(Arc::clone(&node_info_arc_succ_th));
            counter += 1;
            if counter % gval::FILL_SUCC_LIST_INTERVAL_TIMES == 0 {
                // successor_info_listの0番要素以降を規定数まで埋める（埋まらない場合もある）
                stabilizer::fill_succ_info_list(Arc::clone(&node_info_arc_succ_th));
            }
            std::thread::sleep(std::time::Duration::from_millis(100 as u64));
            if counter == 600 {
                match guard.report().build() {
                    Ok(report) => {
                        let mut file = File::create("profile-".to_string() + born_id.to_string().as_str() + ".pb").unwrap();
                        let profile = report.pprof().unwrap();
            
                        let mut content = Vec::new();
                        profile.encode(&mut content).unwrap();
                        file.write_all(&content).unwrap();
            
                        println!("report: {:?}", report);
                    }
                    Err(_) => {}
                };
            }
            //std::thread::sleep(std::time::Duration::from_millis(500 as u64));
        });
    
        let stabilize_ftable_th_handle = std::thread::spawn(move|| loop{
            for idx in 1..(gval::ID_SPACE_BITS + 1){
                    stabilizer::stabilize_finger_table(Arc::clone(&node_info_arc_ftable_th), idx as i32);
                    std::thread::sleep(std::time::Duration::from_millis(50 as u64));
            }
        });    

        // std::thread::spawn(|| loop{
        //     std::thread::sleep(std::time::Duration::from_millis((10000) as u64));
        //     println!("req_rest_api_test!");
        //     req_rest_api_test();
        // });


        let mut thread_handles = vec![];    
        thread_handles.push(stabilize_succ_th_handle);
        thread_handles.push(stabilize_ftable_th_handle);
        
    
        // スレッド終了の待ち合わせ（終了してくるスレッドは基本的に無い）
        for handle in thread_handles {
            handle.join().unwrap();
        }

    }

}

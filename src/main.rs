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
//pub mod taskqueue;
pub mod endpoints;

//type ArRmRs<T> = Arc<ReentrantMutex<RefCell<T>>>;
type ArMu<T> = Arc<Mutex<T>>;

use std::{borrow::{Borrow, BorrowMut}, io::Write, sync::Arc, thread};
use std::cell::{RefMut, RefCell, Ref};
use std::io::{stdout, stdin};
use std::sync::{Mutex, mpsc};
use std::sync::atomic::Ordering;
use std::env;
use std::collections::HashMap;

use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::json;

use parking_lot::{ReentrantMutex, ReentrantMutexGuard, const_reentrant_mutex};


る
    // successorを辿って最初のノードに戻ってきているはずだが、そうなっていない場合は successorの
    // チェーン構造が正しく構成されていないことを意味するためエラーとして終了する
    if all_node_num >=2 && cur_node_info_succ_0.node_id != start_node_info.node_id {
        chord_util::dprint(&("check_nodes_connectivity_succ_err,chain does not includes all node. all_node_num = ".to_string()
                         + all_node_num.to_string().as_str() + ","
                         + chord_util::gen_debug_str_of_node(Some(&start_node_info)).as_str() + ","
                         + chord_util::gen_debug_str_of_node(Some(&cur_node_info_succ_0)).as_str()));
        // raise exception("SUCCESSOR_CHAIN_IS_NOT_CONSTRUCTED_COLLECTLY")
    } else {
        chord_util::dprint(&("check_nodes_connectivity_succ_success,chain includes all node. all_node_num = ".to_string()
                         + all_node_num.to_string().as_str() + ","
                         + chord_util::gen_debug_str_of_node(Some(&start_node_info)).as_str() + ","
                         + chord_util::gen_debug_str_of_node(Some(&cur_node_info_succ_0)).as_str()));
    }
}
*/



/*
pub fn do_stabilize_once_at_all_node_ftable_without_new_th(node_list : Vec<ArRmRs<chord_node::ChordNode>>){
    for times in 0..gval::STABILIZE_FTABLE_BATCH_TIMES {
        for table_idx in 0..gval::ID_SPACE_BITS {
            for node in &node_list {
                let node_refcell = get_refcell_from_arc_with_locking!(node);
                let node_ref = get_ref_from_refcell!(node_refcell);
                
                match stabilizer::stabilize_finger_table(Arc::clone(node), node_ref, table_idx as i32) {
                    Err(_err) => { // err_code == ErrorCode.InternalControlFlowException_CODE
                        // join中のノードのノードオブジェクトを get_node_by_address しようとした場合に
                        // InternalCtronlFlowExceptionがraiseされてくるのでその場合は、対象ノードのstabilize_finger_tableはあきらめる
                        let node_ni_refcell = get_refcell_from_arc_with_locking!(node_ref.node_info);
                        let node_ni_ref = get_ref_from_refcell!(node_ni_refcell);
                        chord_util::dprint(
                            &("do_stabilize_ftable_th,".to_string() + chord_util::gen_debug_str_of_node(Some(node_ni_ref)).as_str()
                            + ",STABILIZE_FAILED_DUE_TO_INTERNAL_CONTROL_FLOW_EXCEPTION_RAISED"));                        
                    },
                    Ok(_dummy_bool) => {
                        //do nothing
                    }
                }
            }
        }
    }
}
*/

/*
def do_stabilize_ftable_th(node_list : List[ChordNode]):
    for times in range(0, gval.STABILIZE_FTABLE_BATCH_TIMES):
        for table_idx in range(0, gval.ID_SPACE_BITS):
            for node in node_list:
                ret = node.stabilizer.stabilize_finger_table(table_idx)
                if (ret.is_ok):
                    pass
                else:  # ret.err_code == ErrorCode.InternalControlFlowException_CODE
                    # join中のノードのノードオブジェクトを get_node_by_address しようとした場合に
                    # InternalCtronlFlowExceptionがraiseされてくるのでその場合は、対象ノードのstabilize_finger_tableはあきらめる
                    ChordUtil.dprint(
                        "do_stabilize_ftable_th," + ChordUtil.gen_debug_str_of_node(node.node_info)
                        + ",STABILIZE_FAILED_DUE_TO_INTERNAL_CONTROL_FLOW_EXCEPTION_RAISED")
*/

/*
def do_stabilize_onace_at_all_node_ftable(node_list : List[ChordNode]) -> List[Thread]:
    list_len = len(node_list)
    range_start = 0
    # 小数点以下切り捨て
    basic_pass_node_cnt = int(list_len / gval.STABILIZE_THREAD_NUM)
    thread_list : List[Thread] = []
    for thread_idx in range(0, gval.STABILIZE_THREAD_NUM):
        if thread_idx == gval.STABILIZE_THREAD_NUM - 1:
            thread = threading.Thread(target=do_stabilize_ftable_th, name="ftable-" + str(thread_idx),
                                      args=([node_list[range_start:-1]]))
        else:
            thread = threading.Thread(target=do_stabilize_successor_th, name="ftable-" + str(thread_idx),
                                      args=([node_list[range_start:range_start + basic_pass_node_cnt]]))
            range_start += basic_pass_node_cnt
        thread.start()
        thread_list.append(thread)

    return thread_list
*/

/*
pub fn do_stabilize_once_at_all_node_successor_without_new_th(node_list : Vec<ArRmRs<chord_node::ChordNode>>){
    for times in 0..gval::STABILIZE_SUCCESSOR_BATCH_TIMES {
        for node in &node_list {
            let node_refcell = get_refcell_from_arc_with_locking!(node);
            let node_ref = get_ref_from_refcell!(node_refcell);
            
            match stabilizer::stabilize_successor(Arc::clone(node)) {
                Err(_err) => { // err_code == ErrorCode.InternalControlFlowException_CODE
                    // join中のノードのノードオブジェクトを get_node_by_address しようとした場合に
                    // InternalCtronlFlowExceptionがraiseされてくるのでその場合は、対象ノードのstabilize_finger_tableはあきらめる
                    let node_ni_refcell = get_refcell_from_arc_with_locking!(node_ref.node_info);
                    let node_ni_ref = get_ref_from_refcell!(node_ni_refcell);
                    chord_util::dprint(
                        &("do_stabilize_once_at_all_node_successor_without_new_th,".to_string() + chord_util::gen_debug_str_of_node(Some(node_ni_ref)).as_str()
                        + ",STABILIZE_FAILED_DUE_TO_INTERNAL_CONTROL_FLOW_EXCEPTION_RAISED"));                        
                },
                Ok(_dummy_bool) => {
                    //do nothing
                }
            }
        }
    }
}
*/

/*
def do_stabilize_successor_th(node_list : List[ChordNode]):
    for times in range(0, gval.STABILIZE_SUCCESSOR_BATCH_TIMES):
        for node in node_list:
            # try:
                #node.stabilizer.stabilize_successor()
            ret = node.stabilizer.stabilize_successor()
            if (ret.is_ok):
                pass
            else:  # ret.err_code == ErrorCode.InternalControlFlowException_CODE
                # join中のノードのノードオブジェクトを get_node_by_address しようとした場合に
                # InternalCtronlFlowExceptionがraiseされてくるのでその場合は、対象ノードのstabilize_finger_tableはあきらめる
                ChordUtil.dprint(
                    "do_stabilize_successor_th," + ChordUtil.gen_debug_str_of_node(node.node_info)
                    + ",STABILIZE_FAILED_DUE_TO_INTERNAL_CONTROL_FLOW_EXCEPTION_RAISED")
*/

/*
// all_node_id辞書のvaluesリスト内から重複なく選択したノードに successor の stabilize のアクションをとらせていく
pub fn do_stabilize_once_succ_at_all_node(){
        // // ロックの取得
        // // ここで取得した値が無効とならない限り gval::global_datasへの別スレッドからのアクセスはブロックされる
        // let gd_refcell = get_refcell_from_arc_with_locking!(gval::global_datas);
        // let gd_ref = get_ref_from_refcell!(gd_refcell);

        chord_util::dprint(&("do_stabilize_once_succ_at_all_node_0,START".to_string()));

        let shuffled_node_list = get_all_network_constructed_nodes();
    
        // TODO: (rust) 暫定実装としてスレッドを新たに立ち上げず全てのノードについて処理をする
        //              後で複数スレッドで行う形に戻すこと. その際は各スレッドが並列に動作しなければ
        //              意味が無いためこの関数の先頭でロックをとってはいけない
        do_stabilize_once_at_all_node_successor_without_new_th(shuffled_node_list);
    
        // TODO: (rust) successorのstabilizeは後回し
        //thread_list_succ : List[Thread] = do_stabilize_onace_at_all_node_successor(shuffled_node_list)
    
        // TODO: (rust) 複数スレッドでの stabilizeも後回し
        //let thread_list_ftable : List[Thread] = do_stabilize_onace_at_all_node_ftable(shuffled_node_list)
        check_nodes_connectivity();        
}

// all_node_id辞書のvaluesリスト内から重複なく選択したノードに ftable の stabilize のアクションをとらせていく
pub fn do_stabilize_once_ftable_at_all_node(){
        // // ロックの取得
        // // ここで取得した値が無効とならない限り gval::global_datasへの別スレッドからのアクセスはブロックされる
        // let gd_refcell = get_refcell_from_arc_with_locking!(gval::global_datas);
        // let gd_ref = get_ref_from_refcell!(gd_refcell);

        chord_util::dprint(&("do_stabilize_once_ftable_at_all_node_0,START".to_string()));

        let shuffled_node_list = get_all_network_constructed_nodes();
    
        // TODO: (rust) 暫定実装としてスレッドを新たに立ち上げず全てのノードについて処理をする
        //              後で複数スレッドで行う形に戻すこと. その際は各スレッドが並列に動作しなければ
        //              意味が無いためこの関数の先頭でロックをとってはいけない
        do_stabilize_once_at_all_node_ftable_without_new_th(shuffled_node_list);
    
        // TODO: (rust) successorのstabilizeは後回し
        //thread_list_succ : List[Thread] = do_stabilize_onace_at_all_node_successor(shuffled_node_list)
    
        // TODO: (rust) 複数スレッドでの stabilizeも後回し
        //let thread_list_ftable : List[Thread] = do_stabilize_onace_at_all_node_ftable(shuffled_node_list)
}


// all_node_id辞書のvaluesリスト内から重複なく選択したノードに stabilize のアクションをとらせていく
pub fn do_stabilize_once_at_all_node(){
    // // ロックの取得
    // // ここで取得した値が無効とならない限り gval::global_datasへの別スレッドからのアクセスはブロックされる
    // let gd_refcell = get_refcell_from_arc_with_locking!(gval::global_datas);
    // let gd_ref = get_ref_from_refcell!(gd_refcell);

    chord_util::dprint(&("do_stabilize_once_at_all_node_0,START".to_string()));
    //with gval.lock_of_all_node_dict:


fn req_rest_api_test_inner_get() {
    let resp = reqwest::blocking::get("http://127.0.0.1:8000/").unwrap()
    .text();
    //.json::<HashMap<String, String>>().unwrap();
    println!("{:#?}", resp);
}

fn req_rest_api_test_inner_get_param_test() {
    //let resp = reqwest::blocking::get("http://localhost:8000/get-param-test/aaaaaa/bbbbbb").unwrap()
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

        let node_info_arc_succ_th = Arc::clone(&node_info);
        let data_store_arc_succ_th = Arc::clone(&data_store);
    
        let node_info_arc_ftable_th = Arc::clone(&node_info);
        let data_store_arc_ftable_th = Arc::clone(&data_store);

        // 仲介ノードを介してChordネットワークに参加する
        stabilizer::join(
            Arc::clone(&node_info),
            &(bind_addr.clone() + ":" + &bind_port_num.to_string()),
            &(tyukai_addr + ":" + &tyukai_port_num.to_string()),
            born_id
        );

        std::thread::sleep(std::time::Duration::from_millis(500 as u64));

        let stabilize_succ_th_handle = std::thread::spawn(move|| loop{
            stabilizer::stabilize_successor(Arc::clone(&node_info_arc_succ_th));
            std::thread::sleep(std::time::Duration::from_millis(100 as u64));
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

        endpoints::rest_api_server_start(Arc::clone(&node_info), Arc::clone(&data_store), bind_addr, bind_port_num);

        let mut thread_handles = vec![];    
        thread_handles.push(stabilize_succ_th_handle);
        thread_handles.push(stabilize_ftable_th_handle);
        
    
        // スレッド終了の待ち合わせ（終了してくるスレッドは基本的に無い）
        for handle in thread_handles {
            handle.join().unwrap();
        }   
    }

}

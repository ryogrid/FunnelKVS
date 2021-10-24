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



// TODO: (rustr) 制御用ツールから、successorを得るAPIなどを用いて（要実装）同等のチェックが行える
//               ように別実装を行う必要あり（制御用ツールが主体となる）
/*
// stabilize_successorの呼び出しが一通り終わったら確認するのに利用する
// ランダムに選択したノードからsuccessor方向にsuccessorの繋がりでノードを辿って
// 行って各ノードの情報を出力する
// また、predecessorの方向にpredecesorの繋がりでもたどって出力する
pub fn check_nodes_connectivity() {
    chord_util::dprint(&("check_nodes_connectivity_1".to_string()));
    let mut counter = 0;
    // まずはsuccessor方向に辿る
    let cur_node_arrmrs = get_a_random_node();
    let cur_node_refcell = get_refcell_from_arc_with_locking!(cur_node_arrmrs);
    let cur_node_ref = get_ref_from_refcell!(cur_node_refcell);
    let cur_node_ni_refcell = get_refcell_from_arc_with_locking!(cur_node_ref.node_info);
    let cur_node_info = get_ref_from_refcell!(cur_node_ni_refcell);
    // let mut cloned_node: ArRmRs<chord_node::ChordNode>;
    // let mut cur_node_info_succ_0_arrmrs: ArRmRs<node_info::NodeInfo>;
    // let mut cur_node_info_succ_0_refcell: &RefCell<node_info::NodeInfo>;
    let cur_node_info_succ_0 = cur_node_info;
    let mut cur_node_info_succ_0_addr = cur_node_info.address_str.clone();
    let start_node_info = (*cur_node_info_succ_0).clone();
    // ノードの総数（is_aliveフィールドがFalseのものは除外して算出）

    //with gval.lock_of_all_node_dict:

    let all_active_nodes = get_all_network_constructed_nodes();
    let all_node_num = all_active_nodes.len() as i32;

    let abnn_str: String;
    unsafe {
        abnn_str = gval::already_born_node_num.load(Ordering::Relaxed).to_string();
    }

    println!("{}","check_nodes_connectivity__succ,all_node_num=".to_string() + "," + all_node_num.to_string().as_str() + ",already_born_node_num=" + abnn_str.as_str());
    print!("{}", cur_node_info_succ_0.born_id.to_string() + "," + chord_util::conv_id_to_ratio_str(cur_node_info_succ_0.node_id).as_str() + " -> ");
    while counter < all_node_num {
        

        // 各ノードはsuccessorの情報を保持しているが、successorのsuccessorは保持しないようになって
        // いるため、単純にsuccessorのチェーンを辿ることはできないため、各ノードから最新の情報を
        // 得ることに対応する形とする

        //match chord_util::get_node_by_address(&cur_node_info_succ_0.successor_info_list[0].address_str) {
        match endpoints::rrpc__get_node_info_by_address(&cur_node_info_succ_0_addr) {            
            Err(err) => { // ErrorCode.InternalControlFlowException_CODE || ErrorCode.NodeIsDownedException_CODE
                if err.err_code == chord_util::ERR_CODE_NODE_IS_DOWNED {
                    println!("");
                    chord_util::dprint(&("check_nodes_connectivity__succ,NODE_IS_DOWNED".to_string()));
                    return;
                } else { //chord_util::ERR_CODE_INTERNAL_CONTROL_FLOW_PROBLEM
                    println!("");
                    chord_util::dprint(&("check_nodes_connectivity__succ,TARGET_NODE_DOES_NOT_EXIST_EXCEPTION_IS_RAISED".to_string()));
                    return;
                }
            },
            Ok(node) => {
                let cloned_node = Arc::clone(&node);
                let cur_node_refcell = get_refcell_from_arc_with_locking!(cloned_node);
                let cur_node_ref = get_ref_from_refcell!(cur_node_refcell);
                let cur_node_ni_refcell = get_refcell_from_arc_with_locking!(cur_node_ref.node_info);
                let cur_node_info = get_ref_from_refcell!(cur_node_ni_refcell);                
                if cur_node_info.successor_info_list.len() < 1 {
                    println!("");
                    print!("no successor having node was detected!");
                }else{
                    print!("{}", cur_node_info.born_id.to_string() + "," + chord_util::conv_id_to_ratio_str(cur_node_info.node_id).as_str() + " -> ");
                    let cur_node_info_succ_0_arrmrs = ArMu_new!(cur_node_info.successor_info_list[0].clone());
                    let cur_node_info_succ_0_refcell = get_refcell_from_arc_with_locking!(cur_node_info_succ_0_arrmrs);
                    let cur_node_info_succ_0_ref  = get_ref_from_refcell!(cur_node_info_succ_0_refcell);
                    cur_node_info_succ_0_addr = cur_node_info_succ_0_ref.address_str.clone();
                }
            }
            //cur_node_info : 'NodeInfo' = cast('ChordNode', ret.result).node_info.successor_info_list[0];
        

        }
        counter += 1;
    }
    println!("");

    // 2ノード目が参加して以降をチェック対象とする
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

    // let shuffled_node_list: Vec<ArRmRs<chord_node::ChordNode>> = vec![];
    // for node_elem in gd_ref.all_node_dict.values() {
    //     // Rustのイテレータは毎回異なる順序で要素を返すため自身でシャッフルする必要はない
    //     let node_refcell = get_refcell_from_arc_with_locking!(*node_elem);
    //     let node_ref = get_ref_from_refcell!(node_refcell);
    //     if node_ref.is_join_op_finished.load(Ordering::Relaxed) == true && node_ref.is_alive.load(Ordering::Relaxed) == true {
    //         shuffled_node_list.push(Arc::clone(node_elem));
    //     }
    // }
    let mut shuffled_node_list = get_all_network_constructed_nodes();

    //let shuffled_node_list: Vec<chord_node::ChordNode> = random.sample(node_list, len(node_list));

    // TODO: (rust) 暫定実装としてスレッドを新たに立ち上げず全てのノードについて処理をする
    //              後で複数スレッドで行う形に戻すこと. その際は各スレッドが並列に動作しなければ
    //              意味が無いためこの関数の先頭でロックをとってはいけない
    do_stabilize_once_at_all_node_ftable_without_new_th(shuffled_node_list);

    shuffled_node_list = get_all_network_constructed_nodes();

    // TODO: (rust) 暫定実装としてスレッドを新たに立ち上げず全てのノードについて処理をする
    //              後で複数スレッドで行う形に戻すこと. その際は各スレッドが並列に動作しなければ
    //              意味が無いためこの関数の先頭でロックをとってはいけない
    do_stabilize_once_at_all_node_successor_without_new_th(shuffled_node_list);

    // TODO: (rust) successorのstabilizeは後回し
    //thread_list_succ : List[Thread] = do_stabilize_onace_at_all_node_successor(shuffled_node_list)

    // TODO: (rust) 複数スレッドでの stabilizeも後回し
    //let thread_list_ftable : List[Thread] = do_stabilize_onace_at_all_node_ftable(shuffled_node_list)

    // 全てのスレッドが終了するまで待つ
    // 一つの呼び出しごとにブロックするが、その間に別にスレッドが終了しても
    // スレッドの処理が終了していることは担保できるため問題ない

    // for thread in thread_list_succ:
    //     thread.join()
    // for thread in thread_list_ftable:
    //     thread.join();

    check_nodes_connectivity();
}
*/

/*
# all_node_id辞書のvaluesリスト内から重複なく選択したノードに stabilize のアクションをとらせていく
def do_stabilize_once_at_all_node():
    ChordUtil.dprint("do_stabilize_once_at_all_node_0,START")
    with gval.lock_of_all_node_dict:
        node_list = list(gval.all_node_dict.values())
        shuffled_node_list : List[ChordNode] = random.sample(node_list, len(node_list))
    thread_list_succ : List[Thread] = do_stabilize_onace_at_all_node_successor(shuffled_node_list)
    thread_list_ftable : List[Thread] = do_stabilize_onace_at_all_node_ftable(shuffled_node_list)

    # 全てのスレッドが終了するまで待つ
    # 一つの呼び出しごとにブロックするが、その間に別にスレッドが終了しても
    # スレッドの処理が終了していることは担保できるため問題ない
    for thread in thread_list_succ:
        thread.join()
    for thread in thread_list_ftable:
        thread.join()

    check_nodes_connectivity()
*/



/*
pub fn stabilize_succ_th(){
    loop{
        // 内部で適宜ロックを解放することで他のスレッドの処理も行えるようにしつつ
        // 呼び出し時点でのノードリストを対象に stabilize 処理を行う
        let abnn_tmp: i32;
        unsafe{
            abnn_tmp = gval::already_born_node_num.load(Ordering::Relaxed) as i32;
        }

        //5ノード以上joinしたらstabilizeを開始する
        if abnn_tmp >= 50 {
            if abnn_tmp == 50 {
                // まだjoin処理中かもしれないので5秒待つ
                std::thread::sleep(std::time::Duration::from_millis(5000 as u64));                
            }
            do_stabilize_once_succ_at_all_node();
        }else{
            std::thread::sleep(std::time::Duration::from_millis(100 as u64));
        }
        
    }
}

pub fn stabilize_ftable_th(){
    loop{
        // 内部で適宜ロックを解放することで他のスレッドの処理も行えるようにしつつ
        // 呼び出し時点でのノードリストを対象に stabilize 処理を行う
        let abnn_tmp: i32;
        unsafe{
            abnn_tmp = gval::already_born_node_num.load(Ordering::Relaxed) as i32;
        }

        //5ノード以上joinしたらstabilizeを開始する
        if abnn_tmp >= 50 {
            if abnn_tmp == 50 {
                // まだjoin処理中かもしれないので5秒待つ
                std::thread::sleep(std::time::Duration::from_millis(5000 as u64));                
            }
            do_stabilize_once_ftable_at_all_node();
        }else{
            std::thread::sleep(std::time::Duration::from_millis(100 as u64));
        }       
    }
}
*/

/*        
def stabilize_th():
    while True:
        # 内部で適宜ロックを解放することで他のスレッドの処理も行えるようにしつつ
        # 呼び出し時点でのノードリストを対象に stabilize 処理を行う
        do_stabilize_once_at_all_node()
*/


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
            std::thread::sleep(std::time::Duration::from_millis(20 as u64));
        });
    
        let stabilize_ftable_th_handle = std::thread::spawn(move|| loop{
            for idx in 1..(gval::ID_SPACE_BITS + 1){
                    stabilizer::stabilize_finger_table(Arc::clone(&node_info_arc_ftable_th), idx as i32);
                    std::thread::sleep(std::time::Duration::from_millis(10 as u64));
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

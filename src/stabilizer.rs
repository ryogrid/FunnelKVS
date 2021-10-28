use std::sync::{Arc, Mutex};
use std::cell::{RefMut, RefCell, Ref};
use std::sync::atomic::Ordering;
use std::borrow::{Borrow, BorrowMut};

use crate::gval;
use crate::chord_node;
use crate::node_info;
use crate::chord_util;
use crate::endpoints;
use crate::data_store;
use crate::router;

type ArMu<T> = Arc<Mutex<T>>;

/*
// join が router.find_successorでの例外発生で失敗した場合にこのクラス変数に格納して次のjoin処理の際にリトライさせる
// なお、本シミュレータの設計上、このフィールドは一つのデータだけ保持できれば良い
lazy_static! {
    pub static ref need_join_retry_node : ArMu<Option<chord_node::ChordNode>> 
        = ArMu_new!(None);
}
*/
/*
need_join_retry_node : Optional['ChordNode'] = None
*/

/*
lazy_static! {
    pub static ref need_join_retry_tyukain_node : ArMu<Option<chord_node::ChordNode>>
        = Arc::new(const_reentrant_mutex(RefCell::new(None)));
}
*/
/*
need_join_retry_tyukai_node: Optional['ChordNode'] = None
*/

// 経路表の情報を他ノードから強制的に設定する.
// joinメソッドの中で、secondノードがfirstノードに対してのみ用いるものであり、他のケースで利用してはならない
pub fn set_routing_infos_force(self_node: ArMu<node_info::NodeInfo>, predecessor_info: node_info::NodeInfo, successor_info_0: node_info::NodeInfo , ftable_enry_0: node_info::NodeInfo){
    //with self.existing_node.node_info.lock_of_pred_info, self.existing_node.node_info.lock_of_succ_infos:
    let self_node_clone;
    {
        let mut self_node_ref = self_node.lock().unwrap();
        
        self_node_ref.successor_info_list[0] = successor_info_0;
        self_node_ref.finger_table[0] = Some(ftable_enry_0);
        self_node_clone = (*self_node_ref).clone();
    }
    node_info::set_pred_info(Arc::clone(&self_node), predecessor_info);
}

// node_addressに対応するノードに問い合わせを行い、教えてもらったノードをsuccessorとして設定する
pub fn join(new_node: ArMu<node_info::NodeInfo>, self_node_address: &String, tyukai_node_address: &String, born_id: i32){
    let mut new_node_ref = new_node.lock().unwrap();
    
    // ミリ秒精度のUNIXTIMEからChordネットワーク上でのIDを決定する
    new_node_ref.born_id = born_id;
    new_node_ref.address_str = (*self_node_address).clone();
    //new_node_ref.node_id = chord_util::hash_str_to_int(&new_node_ref.address_str);
    new_node_ref.node_id = chord_util::hash_str_to_int(&(chord_util::get_unixtime_in_nanos().to_string()));

    let mut deep_cloned_new_node = node_info::partial_clone_from_ref_strong(&new_node_ref);
    let mut is_second_node:bool = false;

    //println!("address_str at join: {:?}", new_node_ref.address_str);

    if born_id == 1 { 
        // first_node の場合

        // successorとpredecessorは自身として終了する
        new_node_ref.successor_info_list.push(deep_cloned_new_node.clone());
        new_node_ref.finger_table[0] = Some(deep_cloned_new_node.clone());
        drop(deep_cloned_new_node);
        deep_cloned_new_node = node_info::partial_clone_from_ref_strong(&new_node_ref);
        drop(new_node_ref);
        node_info::set_pred_info(Arc::clone(&new_node), deep_cloned_new_node.clone());

        println!("first_node at join: {:?}", new_node.lock().unwrap());
        return;
    }

    drop(new_node_ref);    

    //println!("join {:?}", tyukai_node_address);
    // 実装上例外は発生しない.
    // また実システムでもダウンしているノードの情報が与えられることは想定しない
    // TODO: (rustr)RPC呼出しに置き換える必要あり
    let tyukai_node = endpoints::rrpc_call__get_node_info(tyukai_node_address).unwrap();

    // 仲介ノードに自身のsuccessorになるべきノードを探してもらう
    chord_util::dprint(&("join_1,".to_string() + chord_util::gen_debug_str_of_node(&deep_cloned_new_node).as_str() + ","
        + chord_util::gen_debug_str_of_node(&tyukai_node).as_str()));    
    let successor = endpoints::rrpc_call__find_successor(&tyukai_node, deep_cloned_new_node.node_id).unwrap();

    // TODO: (rustr) for debug
    if deep_cloned_new_node.node_id == successor.node_id {
        chord_util::dprint(&("join_2_5,".to_string() + chord_util::gen_debug_str_of_node(&deep_cloned_new_node).as_str() + ","
                        + chord_util::gen_debug_str_of_node(&tyukai_node).as_str() + ","
                        + chord_util::gen_debug_str_of_node(&deep_cloned_new_node.successor_info_list[0]).as_str() + ",FOUND_NODE_IS_SAME_WITH_SELF_NODE!!!"));
    }

    new_node_ref = new_node.lock().unwrap();
    if tyukai_node.node_id == tyukai_node.successor_info_list[0].node_id {
        // secondノードの場合の考慮 (仲介ノードは必ずfirst node)

        //predecessor = tyukai_node;

        // 2ノードでsuccessorでもpredecessorでも、チェーン構造で正しい環が構成されるよう強制的に全て設定してしまう
        // secondノードの場合の考慮 (仲介ノードは必ずfirst node)
        is_second_node = true;
        
        new_node_ref.successor_info_list.push(tyukai_node.clone());

        drop(deep_cloned_new_node);
        deep_cloned_new_node = node_info::partial_clone_from_ref_strong(&new_node_ref);
        drop(new_node_ref);
        node_info::set_pred_info(Arc::clone(&new_node), tyukai_node.clone());
        endpoints::rrpc_call__set_routing_infos_force(
            &tyukai_node,
            deep_cloned_new_node.clone(),
            deep_cloned_new_node.clone(),
            deep_cloned_new_node.clone()
        );

        chord_util::dprint(&("join_3,".to_string() + chord_util::gen_debug_str_of_node(&deep_cloned_new_node).as_str() + ","
        + chord_util::gen_debug_str_of_node(&tyukai_node).as_str() + ","
        + chord_util::gen_debug_str_of_node(&deep_cloned_new_node.successor_info_list[0]).as_str()));
        
        return;
    }else{
        //new_node_ref.successor_info_list.push(successor.clone());
        drop(new_node_ref);
    }

    new_node_ref = new_node.lock().unwrap();

    // successorを設定する
    new_node_ref.successor_info_list.push(successor.clone());

    // finger_tableのインデックス0は必ずsuccessorになるはずなので、設定しておく
    new_node_ref.finger_table[0] = Some(new_node_ref.successor_info_list[0].clone());

    // successorと、successorノードの情報だけ適切なものとする
    // TODO: check_predecessor call at join

    drop(new_node_ref);
    match endpoints::rrpc_call__check_predecessor(&successor, &deep_cloned_new_node){
        Err(err) => {
            // IDを変えてリトライ
            // (これで異なるsuccessorが得られて、そのノードは生きていることを期待する)
            join(new_node, self_node_address, tyukai_node_address, born_id);
        }
        Ok(some) => {}
    };
}

// TODO: 注 -> (rustr) このメソッドの呼び出し時はself_nodeの中身への別の参照は存在しない状態としておくこと
pub fn stabilize_successor(self_node: ArMu<node_info::NodeInfo>) -> Result<bool, chord_util::GeneralError>{
    let mut self_node_ref = self_node.lock().unwrap();
    let mut deep_cloned_self_node = node_info::partial_clone_from_ref_strong(&self_node_ref);
    //println!("P-SELF-S: P: {:?} SELF: {:?} S {:?}", self_node_ref.predecessor_info, *self_node_ref, self_node_ref.successor_info_list);
    drop(self_node_ref);

    chord_util::dprint(&("stabilize_successor_1,".to_string() + chord_util::gen_debug_str_of_node(&deep_cloned_self_node).as_str() + ","
          + chord_util::gen_debug_str_of_node(&deep_cloned_self_node.successor_info_list[0]).as_str()));

    // firstノードだけが存在する状況で、self_nodeがfirst_nodeであった場合に対する考慮
    if deep_cloned_self_node.predecessor_info.len() == 0 && deep_cloned_self_node.node_id == deep_cloned_self_node.successor_info_list[0].node_id {
        chord_util::dprint(&("stabilize_successor_1_5,".to_string() + chord_util::gen_debug_str_of_node(&deep_cloned_self_node).as_str() + ","
                         + chord_util::gen_debug_str_of_node(&deep_cloned_self_node.successor_info_list[0]).as_str()));

        // secondノードがjoinしてきた際にチェーン構造は2ノードで適切に構成されるように
        // なっているため、ここでは何もせずに終了する

        return Ok(true);
    }

    // 自身のsuccessorに、当該ノードが認識しているpredecessorを訪ねる
    // 自身が保持している successor_infoのミュータブルなフィールドは最新の情報でない
    // 場合があるため、successorのChordNodeオブジェクトを引いて、そこから最新のnode_info
    // の参照を得る
    
    let ret = endpoints::rrpc_call__get_node_info(&deep_cloned_self_node.successor_info_list[0].address_str);
    //{

    let successor_info = match ret{
        Err(err) => {
            self_node_ref = self_node.lock().unwrap();
            node_info::recovery_succ(&mut self_node_ref, &deep_cloned_self_node.successor_info_list[0], &err);
            return Err(chord_util::GeneralError::new(err.message, err.err_code));
        }
        Ok(got_node) => {                
            got_node
        }
    };

// TODO: (rustr)実システム化する際にコメントアウトした。check_predecessor呼び出し時に呼び出し元は
//              自身のNodeInfoのロックを解放するようにするので、問題ないはず
/*        
        // 2ノードで環が構成されている場合に、お互いがstabilize_successorを呼び出した場合にデッドロック
        // してしまうケースを避けるための考慮
        if self_node_ni_refmut.node_id == self_node_ni_refmut.successor_info_list[0].node_id {
            // predecessor と successorが同一であり、firstノードの場合は上の方で既に抜けているので
            // 2ノードの場合

            chord_util::dprint(&("stabilize_successor_1_7,".to_string() + chord_util::gen_debug_str_of_node(Some(self_node_ni_refmut)).as_str() + ","
            + chord_util::gen_debug_str_of_node(Some(&self_node_ni_refmut.successor_info_list[0])).as_str()));

            return Ok(true);
        }
*/

    if successor_info.predecessor_info.len() == 0 {
        //is_successor_has_no_pred = true;
        chord_util::dprint(&("stabilize_successor_2,".to_string() + chord_util::gen_debug_str_of_node(&deep_cloned_self_node).as_str() + ","
        + chord_util::gen_debug_str_of_node(&deep_cloned_self_node.successor_info_list[0]).as_str()));

        match endpoints::rrpc_call__check_predecessor(&successor_info, &deep_cloned_self_node){
            Err(err) => {
                self_node_ref = self_node.lock().unwrap();
                node_info::recovery_succ(&mut self_node_ref, &successor_info, &err);
                return Ok(true);
            }
            Ok(some) => {}
        };

        return Ok(true);
    }
        
/*
        if deep_cloned_self_node.node_id == successor_info.node_id {
            //何故か、自身がsuccessorリストに入ってしまっているのでとりあえず抜ける
            chord_util::dprint(&("WARN!!!".to_string()));
            return Ok(true);
        }
*/
    //}

    chord_util::dprint(&("stabilize_successor_3,".to_string() + chord_util::gen_debug_str_of_node(&deep_cloned_self_node).as_str() + ","
                    + chord_util::gen_debug_str_of_node(&successor_info.successor_info_list[0]).as_str()));

    let pred_id_of_successor = successor_info.predecessor_info[0].node_id;

    chord_util::dprint(&("stabilize_successor_3_5,".to_string() + &format!("{:X}", pred_id_of_successor)));

    // 下のパターン1から3という記述は以下の資料による説明に基づく
    // https://www.slideshare.net/did2/chorddht
    if pred_id_of_successor == deep_cloned_self_node.node_id {
        // パターン1
        // 特に訂正は不要なので処理を終了する
        chord_util::dprint(&("stabilize_successor_4,".to_string() + chord_util::gen_debug_str_of_node(&deep_cloned_self_node).as_str() + ","
        + chord_util::gen_debug_str_of_node(&successor_info.successor_info_list[0]).as_str()));
        return Ok(true);
    }else{
        // 以下、パターン2およびパターン3に対応する処理

        // 自身がsuccessorにとっての正しいpredecessorでないか確認を要請し必要であれば
        // 情報を更新してもらう
        // 事前チェックによって避けられるかもしれないが、常に実行する
        //let successor_obj = endpoints::rrpc__get_node_info(&successor_info.address_str).unwrap();

/*        
        if deep_cloned_self_node.address_str == successor_info.address_str {
            //何故か、自身がsuccessorリストに入ってしまっているのでとりあえず抜ける
            //抜けないと多重borrowでpanicしてしまうので
            chord_util::dprint(&("WARN!!!".to_string()));
            return Ok(true);
        }
*/
        chord_util::dprint(&("stabilize_successor_5,".to_string() + chord_util::gen_debug_str_of_node(&deep_cloned_self_node).as_str() + ","
        + chord_util::gen_debug_str_of_node(&successor_info.successor_info_list[0]).as_str()));

        match endpoints::rrpc_call__check_predecessor(&successor_info, &deep_cloned_self_node){
            Err(err) => {
                self_node_ref = self_node.lock().unwrap();
                node_info::recovery_succ(&mut self_node_ref, &successor_info, &err);
                return Ok(true);
            }
            Ok(some) => {}
        };
        //check_predecessor(Arc::clone(&successor_obj), (*self_node_ni_refmut).clone());

        let distance_unknown = chord_util::calc_distance_between_nodes_left_mawari(successor_info.node_id, pred_id_of_successor);
        let distance_me = chord_util::calc_distance_between_nodes_left_mawari(successor_info.node_id, deep_cloned_self_node.node_id);
        chord_util::dprint(&("stabilize_successor distance_unknown=".to_string() 
            + distance_unknown.to_string().as_str() 
            + " distance_me=" + distance_me.to_string().as_str())
        );
        if distance_unknown < distance_me {
            // successorの認識しているpredecessorが自身ではなく、かつ、そのpredecessorが
            // successorから自身に対して前方向にたどった場合の経路中に存在する場合
            // 自身の認識するsuccessorの情報を更新する

            self_node_ref = self_node.lock().unwrap();
            self_node_ref.successor_info_list[0] = successor_info.predecessor_info[0].clone();
            deep_cloned_self_node = node_info::partial_clone_from_ref_strong(&self_node_ref);
            drop(self_node_ref);

            // 新たなsuccessorに対して自身がpredecessorでないか確認を要請し必要であれ
            // ば情報を更新してもらう
            let new_successor_info = match endpoints::rrpc_call__get_node_info(&deep_cloned_self_node.successor_info_list[0].address_str){
                Err(err) => {
                    self_node_ref = self_node.lock().unwrap();
                    node_info::recovery_succ(&mut self_node_ref, &deep_cloned_self_node.successor_info_list[0], &err);
                    return Err(chord_util::GeneralError::new(err.message, err.err_code));
                }
                Ok(got_node) => {                
                    got_node
                }
            };
/*
            if deep_cloned_self_node.node_id == deep_cloned_self_node.successor_info_list[0].node_id {
                //何故か、自身がsuccessorリストに入ってしまっているのでとりあえず抜ける
                //抜けないと多重borrowでpanicしてしまうので
                chord_util::dprint(&("WARN!!!".to_string()));
                return Ok(true);
            }
*/
            match endpoints::rrpc_call__check_predecessor(&new_successor_info, &deep_cloned_self_node){
                Err(err) => {
                    self_node_ref = self_node.lock().unwrap();
                    node_info::recovery_succ(&mut self_node_ref, &successor_info, &err);
                    return Ok(true);
                }
                Ok(some) => {}
            };

            chord_util::dprint(&("stabilize_successor_6,".to_string() + chord_util::gen_debug_str_of_node(&deep_cloned_self_node).as_str() + ","
                             + chord_util::gen_debug_str_of_node(&deep_cloned_self_node.successor_info_list[0]).as_str() + ","
                             + chord_util::gen_debug_str_of_node(&deep_cloned_self_node).as_str()));

            return Ok(true);
        }
    }

    return Ok(true);
}

// FingerTableに関するstabilize処理を行う
// 一回の呼び出しで1エントリを更新する
// FingerTableのエントリはこの呼び出しによって埋まっていく
// TODO: InternalExp at stabilize_finger_table
// TODO: 注 -> (rustr) このメソッドの呼び出し時はself_nodeの中身への別の参照は存在しない状態としておくこと
pub fn stabilize_finger_table(self_node: ArMu<node_info::NodeInfo>, idx: i32) -> Result<bool, chord_util::GeneralError> {
    let mut self_node_ref = self_node.lock().unwrap();
    let deep_cloned_self_node = node_info::partial_clone_from_ref_strong(&self_node_ref);
    //chord_util::dprint_routing_info(self.existing_node, sys._getframe().f_code.co_name);

    chord_util::dprint(&("stabilize_finger_table_1,".to_string() + chord_util::gen_debug_str_of_node(&self_node_ref).as_str()));

    // FingerTableの各要素はインデックスを idx とすると 2^IDX 先のIDを担当する、もしくは
    // 担当するノードに最も近いノードが格納される
    let update_id = chord_util::overflow_check_and_conv((self_node_ref.node_id as u64) + (2u64.pow(idx as u32) as u64));

    println!("update_id: {:?} {:?}", update_id, idx);

    drop(self_node_ref);
    let find_rslt = endpoints::rrpc_call__find_successor(&deep_cloned_self_node, update_id);
    
    self_node_ref = self_node.lock().unwrap();
    match find_rslt {
        Err(err) => {
            // ret.err_code == ErrorCode.AppropriateNodeNotFoundException_Code || ret.err_code == ErrorCode.InternalControlFlowException_CODE
            //  || ret.err_code == ErrorCode.NodeIsDownedException_CODE

            // 適切な担当ノードを得ることができなかった
            // 今回のエントリの更新はあきらめるが、例外の発生原因はおおむね見つけたノードがダウンしていた
            // ことであるので、更新対象のエントリには None を設定しておく
            self_node_ref.finger_table[(idx - 1) as usize] = None;
            chord_util::dprint(&("stabilize_finger_table_2_5,NODE_IS_DOWNED,".to_string()
                + chord_util::gen_debug_str_of_node(&self_node_ref).as_str()));

            node_info::recovery_succ(&mut self_node_ref, &deep_cloned_self_node, &err);

            return Ok(true);
        },
        Ok(found_node) => {
            self_node_ref.finger_table[(idx - 1) as usize] = Some(found_node.clone());

            chord_util::dprint(&("stabilize_finger_table_3,".to_string() 
                    + chord_util::gen_debug_str_of_node(&self_node_ref).as_str() + ","
                    + chord_util::gen_debug_str_of_node(&found_node).as_str()));

            return Ok(true);
        }
    }
}

// caller_node が自身の正しい predecessor でないかチェックし、そうであった場合、経路表の情報を更新する
// 本メソッドはstabilize処理の中で用いられる
// TODO: 注 -> (rustr) このメソッドの呼び出し時はself_nodeの中身への別の参照は存在しない状態としておくこと
pub fn check_predecessor(self_node: ArMu<node_info::NodeInfo>, caller_node_ni: node_info::NodeInfo) -> Result<bool, chord_util::GeneralError> {
    let mut self_node_ref = self_node.lock().unwrap();
    let deep_cloned_self_node = node_info::partial_clone_from_ref_strong(&self_node_ref);
    drop(self_node_ref);

    chord_util::dprint(&("check_predecessor_1,".to_string() + chord_util::gen_debug_str_of_node(&deep_cloned_self_node).as_str() + ","
        + chord_util::gen_debug_str_of_node(&caller_node_ni).as_str() + ","
        + chord_util::gen_debug_str_of_node(&deep_cloned_self_node.successor_info_list[0]).as_str()));

    if deep_cloned_self_node.predecessor_info.len() == 0 {
        // predecesorが設定されていなければ無条件にチェックを求められたノードを設定する
        node_info::set_pred_info(Arc::clone(&self_node), caller_node_ni.clone());
        chord_util::dprint(&("check_predecessor_1,".to_string() + chord_util::gen_debug_str_of_node(&deep_cloned_self_node).as_str() + ","
            + chord_util::gen_debug_str_of_node(&caller_node_ni).as_str() + ","
            + chord_util::gen_debug_str_of_node(&deep_cloned_self_node.successor_info_list[0]).as_str()));
        return Ok(true);
    }

    chord_util::dprint(&("check_predecessor_2,".to_string() + chord_util::gen_debug_str_of_node(&deep_cloned_self_node).as_str() + ","
            + chord_util::gen_debug_str_of_node(&deep_cloned_self_node.successor_info_list[0]).as_str()));

    // TODO: (rustr) まだノードダウンの考慮は不要
    // // この時点で認識している predecessor がノードダウンしていないかチェックする
    // let is_pred_alived = match chord_util::is_node_alive(&self_node_ni_refmut.predecessor_info[0].address_str) {
    //     Err(_e) => false, // err_code == ErrorCode.InternalControlFlowException_CODE
    //     Ok(is_alive) => is_alive
    // };
    // if is_pred_alived {
    
    self_node_ref = self_node.lock().unwrap();
    let distance_check = chord_util::calc_distance_between_nodes_left_mawari(self_node_ref.node_id, caller_node_ni.node_id);
    let distance_cur = chord_util::calc_distance_between_nodes_left_mawari(self_node_ref.node_id,
                                                                        self_node_ref.predecessor_info[0].node_id);
    chord_util::dprint(&("check_predecessor distance_check=".to_string() 
        + distance_check.to_string().as_str() 
        + " distance_cur=" + distance_cur.to_string().as_str())
    );
    // 確認を求められたノードの方が現在の predecessor より predecessorらしければ
    // 経路表の情報を更新する
    if distance_check < distance_cur {
        drop(self_node_ref);
        node_info::set_pred_info(Arc::clone(&self_node), caller_node_ni.clone());
        self_node_ref = self_node.lock().unwrap();
        chord_util::dprint(&("check_predecessor_3,".to_string() + chord_util::gen_debug_str_of_node(&self_node_ref).as_str() + ","
                + chord_util::gen_debug_str_of_node(&self_node_ref.successor_info_list[0]).as_str() + ","
                + chord_util::gen_debug_str_of_node(&self_node_ref.predecessor_info[0]).as_str()));
    }
    // } else { // predecessorがダウンしていた場合は無条件でチェックを求められたノードをpredecessorに設定する
    //     self_node_ni_refmut.set_pred_info(caller_node_ni.clone());
    // }

    return Ok(true);
}


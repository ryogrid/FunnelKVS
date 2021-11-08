use std::sync::{Arc, Mutex};
use std::cell::{RefMut, RefCell, Ref};
use std::sync::atomic::Ordering;
use std::borrow::{Borrow, BorrowMut};
use std::collections::HashMap;

use crate::gval;
use crate::chord_node;
use crate::node_info;
use crate::chord_util;
use crate::endpoints;
use crate::data_store;
use crate::router;

type ArMu<T> = Arc<Mutex<T>>;

// 経路表の情報を他ノードから強制的に設定する.
// joinメソッドの中で、secondノードがfirstノードに対してのみ用いるものであり、他のケースで利用してはならない
pub fn set_routing_infos_force(self_node: ArMu<node_info::NodeInfo>, client_pool: ArMu<HashMap<String, ArMu<reqwest::Client>>>, predecessor_info: node_info::NodeInfo, successor_info_0: node_info::NodeInfo , ftable_enry_0: node_info::NodeInfo){
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
pub async fn join(new_node: ArMu<node_info::NodeInfo>, client_pool: ArMu<HashMap<String, ArMu<reqwest::Client>>>, self_node_address: &String, tyukai_node_address: &String, born_id: i32) {
    let mut new_node_deep_cloned;
    let mut is_second_node:bool = false;
    {
        {
            let mut new_node_ref = new_node.lock().unwrap();
            
            // ミリ秒精度のUNIXTIMEからChordネットワーク上でのIDを決定する
            new_node_ref.born_id = born_id;
            new_node_ref.address_str = (*self_node_address).clone();
            new_node_ref.node_id = chord_util::hash_str_to_int(&(chord_util::get_unixtime_in_nanos().to_string()));

            new_node_deep_cloned = node_info::partial_clone_from_ref_strong(&new_node_ref);
        }

        if born_id == 1 { 
            // first_node の場合
            {
                println!("I am first node!");
                // successorとpredecessorは自身として終了する
                let mut new_node_ref = new_node.lock().unwrap();
                new_node_ref.successor_info_list.push(new_node_deep_cloned.clone());
                new_node_ref.finger_table[0] = Some(new_node_deep_cloned.clone());
                //drop(deep_cloned_new_node);
                new_node_deep_cloned = node_info::partial_clone_from_ref_strong(&new_node_ref);
                //drop(new_node_ref);
            }
            node_info::set_pred_info(Arc::clone(&new_node), new_node_deep_cloned.clone());

            println!("first_node at join: {:?}", new_node.lock().unwrap());
            return;
        }

        //drop(new_node_ref);    
    }

    let mut tyukai_node_dummy = node_info::NodeInfo::new();
    tyukai_node_dummy.address_str = tyukai_node_address.clone();

    // ダウンしているノードの情報が与えられることは想定しない
    let tyukai_node = endpoints::rrpc_call__get_node_info(&tyukai_node_dummy, Arc::clone(&client_pool), new_node_deep_cloned.node_id).await.unwrap();

    // 仲介ノードに自身のsuccessorになるべきノードを探してもらう
    chord_util::dprint(&("join_1,".to_string() + chord_util::gen_debug_str_of_node(&new_node_deep_cloned).as_str() + ","
        + chord_util::gen_debug_str_of_node(&tyukai_node).as_str()));    
    let successor = endpoints::rrpc_call__find_successor(&tyukai_node, Arc::clone(&client_pool), new_node_deep_cloned.node_id, new_node_deep_cloned.node_id).await.unwrap();

    if new_node_deep_cloned.node_id == successor.node_id {
        chord_util::dprint(&("join_2_5,".to_string() + chord_util::gen_debug_str_of_node(&new_node_deep_cloned).as_str() + ","
                        + chord_util::gen_debug_str_of_node(&tyukai_node).as_str() + ","
                        + chord_util::gen_debug_str_of_node(&new_node_deep_cloned.successor_info_list[0]).as_str() + ",FOUND_NODE_IS_SAME_WITH_SELF_NODE!!!"));
    }
    
    if tyukai_node.node_id == tyukai_node.successor_info_list[0].node_id {
        chord_util::dprint(&("join_2_7,".to_string() + chord_util::gen_debug_str_of_node(&new_node_deep_cloned).as_str() + ","
                        + chord_util::gen_debug_str_of_node(&tyukai_node).as_str()));        
        {
            {
                let mut new_node_ref = new_node.lock().unwrap();
                // secondノードの場合の考慮 (仲介ノードは必ずfirst node)

                // 2ノードでsuccessorでもpredecessorでも、チェーン構造で正しい環が構成されるよう強制的に全て設定してしまう
                // secondノードの場合の考慮 (仲介ノードは必ずfirst node)
                is_second_node = true;
                
                new_node_ref.successor_info_list.push(tyukai_node.clone());

                //drop(deep_cloned_new_node);
                new_node_deep_cloned = node_info::partial_clone_from_ref_strong(&new_node_ref);
                //drop(new_node_ref);
            }
            node_info::set_pred_info(Arc::clone(&new_node), tyukai_node.clone());
        }
        chord_util::dprint(&("join_2_8,".to_string() + chord_util::gen_debug_str_of_node(&new_node_deep_cloned).as_str() + ","
                        + chord_util::gen_debug_str_of_node(&tyukai_node).as_str()));

        endpoints::rrpc_call__set_routing_infos_force(
            &tyukai_node,
            new_node_deep_cloned.clone(),
            new_node_deep_cloned.clone(),
            new_node_deep_cloned.clone(),
            Arc::clone(&client_pool)
        ).await;

        chord_util::dprint(&("join_3,".to_string() + chord_util::gen_debug_str_of_node(&new_node_deep_cloned).as_str() + ","
        + chord_util::gen_debug_str_of_node(&tyukai_node).as_str() + ","
        + chord_util::gen_debug_str_of_node(&new_node_deep_cloned.successor_info_list[0]).as_str()));
        
        return;
    }

    {
        let mut new_node_ref = new_node.lock().unwrap();

        // successorを設定する
        new_node_ref.successor_info_list.push(successor.clone());

        // finger_tableのインデックス0は必ずsuccessorになるはずなので、設定しておく
        new_node_ref.finger_table[0] = Some(new_node_ref.successor_info_list[0].clone());

        // successorと、successorノードの情報だけ適切なものとする

        //drop(new_node_ref);
    }

    if successor.node_id == new_node_deep_cloned.node_id {
        // 自身への呼出しを避けるためにreturnする
        return;
    }

    match endpoints::rrpc_call__check_predecessor(&successor, &new_node_deep_cloned, Arc::clone(&client_pool), new_node_deep_cloned.node_id).await {
        Err(err) => {
            // IDを変えてリトライ
            // (これで異なるsuccessorが得られて、そのノードは生きていることを期待する)
            // join(new_node, Arc::clone(&client_pool), self_node_address, tyukai_node_address, born_id).await;
        }
        Ok(some) => {}
    };
}

pub async fn stabilize_successor(self_node: ArMu<node_info::NodeInfo>, client_pool: ArMu<HashMap<String, ArMu<reqwest::Client>>>) -> Result<bool, chord_util::GeneralError>{
    let mut self_node_deep_cloned;
    {
        let self_node_ref = self_node.lock().unwrap();
        self_node_deep_cloned = node_info::partial_clone_from_ref_strong(&self_node_ref);
        //println!("P-SELF-S: P: {:?} SELF: {:?} S {:?}", self_node_ref.predecessor_info, *self_node_ref, self_node_ref.successor_info_list);
        //drop(self_node_ref);
    }

    chord_util::dprint(&("stabilize_successor_1,".to_string() + chord_util::gen_debug_str_of_node(&self_node_deep_cloned).as_str() + ","
          + chord_util::gen_debug_str_of_node(&self_node_deep_cloned.successor_info_list[0]).as_str()));

    // firstノードだけが存在する状況で、self_nodeがfirst_nodeであった場合に対する考慮
    if self_node_deep_cloned.predecessor_info.len() == 0 && self_node_deep_cloned.node_id == self_node_deep_cloned.successor_info_list[0].node_id {
        chord_util::dprint(&("stabilize_successor_1_5,".to_string() + chord_util::gen_debug_str_of_node(&self_node_deep_cloned).as_str() + ","
                         + chord_util::gen_debug_str_of_node(&self_node_deep_cloned.successor_info_list[0]).as_str()));

        // secondノードがjoinしてきた際にチェーン構造は2ノードで適切に構成されるように
        // なっているため、ここでは何もせずに終了する

        return Ok(true);
    }

    // 自身のsuccessorに、当該ノードが認識しているpredecessorを訪ねる
    // 自身が保持している successor_infoのミュータブルなフィールドは最新の情報でない
    // 場合があるため、successorのChordNodeオブジェクトを引いて、そこから最新のnode_info
    // の参照を得る
    
    let ret = endpoints::rrpc_call__get_node_info(&self_node_deep_cloned.successor_info_list[0], Arc::clone(&client_pool), self_node_deep_cloned.node_id);

    let successor_info = match ret.await {
        Err(err) => {
            let mut self_node_ref = self_node.lock().unwrap();
            node_info::handle_downed_node_info(&mut self_node_ref, &self_node_deep_cloned.successor_info_list[0], &err);
            return Err(chord_util::GeneralError::new(err.message, err.err_code));
        }
        Ok(got_node) => {                
            got_node
        }
    };

    if successor_info.predecessor_info.len() == 0 {
        //is_successor_has_no_pred = true;
        chord_util::dprint(&("stabilize_successor_2,".to_string() + chord_util::gen_debug_str_of_node(&self_node_deep_cloned).as_str() + ","
        + chord_util::gen_debug_str_of_node(&self_node_deep_cloned.successor_info_list[0]).as_str()));

        if successor_info.node_id == self_node_deep_cloned.node_id {
            return Ok(true);
        }

        match endpoints::rrpc_call__check_predecessor(&successor_info, &self_node_deep_cloned, Arc::clone(&client_pool), self_node_deep_cloned.node_id).await {
            Err(err) => {
                let mut self_node_ref = self_node.lock().unwrap();
                node_info::handle_downed_node_info(&mut self_node_ref, &successor_info, &err);
                return Ok(true);
            }
            Ok(some) => {}
        };

        return Ok(true);
    }

    chord_util::dprint(&("stabilize_successor_3,".to_string() + chord_util::gen_debug_str_of_node(&self_node_deep_cloned).as_str() + ","
                    + chord_util::gen_debug_str_of_node(&successor_info.successor_info_list[0]).as_str()));

    let pred_id_of_successor = successor_info.predecessor_info[0].node_id;

    chord_util::dprint(&("stabilize_successor_3_5,".to_string() + &format!("{:X}", pred_id_of_successor)));

    // 下のパターン1から3という記述は以下の資料による説明に基づく
    // https://www.slideshare.net/did2/chorddht
    if pred_id_of_successor == self_node_deep_cloned.node_id {
        // パターン1
        // 特に訂正は不要なので処理を終了する
        chord_util::dprint(&("stabilize_successor_4,".to_string() + chord_util::gen_debug_str_of_node(&self_node_deep_cloned).as_str() + ","
        + chord_util::gen_debug_str_of_node(&successor_info.successor_info_list[0]).as_str()));
        return Ok(true);
    }else{
        // 以下、パターン2およびパターン3に対応する処理

        // 自身がsuccessorにとっての正しいpredecessorでないか確認を要請し必要であれば
        // 情報を更新してもらう
        // 事前チェックによって避けられるかもしれないが、常に実行する

        chord_util::dprint(&("stabilize_successor_5,".to_string() + chord_util::gen_debug_str_of_node(&self_node_deep_cloned).as_str() + ","
        + chord_util::gen_debug_str_of_node(&successor_info.successor_info_list[0]).as_str()));

        if successor_info.node_id == self_node_deep_cloned.node_id {
            return Ok(true);
        }

        match endpoints::rrpc_call__check_predecessor(&successor_info, &self_node_deep_cloned, Arc::clone(&client_pool), self_node_deep_cloned.node_id).await {
            Err(err) => {
                let mut self_node_ref = self_node.lock().unwrap();
                node_info::handle_downed_node_info(&mut self_node_ref, &successor_info, &err);
                return Ok(true);
            }
            Ok(some) => {}
        };

        let distance_unknown = chord_util::calc_distance_between_nodes_left_mawari(successor_info.node_id, pred_id_of_successor);
        let distance_me = chord_util::calc_distance_between_nodes_left_mawari(successor_info.node_id, self_node_deep_cloned.node_id);
        chord_util::dprint(&("stabilize_successor distance_unknown=".to_string() 
            + distance_unknown.to_string().as_str() 
            + " distance_me=" + distance_me.to_string().as_str())
        );
        if distance_unknown < distance_me {
            // successorの認識しているpredecessorが自身ではなく、かつ、そのpredecessorが
            // successorから自身に対して前方向にたどった場合の経路中に存在する場合
            // 自身の認識するsuccessorの情報を更新する
            {
                let mut self_node_ref = self_node.lock().unwrap();
                chord_util::dprint(&("stabilize_successor_SET_SUCCESSOR,".to_string() + chord_util::gen_debug_str_of_node(&self_node_deep_cloned).as_str() + ", from "
                + chord_util::gen_debug_str_of_node(&self_node_ref.successor_info_list[0]).as_str() + " to "
                + chord_util::gen_debug_str_of_node(&successor_info.predecessor_info[0]).as_str()));
                self_node_ref.successor_info_list[0] = successor_info.predecessor_info[0].clone();
                self_node_deep_cloned = node_info::partial_clone_from_ref_strong(&self_node_ref);
                //drop(self_node_ref);
            }

            // 新たなsuccessorに対して自身がpredecessorでないか確認を要請し必要であれ
            // ば情報を更新してもらう
            let new_successor_info = match endpoints::rrpc_call__get_node_info(&self_node_deep_cloned.successor_info_list[0], Arc::clone(&client_pool), self_node_deep_cloned.node_id).await {
                Err(err) => {
                    let mut self_node_ref = self_node.lock().unwrap();
                    node_info::handle_downed_node_info(&mut self_node_ref, &self_node_deep_cloned.successor_info_list[0], &err);
                    return Err(chord_util::GeneralError::new(err.message, err.err_code));
                }
                Ok(got_node) => {                
                    got_node
                }
            };

            if new_successor_info.node_id == self_node_deep_cloned.node_id {
                return Ok(true);
            }

            match endpoints::rrpc_call__check_predecessor(&new_successor_info, &self_node_deep_cloned, Arc::clone(&client_pool), self_node_deep_cloned.node_id).await {
                Err(err) => {
                    let mut self_node_ref = self_node.lock().unwrap();
                    node_info::handle_downed_node_info(&mut self_node_ref, &new_successor_info, &err);
                    return Ok(true);
                }
                Ok(some) => {}
            };

            chord_util::dprint(&("stabilize_successor_6,".to_string() + chord_util::gen_debug_str_of_node(&self_node_deep_cloned).as_str() + ","
                             + chord_util::gen_debug_str_of_node(&self_node_deep_cloned.successor_info_list[0]).as_str() + ","
                             + chord_util::gen_debug_str_of_node(&self_node_deep_cloned).as_str()));

            return Ok(true);
        }
    }

    return Ok(true);
}

// successor_info_listのインデックス1より後ろを規定数まで埋める
// 途中でエラーとなった場合は、規定数に届いていなくとも処理を中断する
pub async fn fill_succ_info_list(self_node: ArMu<node_info::NodeInfo>, client_pool: ArMu<HashMap<String, ArMu<reqwest::Client>>>) -> Result<bool, chord_util::GeneralError>{
    let self_node_id;
    let mut next_succ_id;
    let first_succ;
    {
        let self_node_ref = self_node.lock().unwrap();
        chord_util::dprint(&("fill_succ_info_list_0,".to_string() + chord_util::gen_debug_str_of_node(&self_node_ref).as_str()));

        self_node_id = self_node_ref.node_id;
        next_succ_id = self_node_ref.node_id;
        
        first_succ = self_node_ref.successor_info_list[0].clone();
        //let mut next_succ_info = node_info::partial_clone_from_ref_strong(&self_node_ref);
        //drop(self_node_ref);
    }

    if first_succ.node_id == self_node_id {
        return Ok(true);
    }

    let mut next_succ_info = match endpoints::rrpc_call__get_node_info(&first_succ, Arc::clone(&client_pool), self_node_id).await {
        Err(err) => {
            let mut self_node_ref = self_node.lock().unwrap();
            node_info::handle_downed_node_info(&mut self_node_ref, &first_succ, &err);
            // 後続を辿れないので、リスト埋めは中止してreturnする
            return Err(err);
        }
        Ok(ninfo) => ninfo
    };
    
    let mut idx_counter = 1;
    for times in 1..(gval::SUCCESSOR_INFO_LIST_LEN){
        next_succ_id = next_succ_info.node_id;
        next_succ_info = node_info::partial_clone_from_ref_strong(&next_succ_info.successor_info_list[0]);
        if next_succ_info.node_id == self_node_id || next_succ_info.node_id == next_succ_id {
            // next_succ_infoがself_nodeと同一もしくは、1つ前の位置のノードを指していた場合
            // 後続を辿っていく処理がループを構成してしまうため抜ける
            let self_node_ref = self_node.lock().unwrap();
            chord_util::dprint(
                &("fill_succ_info_list_1,".to_string() 
                + chord_util::gen_debug_str_of_node(&self_node_ref).as_str() + ","
                + chord_util::gen_debug_str_of_node(&next_succ_info).as_str() + ","
                + self_node_ref.successor_info_list.len().to_string().as_str()
            ));
            return Ok(true);
        }
        {
            let mut self_node_ref = self_node.lock().unwrap();
            if self_node_ref.successor_info_list.len() < (idx_counter + 1) {            
                self_node_ref.successor_info_list.push(next_succ_info.clone());
                chord_util::dprint(
                    &("fill_succ_info_list_2,".to_string() 
                    + chord_util::gen_debug_str_of_node(&self_node_ref).as_str() + ","
                    + chord_util::gen_debug_str_of_node(&next_succ_info).as_str() + ","
                    + self_node_ref.successor_info_list.len().to_string().as_str()
                ));
            }else{
                self_node_ref.successor_info_list[idx_counter] = next_succ_info.clone();
                chord_util::dprint(
                    &("fill_succ_info_list_3,".to_string() 
                    + chord_util::gen_debug_str_of_node(&self_node_ref).as_str() + ","
                    + chord_util::gen_debug_str_of_node(&next_succ_info).as_str() + ","
                    + self_node_ref.successor_info_list.len().to_string().as_str() + ","
                    + idx_counter.to_string().as_str()
                ));           
            }
            idx_counter += 1;
            //drop(self_node_ref);
        }
        next_succ_info = match endpoints::rrpc_call__get_node_info(&next_succ_info, Arc::clone(&client_pool), self_node_id).await {
            Err(err) => {
                let mut self_node_ref = self_node.lock().unwrap();
                node_info::handle_downed_node_info(&mut self_node_ref, &next_succ_info, &err);
                // 後続を辿れないので、リスト埋めは中止してreturnする
                return Err(err);
            }
            Ok(ninfo) => ninfo
        };
    }

    return Ok(true);
}

// FingerTableに関するstabilize処理を行う
// 一回の呼び出しで1エントリを更新する
// FingerTableのエントリはこの呼び出しによって埋まっていく
pub async fn stabilize_finger_table(self_node: ArMu<node_info::NodeInfo>, client_pool: ArMu<HashMap<String, ArMu<reqwest::Client>>>, idx: i32) -> Result<bool, chord_util::GeneralError> {
    let self_node_deep_cloned;
    let update_id;
    {
        let self_node_ref = self_node.lock().unwrap();
        self_node_deep_cloned = node_info::partial_clone_from_ref_strong(&self_node_ref);
        //chord_util::dprint_routing_info(self.existing_node, sys._getframe().f_code.co_name);

        chord_util::dprint(&("stabilize_finger_table_1,".to_string() + chord_util::gen_debug_str_of_node(&self_node_ref).as_str()));

        // FingerTableの各要素はインデックスを idx とすると 2^IDX 先のIDを担当する、もしくは
        // 担当するノードに最も近いノードが格納される
        update_id = chord_util::overflow_check_and_conv((self_node_ref.node_id as u64) + (2u64.pow(idx as u32) as u64));

        println!("update_id: {:?} {:?}", update_id, idx);

        //drop(self_node_ref);
    }
    
    match endpoints::rrpc_call__find_successor(&self_node_deep_cloned, Arc::clone(&client_pool), update_id, self_node_deep_cloned.node_id).await {
        Err(err) => {
            let mut self_node_ref = self_node.lock().unwrap();
            // 適切な担当ノードを得ることができなかった
            // 今回のエントリの更新はあきらめるが、例外の発生原因はおおむね見つけたノードがダウンしていた
            // ことであるので、更新対象のエントリには None を設定しておく
            self_node_ref.finger_table[(idx - 1) as usize] = None;
            chord_util::dprint(&("stabilize_finger_table_2_5,NODE_IS_DOWNED,".to_string()
                + chord_util::gen_debug_str_of_node(&self_node_ref).as_str()));

            node_info::handle_downed_node_info(&mut self_node_ref, &self_node_deep_cloned, &err);

            return Ok(true);
        },
        Ok(found_node) => {
            let mut self_node_ref = self_node.lock().unwrap();
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
pub async fn check_predecessor(self_node: ArMu<node_info::NodeInfo>, data_store: ArMu<data_store::DataStore>, client_pool: ArMu<HashMap<String, ArMu<reqwest::Client>>>, caller_node_ni: node_info::NodeInfo) -> Result<bool, chord_util::GeneralError> {
    let self_node_deep_cloned;
    {
        let self_node_ref = self_node.lock().unwrap();
        self_node_deep_cloned = node_info::partial_clone_from_ref_strong(&self_node_ref);
        //drop(self_node_ref);
    }

    chord_util::dprint(&("check_predecessor_1,".to_string() + chord_util::gen_debug_str_of_node(&self_node_deep_cloned).as_str() + ","
        + chord_util::gen_debug_str_of_node(&caller_node_ni).as_str() + ","
        + chord_util::gen_debug_str_of_node(&self_node_deep_cloned.successor_info_list[0]).as_str()));

    if self_node_deep_cloned.predecessor_info.len() == 0 {
        // predecesorが設定されていなければ無条件にチェックを求められたノードを設定する
        node_info::set_pred_info(Arc::clone(&self_node), caller_node_ni.clone());
        chord_util::dprint(&("check_predecessor_1,".to_string() + chord_util::gen_debug_str_of_node(&self_node_deep_cloned).as_str() + ","
            + chord_util::gen_debug_str_of_node(&caller_node_ni).as_str() + ","
            + chord_util::gen_debug_str_of_node(&self_node_deep_cloned.successor_info_list[0]).as_str()));
        return Ok(true);
    }

    // predecessorの生死チェックを行い、ダウンしていた場合 未設定状態に戻して return する
    // (本来 check_predecessor でやる処理ではないと思われるが、finger tableの情報を用いて
    // ノードダウン時の対処を行う場合に、このコードがないとうまくいかなそうなのでここで処理)
    match endpoints::rrpc_call__get_node_info(&self_node_deep_cloned.predecessor_info[0], Arc::clone(&client_pool), self_node_deep_cloned.node_id).await {
        Err(err) => {
            let mut self_node_ref = self_node.lock().unwrap();
            node_info::handle_downed_node_info(&mut self_node_ref, &self_node_deep_cloned.predecessor_info[0], &err);
            return Ok(true);
        }
        Ok(some) => {}        
    }

    chord_util::dprint(&("check_predecessor_2,".to_string() + chord_util::gen_debug_str_of_node(&self_node_deep_cloned).as_str() + ","
            + chord_util::gen_debug_str_of_node(&self_node_deep_cloned.successor_info_list[0]).as_str()));
    
    let distance_check;
    let distance_cur;
    {
        let self_node_ref = self_node.lock().unwrap();
        distance_check = chord_util::calc_distance_between_nodes_left_mawari(self_node_ref.node_id, caller_node_ni.node_id);
        distance_cur = chord_util::calc_distance_between_nodes_left_mawari(self_node_ref.node_id,
                                                                            self_node_ref.predecessor_info[0].node_id);
        chord_util::dprint(&("check_predecessor distance_check=".to_string() 
            + distance_check.to_string().as_str() 
            + " distance_cur=" + distance_cur.to_string().as_str())
        );
    }
    // 確認を求められたノードの方が現在の predecessor より predecessorらしければ
    // 経路表の情報を更新する
    if distance_check < distance_cur {
        //drop(self_node_ref);
        node_info::set_pred_info(Arc::clone(&self_node), caller_node_ni.clone());

        // 切り替えたpredecessorに対してデータの委譲を行う
        let delegate_datas: Vec<chord_util::DataIdAndValue>;
        {        
            let self_id = self_node_deep_cloned.node_id;
            let new_pred_id = caller_node_ni.node_id;
            let mut data_store_ref = data_store.lock().unwrap();
            
            delegate_datas = data_store_ref.get_and_delete_iv_with_pred_self_id(new_pred_id, self_id);
            //drop(data_store_ref);
        }

        {
            let self_node_ref = self_node.lock().unwrap();
            chord_util::dprint(&("check_predecessor_3,".to_string() + chord_util::gen_debug_str_of_node(&self_node_ref).as_str() + ","
                    + chord_util::gen_debug_str_of_node(&self_node_ref.successor_info_list[0]).as_str() + ","
                    + chord_util::gen_debug_str_of_node(&self_node_ref.predecessor_info[0]).as_str()));
            //drop(self_node_ref);
        }

        if caller_node_ni.node_id == self_node_deep_cloned.node_id {
            return Ok(true);
        }

        match endpoints::rrpc_call__pass_datas(&caller_node_ni, Arc::clone(&client_pool), delegate_datas, self_node_deep_cloned.node_id).await {
            Err(err) => {
                let mut self_node_ref = self_node.lock().unwrap();
                node_info::handle_downed_node_info(&mut self_node_ref, &caller_node_ni, &err);
                return Ok(true);
            }
            Ok(some) => { return Ok(true) }
        }
    }
    return Ok(true);
}

// passed_datasで渡されたデータのリストを自身のDataStoreに加える
// 基本的に、ノード参加が判明した際に他のノードが self_node に対してデータを委譲
// する際に利用することを想定する
pub fn pass_datas(self_node: ArMu<node_info::NodeInfo>, data_store: ArMu<data_store::DataStore>, client_pool: ArMu<HashMap<String, ArMu<reqwest::Client>>>, pass_datas: Vec<chord_util::DataIdAndValue>) -> Result<bool, chord_util::GeneralError> {
    let mut data_store_ref = data_store.lock().unwrap();
    data_store_ref.store_iv_with_vec(pass_datas);

    return Ok(true);
}


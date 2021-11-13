use std::sync::atomic::{AtomicIsize, AtomicBool};
use std::sync::{Arc, Mutex};
use std::cell::{RefMut, RefCell, Ref};
use std::borrow::Borrow;
use std::sync::atomic::Ordering;
use std::collections::HashMap;

use crate::gval;
use crate::node_info;
use crate::chord_util;
use crate::stabilizer;
use crate::router;
use crate::data_store;
use crate::endpoints;

type ArMu<T> = Arc<Mutex<T>>;

pub async fn global_put(self_node: ArMu<node_info::NodeInfo>, data_store: ArMu<data_store::DataStore>, client_pool: ArMu<HashMap<String, ArMu<reqwest::Client>>>, key_str: String, val_str: String) -> Result<bool, chord_util::GeneralError> {
    let self_node_deep_cloned;
    {
        let self_node_ref = self_node.lock().unwrap();    
        self_node_deep_cloned = node_info::partial_clone_from_ref_strong(&self_node_ref);
        //drop(self_node_ref);
    }

    // 更新に失敗するレプリカがあった場合、それはノードダウンであると（本当にそうか確実ではないが）前提をおいて、
    // 続くレプリカの更新は継続する
    let data_id = chord_util::hash_str_to_int(&key_str);
    let mut is_exist = true;
    for idx in 0..(gval::REPLICA_NUM + 1) {
        let target_id = chord_util::overflow_check_and_conv(data_id as u64 + (gval::REPLICA_ID_DISTANCE as u64) * (idx as u64));
        let replica_node = match endpoints::rrpc_call__find_successor(&self_node_deep_cloned, Arc::clone(&client_pool), target_id, self_node_deep_cloned.node_id).await {
            Err(err) => {
                {
                    let mut self_node_ref = self_node.lock().unwrap();
                    node_info::handle_downed_node_info(&mut self_node_ref, &self_node_deep_cloned, &err);
                    //drop(self_node_ref);
                }
                //return Err(err);
                continue;
            }
            Ok(ninfo) => ninfo
        };

        // chord_util::dprint(&("global_put_1,".to_string() 
        //     + chord_util::gen_debug_str_of_node(&self_node_deep_cloned).as_str() + ","
        //     + chord_util::gen_debug_str_of_node(&replica_node).as_str() + ","
        //     + chord_util::gen_debug_str_of_data(data_id).as_str() + ","
        //     + chord_util::gen_debug_str_of_data(target_id).as_str() + ","
        //     + idx.to_string().as_str()
        // ));        

        is_exist = match endpoints::rrpc_call__put(&node_info::gen_node_info_from_summary(&replica_node), Arc::clone(&data_store), Arc::clone(&client_pool), target_id, val_str.clone(), self_node_deep_cloned.node_id).await {        
        //let is_exist = match endpoints::rrpc_call__put(&node_info::gen_node_info_from_summary(&replica_node), target_id, val_str.clone()){
            Err(err) => {
                let mut self_node_ref = self_node.lock().unwrap();
                node_info::handle_downed_node_info(&mut self_node_ref, &node_info::gen_node_info_from_summary(&replica_node), &err);
                //drop(self_node_ref);
                continue;
                //return Err(err);
            }
            Ok(is_exist) => is_exist
        };

        // chord_util::dprint(&("global_put_2,".to_string() 
        //     + chord_util::gen_debug_str_of_node(&self_node_deep_cloned).as_str() + ","
        //     + chord_util::gen_debug_str_of_node(&replica_node).as_str() + ","
        //     + chord_util::gen_debug_str_of_data(data_id).as_str() + ","
        //     + chord_util::gen_debug_str_of_data(target_id).as_str() + ","
        //     + idx.to_string().as_str()
        // ));
    }

    //return Ok(true);
    return Ok(is_exist);
}

pub fn put(self_node: ArMu<node_info::NodeInfo>, data_store: ArMu<data_store::DataStore>, client_pool: ArMu<HashMap<String, ArMu<reqwest::Client>>>, key_id: u32, val_str: String) -> Result<bool, chord_util::GeneralError> {
    let self_node_deep_cloned;
    {
        let self_node_ref = self_node.lock().unwrap();
        self_node_deep_cloned = node_info::partial_clone_from_ref_strong(&self_node_ref);
        //drop(self_node_ref);
    }

    chord_util::dprint(
                    &("put_1,".to_string() 
                    + chord_util::gen_debug_str_of_node(&self_node_deep_cloned).as_str() + ","
                    + chord_util::gen_debug_str_of_data(key_id).as_str())
    );

    // 担当範囲（predecessorのidと自身のidの間）のデータであるかチェックする
    // そこに収まっていなかった場合、一定時間後リトライが行われるようエラーを返す
    // リクエストを受けるという実装も可能だが、stabilize処理で predecessor が生きて
    // いるノードとなるまで下手にデータを持たない方が、データ配置の整合性を壊すリスクが
    // 減りそうな気がするので、そうする
    if self_node_deep_cloned.predecessor_info.len() == 0 {
        return Err(chord_util::GeneralError::new("predecessor is None".to_string(), chord_util::ERR_CODE_PRED_IS_NONE));
    }

    chord_util::dprint(
        &("put_2,".to_string()
        + chord_util::gen_debug_str_of_node(&self_node_deep_cloned).as_str() + ","
        + chord_util::gen_debug_str_of_data(key_id).as_str() + "," 
        + val_str.clone().as_str())
    );

    
    // Chordネットワークを右回りにたどった時に、データの id (key_id) が predecessor の node_id から
    // 自身の node_id の間に位置する場合、そのデータは自身の担当だが、そうではない場合
    if chord_util::exist_between_two_nodes_right_mawari(
        self_node_deep_cloned.predecessor_info[0].node_id,
        self_node_deep_cloned.node_id, 
        key_id) == false {
            //return Err(chord_util::GeneralError::new("passed data is out of my tantou range".to_string(), chord_util::ERR_CODE_NOT_TANTOU));
            return Ok(false);
    }


    chord_util::dprint(
        &("put_3,".to_string()
        + chord_util::gen_debug_str_of_node(&self_node_deep_cloned).as_str() + ","
        + chord_util::gen_debug_str_of_data(key_id).as_str() + "," 
        + val_str.clone().as_str())
    );

    let ret;
    {
        let mut data_store_ref = data_store.lock().unwrap();
        ret = data_store_ref.store_one_iv(key_id, val_str.clone());
        //drop(data_store_ref);
    }

    chord_util::dprint(
            &("put_4,".to_string()
            + chord_util::gen_debug_str_of_node(&self_node_deep_cloned).as_str() + ","
            + chord_util::gen_debug_str_of_data(key_id).as_str() + "," 
            + val_str.clone().as_str() + ","
            + ret.to_string().as_str())
    );

    return Ok(ret);
}

// 得られた value の文字列を返す
// データの取得に失敗した場合は ERR_CODE_QUERIED_DATA_NOT_FOUND をエラーとして返す
// 取得対象のデータが削除済みのデータであった場合は DELETED_ENTRY_MARKING_STR が正常値として返る
pub async fn global_get(self_node: ArMu<node_info::NodeInfo>, data_store: ArMu<data_store::DataStore>, client_pool: ArMu<HashMap<String, ArMu<reqwest::Client>>>, key_str: String) -> Result<chord_util::DataIdAndValue, chord_util::GeneralError> {
    let self_node_deep_cloned;
    {
        let self_node_ref = self_node.lock().unwrap();
        self_node_deep_cloned = node_info::partial_clone_from_ref_strong(&self_node_ref);
        //drop(self_node_ref);
    }

    let data_id = chord_util::hash_str_to_int(&key_str);
    for idx in 0..(gval::REPLICA_NUM + 1) {
        let target_id = chord_util::overflow_check_and_conv(data_id as u64 + (gval::REPLICA_ID_DISTANCE as u64) * (idx as u64));
        let replica_node = match endpoints::rrpc_call__find_successor(&self_node_deep_cloned, Arc::clone(&client_pool), target_id, self_node_deep_cloned.node_id).await {
            Err(err) => {
                {
                    let mut self_node_ref = self_node.lock().unwrap();
                    node_info::handle_downed_node_info(&mut self_node_ref, &self_node_deep_cloned, &err);
                    //drop(self_node_ref);
                }
                continue;
                //return Err(err);
            }
            Ok(ninfo) => ninfo
        };

        // chord_util::dprint(&("global_get_1,".to_string() 
        //     + chord_util::gen_debug_str_of_node(&self_node_deep_cloned).as_str() + ","
        //     + chord_util::gen_debug_str_of_node(&replica_node).as_str() + ","
        //     + chord_util::gen_debug_str_of_data(data_id).as_str() + ","
        //     + chord_util::gen_debug_str_of_data(target_id).as_str() + ","
        //     + idx.to_string().as_str()
        // ));        
/*
    if (ret.is_ok):
        target_node: 'ChordNode' = cast('ChordNode', ret.result)
        # リトライは不要であったため、リトライ用情報の存在を判定するフィールドを
        # 初期化しておく
        ChordNode.need_put_retry_data_id = -1
    else:  # ret.err_code == ErrorCode.AppropriateNodeNotFoundException_CODE || ret.err_code == ErrorCode.InternalControlFlowException_CODE || ret.err_code == ErrorCode.NodeIsDownedException_CODE
        # 適切なノードを得られなかった、もしくは join処理中のノードを扱おうとしてしまい例外発生
        # となってしまったため次回呼び出し時にリトライする形で呼び出しをうけられるように情報を設定しておく
        ChordNode.need_put_retry_data_id = data_id
        ChordNode.need_put_retry_node = self
        ChordUtil.dprint("global_put_1,RETRY_IS_NEEDED" + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                        + ChordUtil.gen_debug_str_of_data(data_id))
        return False
*/

        let data_iv = match endpoints::rrpc_call__get(&node_info::gen_node_info_from_summary(&replica_node), Arc::clone(&data_store), Arc::clone(&client_pool), target_id, self_node_deep_cloned.node_id).await {
            Err(err) => {
                let mut self_node_ref = self_node.lock().unwrap();
                node_info::handle_downed_node_info(&mut self_node_ref, &node_info::gen_node_info_from_summary(&replica_node), &err);
                //drop(self_node_ref);
                continue;
                //return Err(err);
            }
            Ok(data_iv) => { 
                    // chord_util::dprint(&("global_get_2,".to_string() 
                    // + chord_util::gen_debug_str_of_node(&self_node_deep_cloned).as_str() + ","
                    // + chord_util::gen_debug_str_of_node(&replica_node).as_str() + ","
                    // + chord_util::gen_debug_str_of_data(data_id).as_str() + ","
                    // + chord_util::gen_debug_str_of_data(target_id).as_str() + ","
                    // + idx.to_string().as_str()));
                if data_iv.val_str == "Error" {
                    chord_util::dprint(&("TRY_GET_ERROR: tried_node_addr=".to_string() 
                    + replica_node.address_str.clone().as_str()
                    + " tried_node_id=" + replica_node.node_id.to_string().as_str()
                    ));
                    continue;
                }
                return Ok(data_iv); 
            }
        };
    }

    return Err(chord_util::GeneralError::new("QUERIED DATA NOT FOUND".to_string(), chord_util::ERR_CODE_QUERIED_DATA_NOT_FOUND));
}

pub fn get(self_node: ArMu<node_info::NodeInfo>, data_store: ArMu<data_store::DataStore>, key_id: u32) -> Result<chord_util::DataIdAndValue, chord_util::GeneralError> {
    let self_node_deep_cloned;
    {
        let self_node_ref = self_node.lock().unwrap();
        self_node_deep_cloned = node_info::partial_clone_from_ref_strong(&self_node_ref);
        //drop(self_node_ref);
    }

    chord_util::dprint(
                    &("get_1,".to_string() 
                    + chord_util::gen_debug_str_of_node(&self_node_deep_cloned).as_str() + ","
                    + chord_util::gen_debug_str_of_data(key_id).as_str())
    );

    // 担当範囲（predecessorのidと自身のidの間）のデータであるかチェックする
    // そこに収まっていなかった場合、一定時間後リトライが行われるようエラーを返す
    // リクエストを受けるという実装も可能だが、stabilize処理で predecessor が生きて
    // いるノードとなるまで下手にデータを持たない方が、データ配置の整合性を壊すリスクが
    // 減りそうな気がするので、そうする
    if self_node_deep_cloned.predecessor_info.len() == 0 {
        let ret_val = chord_util::DataIdAndValue { data_id: self_node_deep_cloned.node_id, val_str: "Error".to_string() };
        return Ok(ret_val);
        //return Err(chord_util::GeneralError::new("predecessor is None".to_string(), chord_util::ERR_CODE_PRED_IS_NONE));
    }

    chord_util::dprint(
        &("get_2,".to_string()
        + chord_util::gen_debug_str_of_node(&self_node_deep_cloned).as_str() + ","
        + chord_util::gen_debug_str_of_data(key_id).as_str())
    );


    // Chordネットワークを右回りにたどった時に、データの id (key_id) が predecessor の node_id から
    // 自身の node_id の間に位置する場合、そのデータは自身の担当だが、そうではない場合
    if chord_util::exist_between_two_nodes_right_mawari(
        self_node_deep_cloned.predecessor_info[0].node_id,
        self_node_deep_cloned.node_id, 
        key_id) == false {
            //return Err(chord_util::GeneralError::new("passed data is out of my tantou range".to_string(), chord_util::ERR_CODE_NOT_TANTOU));
            return Ok(chord_util::DataIdAndValue { data_id: self_node_deep_cloned.node_id, val_str: "passed data is out of my tantou range".to_string() })
    }


    chord_util::dprint(
        &("get_3,".to_string()
        + chord_util::gen_debug_str_of_node(&self_node_deep_cloned).as_str() + ","
        + chord_util::gen_debug_str_of_data(key_id).as_str())
    );

    let ret_val;
    {
        let data_store_ref = data_store.lock().unwrap();
        ret_val = match data_store_ref.get(key_id){
            Err(err) => {
                let ret_val = chord_util::DataIdAndValue { data_id: self_node_deep_cloned.node_id, val_str: "Error".to_string() };
                return Ok(ret_val);
                //return Err(err);
            }
            Ok(data_iv) => {
                if data_iv.val_str == data_store::DELETED_ENTRY_MARKING_STR.to_string() {
                    let ret_val = chord_util::DataIdAndValue { data_id: 0, val_str: data_store::DELETED_ENTRY_MARKING_STR.to_string() };
                    return Ok(ret_val);                    
                    //return Err(chord_util::GeneralError::new(data_store::DELETED_ENTRY_MARKING_STR.to_string(), chord_util::ERR_CODE_DATA_TO_GET_IS_DELETED));
                }
                data_iv
            }
        };
        //drop(data_store_ref);
    }

    chord_util::dprint(
            &("get_4,".to_string()
            + chord_util::gen_debug_str_of_node(&self_node_deep_cloned).as_str() + ","
            + chord_util::gen_debug_str_of_data(key_id).as_str() + "," 
            + ret_val.val_str.clone().as_str())
    );

    return Ok(ret_val);
}

pub async fn global_delete(self_node: ArMu<node_info::NodeInfo>, data_store: ArMu<data_store::DataStore>, client_pool: ArMu<HashMap<String, ArMu<reqwest::Client>>>, key_str: String) -> Result<bool, chord_util::GeneralError> {
    match global_get(Arc::clone(&self_node), Arc::clone(&data_store), Arc::clone(&client_pool), key_str.clone()).await {
        Err(err) => { return Err(err); }
        Ok(data_iv) => {
            match global_put(Arc::clone(&self_node), Arc::clone(&data_store), Arc::clone(&client_pool), key_str, data_store::DELETED_ENTRY_MARKING_STR.to_string()).await {
                Err(err) => { return Err(err); }
                Ok(is_exist) => {
                    return Ok(is_exist);
                }
            }
        }
    }
}

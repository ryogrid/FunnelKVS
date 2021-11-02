use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::cell::RefCell;
use std::ops::Range;

use crate::gval;
use crate::chord_node;
use crate::node_info;
use crate::chord_util;
use crate::stabilizer;
use crate::router;
use crate::endpoints;

pub const DELETED_ENTRY_MARKING_STR : &str = "THIS_KEY_IS_DELETED";

type ArMu<T> = Arc<Mutex<T>>;

#[derive(Debug, Clone)]
pub struct DataStore {
    // Keyはハッシュを通されたものなので元データの値とは異なる
    stored_data : HashMap<String, chord_util::DataIdAndValue>,
}

impl DataStore {
    pub fn new() -> DataStore {
        let sd = HashMap::new();
        DataStore {stored_data : sd}
    }

    pub fn store_one_iv(& mut self, data_id: u32, value_str: String) -> bool {
        let iv_entry = chord_util::DataIdAndValue::new(data_id, value_str.clone());
        match self.stored_data.insert(data_id.to_string(), iv_entry){
            None => { return false; }
            Some(_old_val) => { return true; }
        };
    }
            
    pub fn get(&self, data_id: u32) -> Result<chord_util::DataIdAndValue, chord_util::GeneralError>{
        match self.stored_data.get(&data_id.to_string()){
            None => {
                return Err(chord_util::GeneralError::new("GET REQUESTED DATA IS NOT FOUND".to_string(), chord_util::ERR_CODE_DATA_TO_GET_NOT_FOUND));
            }
            Some(data_iv) => {
                return Ok(chord_util::iv_clone_from_ref(&data_iv));
            }
        }
    }

    pub fn remove_one_data(&mut self, key_id: u32){
        self.stored_data.remove(&key_id.to_string());
    }

    pub fn store_iv_with_vec(&mut self, iv_vec: Vec<chord_util::DataIdAndValue>){
        for each_iv in iv_vec {
            self.store_one_iv(each_iv.data_id, each_iv.val_str);
        }
    }

    // 自身のノードIDとpredecessorのノードIDを指定すると、自身の担当範囲外のデータを削除し、同時に削除したデータ
    // のリストが返る
    pub fn get_and_delete_iv_with_pred_self_id(&mut self, pred_id: u32, self_id: u32) -> Vec<chord_util::DataIdAndValue> {
        let mut ret_vec: Vec<chord_util::DataIdAndValue> = vec![];
        for (key, value) in &self.stored_data {
            let data_id: u32 = key.parse().unwrap();
            if chord_util::exist_between_two_nodes_right_mawari(self_id, pred_id, data_id) == true {
                ret_vec.push((*value).clone());                
            }
        }
        // ret_vecに詰めたデータを self.stored_data から削除する
        for entry in &ret_vec {
            self.remove_one_data(entry.data_id);
        }

        return ret_vec;
    }    

}

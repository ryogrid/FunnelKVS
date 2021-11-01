use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::cell::RefCell;

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

    pub fn store_new_data(& mut self, data_id: u32, value_str: String) -> bool {
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
}

use std::sync::{Arc, Mutex};
use std::borrow::{Borrow, BorrowMut};
use std::cell::{RefMut, RefCell, Ref};
use std::sync::atomic::Ordering;
use std::time::Duration;

use crate::gval;
//use crate::chord_node::ChordNode;
use crate::node_info;
use crate::chord_util;
use crate::stabilizer;
use crate::endpoints;
use crate::data_store;

type ArMu<T> = Arc<Mutex<T>>;

// idで識別されるデータを担当するノードの名前解決を行う
pub fn find_successor(self_node: ArMu<node_info::NodeInfo>, id : u32) -> Result<node_info::NodeInfo, chord_util::GeneralError> {
    let mut self_node_ref = self_node.lock().unwrap();
    let deep_cloned_self_node = node_info::partial_clone_from_ref_strong(&self_node_ref);
    drop(self_node_ref);

    chord_util::dprint(&("find_successor_1,".to_string() + chord_util::gen_debug_str_of_node(&deep_cloned_self_node).as_str() + ","
                + chord_util::gen_debug_str_of_data(id).as_str()));
    
    let n_dash = match find_predecessor(&deep_cloned_self_node, id){
        Err(err) => {
            return Err(chord_util::GeneralError::new(err.message, err.err_code));
        }
        Ok(ninfo) => ninfo
    };

    chord_util::dprint(&("find_successor_3,".to_string() + chord_util::gen_debug_str_of_node(&deep_cloned_self_node).as_str() + ","
                        + chord_util::gen_debug_str_of_node(&n_dash).as_str() + ","
                        + chord_util::gen_debug_str_of_node(&deep_cloned_self_node.successor_info_list[0]).as_str() + ","
                        + chord_util::gen_debug_str_of_data(id).as_str()));

    let asked_n_dash_info = match endpoints::rrpc_call__get_node_info(&n_dash.address_str) {
        Err(err) => {
            self_node_ref = self_node.lock().unwrap();
            node_info::handle_downed_node_info(&mut self_node_ref, &n_dash, &err);
            return Err(chord_util::GeneralError::new(err.message, err.err_code));
        }
        Ok(got_node) => {                
            got_node
        }
    };
    
    match endpoints::rrpc_call__get_node_info(&asked_n_dash_info.successor_info_list[0].address_str) {
        Err(err) => {
            self_node_ref = self_node.lock().unwrap();
            node_info::handle_downed_node_info(&mut self_node_ref, &asked_n_dash_info.successor_info_list[0], &err);
            return Err(chord_util::GeneralError::new(err.message, err.err_code));
        }
        Ok(got_node) => {                
            return Ok(got_node.clone());
        }
    };
}
 
// id の前で一番近い位置に存在するノードを探索する
pub fn find_predecessor(exnode_ni_ref: &node_info::NodeInfo, id: u32) -> Result<node_info::NodeInfo, chord_util::GeneralError> {
    let mut n_dash: node_info::NodeInfo = node_info::partial_clone_from_ref_strong(exnode_ni_ref);
    let mut n_dash_found: node_info::NodeInfo = node_info::partial_clone_from_ref_strong(exnode_ni_ref);

    chord_util::dprint(&("find_predecessor_1,".to_string() + chord_util::gen_debug_str_of_node(&exnode_ni_ref).as_str()));
    
    // n_dash と n_dashのsuccessorの 間に id が位置するような n_dash を見つけたら、ループを終了し n_dash を return する
    loop {
        // 1周目でも実質的に同じ値が入るようになっているので問題ない
        n_dash = n_dash_found;

        //while文の書き換えの形でできたif文
        if chord_util::exist_between_two_nodes_right_mawari(n_dash.node_id, n_dash.successor_info_list[0].node_id, id) {
            println!("check loop break at find_predecessor {:?} {:?}", n_dash.node_id, n_dash.successor_info_list[0].node_id);
            break;
        }

        chord_util::dprint(&("find_predecessor_2,".to_string() + chord_util::gen_debug_str_of_node(exnode_ni_ref).as_str() + ","
                            + chord_util::gen_debug_str_of_node(&n_dash).as_str()));

        n_dash_found = match endpoints::rrpc_call__closest_preceding_finger(&n_dash, id){
            Err(err) => {
                return Err(chord_util::GeneralError::new(err.message, err.err_code));
            }
            Ok(ninfo) => ninfo
        };

        if n_dash_found.node_id == n_dash.node_id {
            // 見つかったノードが、n_dash と同じで、変わらなかった場合
            // 同じを経路表を用いて探索することになり、結果は同じになり無限ループと
            // なってしまうため、探索は継続せず、探索結果として n_dash (= n_dash_found) を返す
            chord_util::dprint(&("find_predecessor_3,".to_string() + chord_util::gen_debug_str_of_node(exnode_ni_ref).as_str() + ","
                                + chord_util::gen_debug_str_of_node(&n_dash).as_str()));
            return Ok(n_dash_found.clone());
        }

        // closelst_preceding_finger は id を通り越してしまったノードは返さない
        // という前提の元で以下のチェックを行う
        let distance_old = chord_util::calc_distance_between_nodes_right_mawari(exnode_ni_ref.node_id, n_dash.node_id);
        let distance_found = chord_util::calc_distance_between_nodes_right_mawari(exnode_ni_ref.node_id, n_dash_found.node_id);
        let distance_data_id = chord_util::calc_distance_between_nodes_right_mawari(exnode_ni_ref.node_id, id);
        if distance_found < distance_old && !(distance_old >= distance_data_id) {
            // 探索を続けていくと n_dash は id に近付いていくはずであり、それは上記の前提を踏まえると
            // 自ノードからはより遠い位置の値になっていくということのはずである
            // 従って、そうなっていなかった場合は、繰り返しを継続しても意味が無く、最悪、無限ループになってしまう
            // 可能性があるため、探索を打ち切り、探索結果は古いn_dashを返す.
            // ただし、古い n_dash が 一回目の探索の場合 self であり、同じ node_idの距離は ID_SPACE_RANGE となるようにしている
            // ため、上記の条件が常に成り立ってしまう. 従って、その場合は例外とする（n_dashが更新される場合は、更新されたn_dashのnode_idが
            // 探索対象のデータのid を通り越すことは無い）

            chord_util::dprint(&("find_predecessor_4,".to_string() + chord_util::gen_debug_str_of_node(exnode_ni_ref).as_str() + ","
                                + chord_util::gen_debug_str_of_node(&n_dash).as_str()));

            return Ok(n_dash.clone());
        }

        chord_util::dprint(&("find_predecessor_5_n_dash_updated,".to_string() + chord_util::gen_debug_str_of_node(exnode_ni_ref).as_str() + ","
                            + chord_util::gen_debug_str_of_node(&n_dash).as_str() + "->"
                            + chord_util::gen_debug_str_of_node(&n_dash_found).as_str()));

        // チェックの結果問題ないので n_dashを closest_preceding_fingerで探索して得た
        // ノード情報は次周のループの先頭でn_dash_foundに置き換えられる
    }

    return Ok(n_dash.clone());
}

//  自身の持つ経路情報をもとに,  id から前方向に一番近いノードの情報を返す
pub fn closest_preceding_finger(self_node: ArMu<node_info::NodeInfo>, id : u32) -> Result<node_info::NodeInfo, chord_util::GeneralError> {
    // 範囲の広いエントリから探索していく
    // finger_tableはインデックスが小さい方から大きい方に、範囲が大きくなっていく
    // ように構成されているため、リバースしてインデックスの大きな方から小さい方へ
    // 順に見ていくようにする

    chord_util::dprint(&"closest_preceding_finger_start".to_string());

    let mut self_node_ref = self_node.lock().unwrap();
    let deep_cloned_self_node = node_info::partial_clone_from_ref_strong(&self_node_ref);
    drop(self_node_ref);

    for node_info in (&deep_cloned_self_node).finger_table.iter().rev() {
        let conved_node_info = match node_info {
            None => {
                chord_util::dprint(&("closest_preceding_finger_0,".to_string() + chord_util::gen_debug_str_of_node(&deep_cloned_self_node).as_str()));
                continue;
            },
            Some(ni) => ni
        };

        chord_util::dprint(&("closest_preceding_finger_1,".to_string() + chord_util::gen_debug_str_of_node(&deep_cloned_self_node).as_str() + ","
            + chord_util::gen_debug_str_of_node(&conved_node_info).as_str()));

        // テーブル内のエントリが保持しているノードのIDが自身のIDと探索対象のIDの間にあれば
        // それを返す
        // (大きな範囲を見た場合、探索対象のIDが自身のIDとエントリが保持しているノードのIDの中に含まれて
        //  しまっている可能性が高く、エントリが保持しているノードが、探索対象のIDを飛び越してしまっている
        //  可能性が高いということになる。そこで探索範囲を狭めていって、飛び越さない範囲で一番近いノードを
        //  見つけるという処理になっていると思われる）
        if chord_util::exist_between_two_nodes_right_mawari(deep_cloned_self_node.node_id, id, conved_node_info.node_id) {

            chord_util::dprint(&("closest_preceding_finger_2,".to_string() + chord_util::gen_debug_str_of_node(&deep_cloned_self_node).as_str() + ","
                            + chord_util::gen_debug_str_of_node(&conved_node_info).as_str()));

            let gnba_rslt = match endpoints::rrpc_call__get_node_info(&conved_node_info.address_str){
                Err(err) => {
                    self_node_ref = self_node.lock().unwrap();
                    node_info::handle_downed_node_info(&mut self_node_ref, &conved_node_info, &err);
                    return Err(chord_util::GeneralError::new(err.message, err.err_code));
                }
                Ok(got_node) => {                
                    return Ok(got_node);
                }
            };
        }
    }

    chord_util::dprint(&"closest_preceding_finger_3".to_string());

    // どんなに範囲を狭めても探索対象のIDを超えてしまうノードしか存在しなかった場合
    // 自身の知っている情報の中で対象を飛び越さない範囲で一番近いノードは自身という
    // ことになる
    return Ok(deep_cloned_self_node);
}


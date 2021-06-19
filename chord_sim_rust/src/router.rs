/*
# coding:utf-8

import modules.gval as gval
from .chord_util import ChordUtil, NodeIsDownedExceptiopn, \
    AppropriateNodeNotFoundException, InternalControlFlowException, PResult, ErrorCode

class Router:

    def __init__(self, existing_node : 'ChordNode'):
        self.existing_node : 'ChordNode' = existing_node

    # id（int）で識別されるデータを担当するノードの名前解決を行う
    # Attention: 適切な担当ノードを得ることができなかった場合、FindNodeFailedExceptionがraiseされる
    # TODO: AppropriateExp, DownedExp, InternalExp at find_successor
    def find_successor(self, id : int) -> PResult[Optional['ChordNode']]:
        # TODO: ここでのロックをはじめとしてRust実装ではロック対象を更新するか否かでRWロックを使い分けるようにする. at find_successor
        #       そうでないと、少なくともglobal_xxxの呼び出しを同一ノードもしくは、いくつかのノードに行うような運用でクエリが並列に
        #       動作せず、パフォーマンスが出ないはず
        if self.existing_node.node_info.lock_of_succ_infos.acquire(timeout=gval.LOCK_ACQUIRE_TIMEOUT) == False:
            # 失敗させる
            ChordUtil.dprint("find_successor_0," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + "LOCK_ACQUIRE_TIMEOUT")
            return PResult.Err(None, ErrorCode.InternalControlFlowException_CODE)

        if self.existing_node.is_alive == False:
            # 処理の合間でkillされてしまっていた場合の考慮
            # 何もしないで終了する
            self.existing_node.node_info.lock_of_succ_infos.release()
            ChordUtil.dprint("find_successor_0_5," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + "REQUEST_RECEIVED_BUT_I_AM_ALREADY_DEAD")
            return PResult.Err(None, ErrorCode.NodeIsDownedException_CODE)

        try:
            ChordUtil.dprint("find_successor_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                  + ChordUtil.gen_debug_str_of_data(id))

            n_dash = self.find_predecessor(id)
            if n_dash == None:
                ChordUtil.dprint("find_successor_2," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_data(id))
                return PResult.Err(None, ErrorCode.AppropriateNodeNotFoundException_CODE)

            # TODO: x direct access to node_info of n_dash at find_successor
            ChordUtil.dprint("find_successor_3," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + ChordUtil.gen_debug_str_of_node(n_dash.node_info) + ","
                             + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]) + ","
                             + ChordUtil.gen_debug_str_of_data(id))

            # 取得しようとしたノードがダウンしていた場合 AppropriateNodeNotFoundException が raise される
            # TODO: direct access to successor_info_list of n_dash at find_successor
            ret = ChordUtil.get_node_by_address(n_dash.node_info.successor_info_list[0].address_str)
            if(ret.is_ok):
                n_dash_successor : 'ChordNode' = cast('ChordNode', ret.result)
                return PResult.Ok(n_dash_successor)
            else: # ret.err_code == ErrorCode.InternalControlFlowException_CODE || ret.err_code == ErrorCode.NodeIsDownedException_CODE
                # ここでは何も対処しない
                ChordUtil.dprint("find_successor_4,FOUND_NODE_IS_DOWNED," + ChordUtil.gen_debug_str_of_node(
                    self.existing_node.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_data(id))
                return PResult.Err(None, ErrorCode.AppropriateNodeNotFoundException_CODE)
        finally:
            self.existing_node.node_info.lock_of_succ_infos.release()

    # id(int)　の前で一番近い位置に存在するノードを探索する
    def find_predecessor(self, id: int) -> 'ChordNode':
        ChordUtil.dprint("find_predecessor_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info))

        n_dash : 'ChordNode' = self.existing_node

        if self.existing_node.node_info.lock_of_succ_infos.acquire(timeout=gval.LOCK_ACQUIRE_TIMEOUT) == False:
            # 最初の n_dash を返してしまい、find_predecessorは失敗したと判断させる
            ChordUtil.dprint("find_predecessor_1_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + "LOCK_ACQUIRE_TIMEOUT")
            return n_dash
        try:
            # n_dash と n_dashのsuccessorの 間に id が位置するような n_dash を見つけたら、ループを終了し n_dash を return する
            # TODO: direct access to node_id and successor_info_list of n_dash at find_predecessor
            while not ChordUtil.exist_between_two_nodes_right_mawari(n_dash.node_info.node_id, n_dash.node_info.successor_info_list[0].node_id, id):
                # TODO: x direct access to node_info of n_dash at find_predecessor
                ChordUtil.dprint("find_predecessor_2," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(n_dash.node_info))
                # TODO: closest_preceding_finger call at find_predecessor
                n_dash_found = n_dash.endpoints.grpc__closest_preceding_finger(id)

                # TODO: x direct access to node_info of n_dash_found and n_dash at find_predecessor
                if n_dash_found.node_info.node_id == n_dash.node_info.node_id:
                    # 見つかったノードが、n_dash と同じで、変わらなかった場合
                    # 同じを経路表を用いて探索することになり、結果は同じになり無限ループと
                    # なってしまうため、探索は継続せず、探索結果として n_dash (= n_dash_found) を返す
                    # TODO: x direct access to node_info of n_dash at find_predecessor
                    ChordUtil.dprint("find_predecessor_3," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                     + ChordUtil.gen_debug_str_of_node(n_dash.node_info))
                    return n_dash_found

                # closelst_preceding_finger は id を通り越してしまったノードは返さない
                # という前提の元で以下のチェックを行う
                # TODO: x direct access to node_info of n_dash at find_predecessor
                distance_old = ChordUtil.calc_distance_between_nodes_right_mawari(self.existing_node.node_info.node_id, n_dash.node_info.node_id)
                # TODO: x direct access to node_info of n_dash_found at find_predecessor
                distance_found = ChordUtil.calc_distance_between_nodes_right_mawari(self.existing_node.node_info.node_id, n_dash_found.node_info.node_id)
                distance_data_id = ChordUtil.calc_distance_between_nodes_right_mawari(self.existing_node.node_info.node_id, id)
                if distance_found < distance_old and not (distance_old >= distance_data_id):
                    # 探索を続けていくと n_dash は id に近付いていくはずであり、それは上記の前提を踏まえると
                    # 自ノードからはより遠い位置の値になっていくということのはずである
                    # 従って、そうなっていなかった場合は、繰り返しを継続しても意味が無く、最悪、無限ループになってしまう
                    # 可能性があるため、探索を打ち切り、探索結果は古いn_dashを返す.
                    # ただし、古い n_dash が 一回目の探索の場合 self であり、同じ node_idの距離は ID_SPACE_RANGE となるようにしている
                    # ため、上記の条件が常に成り立ってしまう. 従って、その場合は例外とする（n_dashが更新される場合は、更新されたn_dashのnode_idが
                    # 探索対象のデータのid を通り越すことは無い）

                    # TODO: x direct access to node_info of n_dash at find_predecessor
                    ChordUtil.dprint("find_predecessor_4," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                     + ChordUtil.gen_debug_str_of_node(n_dash.node_info))

                    return n_dash

                # TODO: x direct access to node_info of n_dash and n_dash_found at find_predecessor
                ChordUtil.dprint("find_predecessor_5_n_dash_updated," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(n_dash.node_info) + "->"
                                 + ChordUtil.gen_debug_str_of_node(n_dash_found.node_info))

                # チェックの結果問題ないので n_dashを closest_preceding_fingerで探索して得た
                # ノード情報 n_dash_foundに置き換える
                n_dash = n_dash_found
        finally:
            self.existing_node.node_info.lock_of_succ_infos.release()

        return n_dash

    #  自身の持つ経路情報をもとに,  id から前方向に一番近いノードの情報を返す
    def closest_preceding_finger(self, id : int) -> 'ChordNode':
        # 範囲の広いエントリから探索していく
        # finger_tableはインデックスが小さい方から大きい方に、範囲が大きくなっていく
        # ように構成されているため、リバースしてインデックスの大きな方から小さい方へ
        # 順に見ていくようにする
        for node_info in reversed(self.existing_node.node_info.finger_table):
            # 埋まっていないエントリも存在し得る
            if node_info == None:
                ChordUtil.dprint("closest_preceding_finger_0," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info))
                continue

            casted_node_info = cast('NodeInfo', node_info)

            ChordUtil.dprint("closest_preceding_finger_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                  + ChordUtil.gen_debug_str_of_node(casted_node_info))

            # テーブル内のエントリが保持しているノードのIDが自身のIDと探索対象のIDの間にあれば
            # それを返す
            # (大きな範囲を見た場合、探索対象のIDが自身のIDとエントリが保持しているノードのIDの中に含まれて
            #  しまっている可能性が高く、エントリが保持しているノードが、探索対象のIDを飛び越してしまっている
            #  可能性が高いということになる。そこで探索範囲を狭めていって、飛び越さない範囲で一番近いノードを
            #  見つけるという処理になっていると思われる）
            # #if self.existing_node.node_info.node_id < entry.node_id and entry.node_id <= id:
            if ChordUtil.exist_between_two_nodes_right_mawari(self.existing_node.node_info.node_id, id, casted_node_info.node_id):
                ChordUtil.dprint("closest_preceding_finger_2," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(casted_node_info))

                                 ret = ChordUtil.get_node_by_address(casted_node_info.address_str)
                if (ret.is_ok):
                    casted_node : 'ChordNode' = cast('ChordNode', ret.result)
                    return casted_node
                else:  # ret.err_code == ErrorCode.InternalControlFlowException_CODE || ret.err_code == ErrorCode.NodeIsDownedException_CODE
                    # ここでは何も対処しない
                    continue

        ChordUtil.dprint("closest_preceding_finger_3")

        # どんなに範囲を狭めても探索対象のIDを超えてしまうノードしか存在しなかった場合
        # 自身の知っている情報の中で対象を飛び越さない範囲で一番近いノードは自身という
        # ことになる
        return self.existing_node
*/
use std::sync::Arc;
use std::borrow::{Borrow, BorrowMut};
use std::cell::{RefMut, RefCell, Ref};
use std::sync::atomic::Ordering;
use std::time::Duration;

use parking_lot::{ReentrantMutex, const_reentrant_mutex};

use crate::gval;
use crate::chord_node::{self, ChordNode};
use crate::node_info;
use crate::chord_util;
use crate::stabilizer;
use crate::taskqueue;
use crate::endpoints;
use crate::data_store;

type ArRmRs<T> = Arc<ReentrantMutex<RefCell<T>>>;

/*
#[derive(Debug, Clone)]
pub struct Router {
//    pub existing_node : ArRmRs<chord_node::ChordNode>,
}

impl Router {
    pub fn new() -> Router {
        Router {}
    }
}
*/

// id（int）で識別されるデータを担当するノードの名前解決を行う
// Attention: 適切な担当ノードを得ることができなかった場合、FindNodeFailedExceptionがraiseされる
// TODO: AppropriateExp, DownedExp, InternalExp at find_successor
pub fn find_successor(existing_node: ArRmRs<chord_node::ChordNode>, exnode_ref: &Ref<chord_node::ChordNode>, exnode_ni_ref: &Ref<node_info::NodeInfo>, id : i32) -> Result<ArRmRs<chord_node::ChordNode>, chord_util::GeneralError> {
    // TODO: ここでのロックをはじめとしてRust実装ではロック対象を更新するか否かでRWロックを使い分けるようにする. at find_successor
    //       そうでないと、少なくともglobal_xxxの呼び出しを同一ノードもしくは、いくつかのノードに行うような運用でクエリが並列に
    //       動作せず、パフォーマンスが出ないはず

    // TODO: 実システム化する際か、シミュレータでもノード丸ごとロックしてしまう実装から触るデータを個別にロックする
    //       作りの検証を行う場合はこの手のコードを復活させる必要がある
    // if self.existing_node.node_info.lock_of_succ_infos.acquire(timeout=gval.LOCK_ACQUIRE_TIMEOUT) == False:
    //     # 失敗させる
    //     ChordUtil.dprint("find_successor_0," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
    //                      + "LOCK_ACQUIRE_TIMEOUT")
    //     return PResult.Err(None, ErrorCode.InternalControlFlowException_CODE)

    if exnode_ref.is_alive.load(Ordering::Relaxed) == false {
        // 処理の合間でkillされてしまっていた場合の考慮
        // 何もしないで終了する
        chord_util::dprint(&("find_successor_0_5,".to_string() + chord_util::gen_debug_str_of_node(Some(exnode_ni_ref)).as_str() + ","
                            + "REQUEST_RECEIVED_BUT_I_AM_ALREADY_DEAD"));
        return Err(chord_util::GeneralError::new("".to_string(), chord_util::ERR_CODE_NODE_IS_DOWNED));
    }

    chord_util::dprint(&("find_successor_1,".to_string() + chord_util::gen_debug_str_of_node(Some(exnode_ni_ref)).as_str() + ","
            + chord_util::gen_debug_str_of_data(id).as_str()));

    let n_dash = exnode_ref.router.find_predecessor(Arc::clone(&existing_node), exnode_ni_ref, id);
    let n_dash_refcell = get_refcell_from_arc_with_locking!(n_dash);
    let n_dash_ref = get_ref_from_refcell!(n_dash_refcell);
    let n_dash_ni_refcell = get_refcell_from_arc_with_locking!(n_dash_ref.node_info);
    let n_dash_ninfo = get_ref_from_refcell!(n_dash_ni_refcell);

    // below comment-outed code has been not needed at porting
    // if n_dash == None {
    //     chord_util::dprint(&("find_successor_2,".to_string() + chord_util::gen_debug_str_of_node(Some(exnode_ni_ref)).as_str() + ","
    //                         + chord_util::gen_debug_str_of_data(id).as_str()));
    //     return Err(chord_util::GeneralError::new("".to_string(), chord_util::ERR_CODE_APPROPRIATE_NODE_NOT_FOND));
    // }


    // TODO: x direct access to node_info of n_dash at find_successor
    chord_util::dprint(&("find_successor_3,".to_string() + chord_util::gen_debug_str_of_node(Some(exnode_ni_ref)).as_str() + ","
                        + chord_util::gen_debug_str_of_node(Some(n_dash_ninfo)).as_str() + ","
                        + chord_util::gen_debug_str_of_node(Some(&exnode_ni_ref.successor_info_list[0])).as_str() + ","
                        + chord_util::gen_debug_str_of_data(id).as_str()));

    
    // TODO: direct access to successor_info_list of n_dash at find_successor
    match chord_util::get_node_by_address(&n_dash_ninfo.successor_info_list[0].address_str) {
        Err(err_code) => {
            // ret.err_code == ErrorCode.InternalControlFlowException_CODE || ret.err_code == ErrorCode.NodeIsDownedException_CODE
            // ここでは何も対処しない
            chord_util::dprint(&("find_successor_4,FOUND_NODE_IS_DOWNED,".to_string()
            + chord_util::gen_debug_str_of_node(Some(exnode_ni_ref)).as_str() + ","
            + chord_util::gen_debug_str_of_data(id).as_str()));
            return Err(chord_util::GeneralError::new("".to_string(), chord_util::ERR_CODE_APPROPRIATE_NODE_NOT_FOND));
        },
        Ok(got_node) => {                
            return Ok(got_node);
        }
    }
}


/*
    # id（int）で識別されるデータを担当するノードの名前解決を行う
    # Attention: 適切な担当ノードを得ることができなかった場合、FindNodeFailedExceptionがraiseされる
    # TODO: AppropriateExp, DownedExp, InternalExp at find_successor
    def find_successor(self, id : int) -> PResult[Optional['ChordNode']]:
        # TODO: ここでのロックをはじめとしてRust実装ではロック対象を更新するか否かでRWロックを使い分けるようにする. at find_successor
        #       そうでないと、少なくともglobal_xxxの呼び出しを同一ノードもしくは、いくつかのノードに行うような運用でクエリが並列に
        #       動作せず、パフォーマンスが出ないはず
        if self.existing_node.node_info.lock_of_succ_infos.acquire(timeout=gval.LOCK_ACQUIRE_TIMEOUT) == False:
            # 失敗させる
            ChordUtil.dprint("find_successor_0," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + "LOCK_ACQUIRE_TIMEOUT")
            return PResult.Err(None, ErrorCode.InternalControlFlowException_CODE)

        if self.existing_node.is_alive == False:
            # 処理の合間でkillされてしまっていた場合の考慮
            # 何もしないで終了する
            self.existing_node.node_info.lock_of_succ_infos.release()
            ChordUtil.dprint("find_successor_0_5," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + "REQUEST_RECEIVED_BUT_I_AM_ALREADY_DEAD")
            return PResult.Err(None, ErrorCode.NodeIsDownedException_CODE)

        try:
            ChordUtil.dprint("find_successor_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                  + ChordUtil.gen_debug_str_of_data(id))

            n_dash = self.find_predecessor(id)
            if n_dash == None:
                ChordUtil.dprint("find_successor_2," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_data(id))
                return PResult.Err(None, ErrorCode.AppropriateNodeNotFoundException_CODE)

            # TODO: x direct access to node_info of n_dash at find_successor
            ChordUtil.dprint("find_successor_3," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + ChordUtil.gen_debug_str_of_node(n_dash.node_info) + ","
                             + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]) + ","
                             + ChordUtil.gen_debug_str_of_data(id))

            # 取得しようとしたノードがダウンしていた場合 AppropriateNodeNotFoundException が raise される
            # TODO: direct access to successor_info_list of n_dash at find_successor
            ret = ChordUtil.get_node_by_address(n_dash.node_info.successor_info_list[0].address_str)
            if(ret.is_ok):
                n_dash_successor : 'ChordNode' = cast('ChordNode', ret.result)
                return PResult.Ok(n_dash_successor)
            else: # ret.err_code == ErrorCode.InternalControlFlowException_CODE || ret.err_code == ErrorCode.NodeIsDownedException_CODE
                # ここでは何も対処しない
                ChordUtil.dprint("find_successor_4,FOUND_NODE_IS_DOWNED," + ChordUtil.gen_debug_str_of_node(
                    self.existing_node.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_data(id))
                return PResult.Err(None, ErrorCode.AppropriateNodeNotFoundException_CODE)
        finally:
            self.existing_node.node_info.lock_of_succ_infos.release()
*/


// id(int)　の前で一番近い位置に存在するノードを探索する
pub fn find_predecessor(existing_node: ArRmRs<chord_node::ChordNode>, exnode_ni_ref: &Ref<node_info::NodeInfo>, id: i32) -> ArRmRs<chord_node::ChordNode> {
/*
    let exnode_refcell = get_refcell_from_arc_with_locking!(existing_node);
    let exnode_ref = get_ref_from_refcell!(exnode_refcell);

    let exnode_ni_refcell = get_refcell_from_arc_with_locking!(exnode_ref.node_info);
    let exnode_ni_ref = get_ref_from_refcell!(exnode_ni_refcell);

    let exnode_ft_refcell = get_refcell_from_arc_with_locking!(exnode_ni_ref.finger_table);
    let exnode_ft_ref = get_ref_from_refcell!(exnode_ft_refcell);

    let exnode_succ_list_refcell = get_refcell_from_arc_with_locking!(exnode_ni_ref.successor_info_list);
    let exnode_succ_list_ref = get_ref_from_refcell!(exnode_succ_list_refcell);
*/
    let n_dash = Arc::clone(&existing_node);
    let n_dash_ninfo = exnode_ni_ref;
    chord_util::dprint(&("find_predecessor_1,".to_string() + chord_util::gen_debug_str_of_node(Some(exnode_ni_ref)).as_str()));

    // n_dash と n_dashのsuccessorの 間に id が位置するような n_dash を見つけたら、ループを終了し n_dash を return する
    // TODO: direct access to node_id and successor_info_list of n_dash at find_predecessor
    while !chord_util::exist_between_two_nodes_right_mawari(n_dash_ninfo.node_id, n_dash_ninfo.successor_info_list[0].node_id, id) {
        // TODO: x direct access to node_info of n_dash at find_predecessor
        chord_util::dprint(&("find_predecessor_2,".to_string() + chord_util::gen_debug_str_of_node(Some(exnode_ni_ref)).as_str() + ","
                            + chord_util::gen_debug_str_of_node(Some(n_dash_ninfo)).as_str()));
        // TODO: closest_preceding_finger call at find_predecessor

        let n_dash_refcell = get_refcell_from_arc_with_locking!(n_dash);
        let n_dash_ref = get_ref_from_refcell!(n_dash_refcell);
    
        let n_dash_ni_refcell = get_refcell_from_arc_with_locking!(n_dash_ref.node_info);
        let n_dash_ninfo = get_ref_from_refcell!(n_dash_ni_refcell);

        let n_dash_endpoints_refcell = get_refcell_from_arc_with_locking!(n_dash_ref.endpoints);
        let n_dash_endpoints_ref = get_ref_from_refcell!(n_dash_endpoints_refcell);
        let n_dash_found = n_dash_endpoints_ref.grpc__closest_preceding_finger(Arc::clone(&existing_node), n_dash_ref, exnode_ni_ref, id);

        let n_dash_found_refcell = get_refcell_from_arc_with_locking!(n_dash_found);
        let n_dash_found_ref = get_ref_from_refcell!(n_dash_found_refcell);
    
        let n_dash_found_ni_refcell = get_refcell_from_arc_with_locking!(n_dash_found_ref.node_info);
        let n_dash_found_ni_ref = get_ref_from_refcell!(n_dash_found_ni_refcell);

        // TODO: x direct access to node_info of n_dash_found and n_dash at find_predecessor
        if n_dash_found_ni_ref.node_id == n_dash_ninfo.node_id {
            // 見つかったノードが、n_dash と同じで、変わらなかった場合
            // 同じを経路表を用いて探索することになり、結果は同じになり無限ループと
            // なってしまうため、探索は継続せず、探索結果として n_dash (= n_dash_found) を返す
            // TODO: x direct access to node_info of n_dash at find_predecessor
            chord_util::dprint(&("find_predecessor_3,".to_string() + chord_util::gen_debug_str_of_node(Some(exnode_ni_ref)).as_str() + ","
                                + chord_util::gen_debug_str_of_node(Some(n_dash_ninfo)).as_str()));
            return Arc::clone(&n_dash_found);
        }

        // closelst_preceding_finger は id を通り越してしまったノードは返さない
        // という前提の元で以下のチェックを行う
        // TODO: x direct access to node_info of n_dash at find_predecessor
        let distance_old = chord_util::calc_distance_between_nodes_right_mawari(exnode_ni_ref.node_id, n_dash_ninfo.node_id);
        // TODO: x direct access to node_info of n_dash_found at find_predecessor
        let distance_found = chord_util::calc_distance_between_nodes_right_mawari(exnode_ni_ref.node_id, n_dash_found_ni_ref.node_id);
        let distance_data_id = chord_util::calc_distance_between_nodes_right_mawari(exnode_ni_ref.node_id, id);
        if distance_found < distance_old && !(distance_old >= distance_data_id) {
            // 探索を続けていくと n_dash は id に近付いていくはずであり、それは上記の前提を踏まえると
            // 自ノードからはより遠い位置の値になっていくということのはずである
            // 従って、そうなっていなかった場合は、繰り返しを継続しても意味が無く、最悪、無限ループになってしまう
            // 可能性があるため、探索を打ち切り、探索結果は古いn_dashを返す.
            // ただし、古い n_dash が 一回目の探索の場合 self であり、同じ node_idの距離は ID_SPACE_RANGE となるようにしている
            // ため、上記の条件が常に成り立ってしまう. 従って、その場合は例外とする（n_dashが更新される場合は、更新されたn_dashのnode_idが
            // 探索対象のデータのid を通り越すことは無い）

            // TODO: x direct access to node_info of n_dash at find_predecessor
            chord_util::dprint(&("find_predecessor_4,".to_string() + chord_util::gen_debug_str_of_node(Some(exnode_ni_ref)).as_str() + ","
                                + chord_util::gen_debug_str_of_node(Some(n_dash_ninfo)).as_str()));

            return Arc::clone(&n_dash);
        }

        // TODO: x direct access to node_info of n_dash and n_dash_found at find_predecessor
        chord_util::dprint(&("find_predecessor_5_n_dash_updated,".to_string() + chord_util::gen_debug_str_of_node(Some(exnode_ni_ref)).as_str() + ","
                            + chord_util::gen_debug_str_of_node(Some(n_dash_ninfo)).as_str() + "->"
                            + chord_util::gen_debug_str_of_node(Some(n_dash_found_ni_ref)).as_str()));

        // チェックの結果問題ないので n_dashを closest_preceding_fingerで探索して得た
        // ノード情報 n_dash_foundに置き換える

        let n_dash = Arc::clone(&n_dash_found);
    }

    return Arc::clone(&n_dash);
}


/*
    # id(int)　の前で一番近い位置に存在するノードを探索する
    def find_predecessor(self, id: int) -> 'ChordNode':
        ChordUtil.dprint("find_predecessor_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info))

        n_dash : 'ChordNode' = self.existing_node

        if self.existing_node.node_info.lock_of_succ_infos.acquire(timeout=gval.LOCK_ACQUIRE_TIMEOUT) == False:
            # 最初の n_dash を返してしまい、find_predecessorは失敗したと判断させる
            ChordUtil.dprint("find_predecessor_1_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + "LOCK_ACQUIRE_TIMEOUT")
            return n_dash
        try:
            # n_dash と n_dashのsuccessorの 間に id が位置するような n_dash を見つけたら、ループを終了し n_dash を return する
            # TODO: direct access to node_id and successor_info_list of n_dash at find_predecessor
            while not ChordUtil.exist_between_two_nodes_right_mawari(n_dash.node_info.node_id, n_dash.node_info.successor_info_list[0].node_id, id):
                # TODO: x direct access to node_info of n_dash at find_predecessor
                ChordUtil.dprint("find_predecessor_2," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(n_dash.node_info))
                # TODO: closest_preceding_finger call at find_predecessor
                n_dash_found = n_dash.endpoints.grpc__closest_preceding_finger(id)

                # TODO: x direct access to node_info of n_dash_found and n_dash at find_predecessor
                if n_dash_found.node_info.node_id == n_dash.node_info.node_id:
                    # 見つかったノードが、n_dash と同じで、変わらなかった場合
                    # 同じを経路表を用いて探索することになり、結果は同じになり無限ループと
                    # なってしまうため、探索は継続せず、探索結果として n_dash (= n_dash_found) を返す
                    # TODO: x direct access to node_info of n_dash at find_predecessor
                    ChordUtil.dprint("find_predecessor_3," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                     + ChordUtil.gen_debug_str_of_node(n_dash.node_info))
                    return n_dash_found

                # closelst_preceding_finger は id を通り越してしまったノードは返さない
                # という前提の元で以下のチェックを行う
                # TODO: x direct access to node_info of n_dash at find_predecessor
                distance_old = ChordUtil.calc_distance_between_nodes_right_mawari(self.existing_node.node_info.node_id, n_dash.node_info.node_id)
                # TODO: x direct access to node_info of n_dash_found at find_predecessor
                distance_found = ChordUtil.calc_distance_between_nodes_right_mawari(self.existing_node.node_info.node_id, n_dash_found.node_info.node_id)
                distance_data_id = ChordUtil.calc_distance_between_nodes_right_mawari(self.existing_node.node_info.node_id, id)
                if distance_found < distance_old and not (distance_old >= distance_data_id):
                    # 探索を続けていくと n_dash は id に近付いていくはずであり、それは上記の前提を踏まえると
                    # 自ノードからはより遠い位置の値になっていくということのはずである
                    # 従って、そうなっていなかった場合は、繰り返しを継続しても意味が無く、最悪、無限ループになってしまう
                    # 可能性があるため、探索を打ち切り、探索結果は古いn_dashを返す.
                    # ただし、古い n_dash が 一回目の探索の場合 self であり、同じ node_idの距離は ID_SPACE_RANGE となるようにしている
                    # ため、上記の条件が常に成り立ってしまう. 従って、その場合は例外とする（n_dashが更新される場合は、更新されたn_dashのnode_idが
                    # 探索対象のデータのid を通り越すことは無い）

                    # TODO: x direct access to node_info of n_dash at find_predecessor
                    ChordUtil.dprint("find_predecessor_4," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                     + ChordUtil.gen_debug_str_of_node(n_dash.node_info))

                    return n_dash

                # TODO: x direct access to node_info of n_dash and n_dash_found at find_predecessor
                ChordUtil.dprint("find_predecessor_5_n_dash_updated," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(n_dash.node_info) + "->"
                                 + ChordUtil.gen_debug_str_of_node(n_dash_found.node_info))

                # チェックの結果問題ないので n_dashを closest_preceding_fingerで探索して得た
                # ノード情報 n_dash_foundに置き換える
                n_dash = n_dash_found
        finally:
            self.existing_node.node_info.lock_of_succ_infos.release()

        return n_dash
*/

//  自身の持つ経路情報をもとに,  id から前方向に一番近いノードの情報を返す
// ni_ref -> existing_nodeのもの
pub fn closest_preceding_finger(existing_node: ArRmRs<chord_node::ChordNode>, exnode_ni_ref: &Ref<node_info::NodeInfo>, id : i32) -> ArRmRs<chord_node::ChordNode> {        
    // 範囲の広いエントリから探索していく
    // finger_tableはインデックスが小さい方から大きい方に、範囲が大きくなっていく
    // ように構成されているため、リバースしてインデックスの大きな方から小さい方へ
    // 順に見ていくようにする
    
    // let exnode_refcell = get_refcell_from_arc_with_locking!(existing_node);
    // let exnode_ref = get_ref_from_refcell!(exnode_refcell);
    // let exnode_ni_refcell = get_refcell_from_arc_with_locking!(exnode_ref.node_info);
    // let exnode_ni_ref = get_ref_from_refcell!(exnode_ni_refcell);

    for node_info in exnode_ni_ref.finger_table.iter().rev() {
        // 注: Noneなエントリも存在し得る
        let conved_node_info = match node_info {
            None => {
                chord_util::dprint(&("closest_preceding_finger_0,".to_string() + chord_util::gen_debug_str_of_node(Some(exnode_ni_ref)).as_str()));
                continue;
            },
            Some(ni) => ni
        };

        chord_util::dprint(&("closest_preceding_finger_1,".to_string() + chord_util::gen_debug_str_of_node(Some(exnode_ni_ref)).as_str() + ","
            + chord_util::gen_debug_str_of_node(Some(&conved_node_info)).as_str()));

        // テーブル内のエントリが保持しているノードのIDが7自身のIDと探索対象のIDの間にあれば
        // それを返す
        // (大きな範囲を見た場合、探索対象のIDが自身のIDとエントリが保持しているノードのIDの中に含まれて
        //  しまっている可能性が高く、エントリが保持しているノードが、探索対象のIDを飛び越してしまっている
        //  可能性が高いということになる。そこで探索範囲を狭めていって、飛び越さない範囲で一番近いノードを
        //  見つけるという処理になっていると思われる）
        if chord_util::exist_between_two_nodes_right_mawari(exnode_ni_ref.node_id, id, conved_node_info.node_id) {

            chord_util::dprint(&("closest_preceding_finger_2,".to_string() + chord_util::gen_debug_str_of_node(Some(exnode_ni_ref)).as_str() + ","
                            + chord_util::gen_debug_str_of_node(Some(&conved_node_info)).as_str()));

            let gnba_rslt = chord_util::get_node_by_address(&conved_node_info.address_str);

            match gnba_rslt {
                Ok(node_opt) => { return Arc::clone(&node_opt);},
                Err(_err) => {
                    // err.err_code == chord_util::ERR_CODE_INTERNAL_CONTROL_FLOW_PROBLEM || err.err_code == chord_util::ERR_CODE_NODE_IS_DOWNED
                    // ここでは何も対処しない
                    continue;
                }
            };
        }
    }

    chord_util::dprint(&"closest_preceding_finger_3".to_string());

    // どんなに範囲を狭めても探索対象のIDを超えてしまうノードしか存在しなかった場合
    // 自身の知っている情報の中で対象を飛び越さない範囲で一番近いノードは自身という
    // ことになる
    return Arc::clone(&existing_node);
}


/*
    #  自身の持つ経路情報をもとに,  id から前方向に一番近いノードの情報を返す
    def closest_preceding_finger(self, id : int) -> 'ChordNode':
        # 範囲の広いエントリから探索していく
        # finger_tableはインデックスが小さい方から大きい方に、範囲が大きくなっていく
        # ように構成されているため、リバースしてインデックスの大きな方から小さい方へ
        # 順に見ていくようにする
        for node_info in reversed(self.existing_node.node_info.finger_table):
            # 埋まっていないエントリも存在し得る
            if node_info == None:
                ChordUtil.dprint("closest_preceding_finger_0," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info))
                continue

            casted_node_info = cast('NodeInfo', node_info)

            ChordUtil.dprint("closest_preceding_finger_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                  + ChordUtil.gen_debug_str_of_node(casted_node_info))

            # テーブル内のエントリが保持しているノードのIDが自身のIDと探索対象のIDの間にあれば
            # それを返す
            # (大きな範囲を見た場合、探索対象のIDが自身のIDとエントリが保持しているノードのIDの中に含まれて
            #  しまっている可能性が高く、エントリが保持しているノードが、探索対象のIDを飛び越してしまっている
            #  可能性が高いということになる。そこで探索範囲を狭めていって、飛び越さない範囲で一番近いノードを
            #  見つけるという処理になっていると思われる）
            # #if self.existing_node.node_info.node_id < entry.node_id and entry.node_id <= id:
            if ChordUtil.exist_between_two_nodes_right_mawari(self.existing_node.node_info.node_id, id, casted_node_info.node_id):
                ChordUtil.dprint("closest_preceding_finger_2," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(casted_node_info))

                                 ret = ChordUtil.get_node_by_address(casted_node_info.address_str)
                if (ret.is_ok):
                    casted_node : 'ChordNode' = cast('ChordNode', ret.result)
                    return casted_node
                else:  # ret.err_code == ErrorCode.InternalControlFlowException_CODE || ret.err_code == ErrorCode.NodeIsDownedException_CODE
                    # ここでは何も対処しない
                    continue

        ChordUtil.dprint("closest_preceding_finger_3")

        # どんなに範囲を狭めても探索対象のIDを超えてしまうノードしか存在しなかった場合
        # 自身の知っている情報の中で対象を飛び越さない範囲で一番近いノードは自身という
        # ことになる
        return self.existing_node
*/
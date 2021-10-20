/*
class ChordNode:
    def global_put(self, data_id : int, value_str : str) -> bool:
        ret = self.router.find_successor(data_id)
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

        success = target_node.endpoints.grpc__put(data_id, value_str)
        if not success:
            ChordNode.need_put_retry_data_id = data_id
            ChordNode.need_put_retry_node = self
            ChordUtil.dprint("global_put_2,RETRY_IS_NEEDED" + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                             + ChordUtil.gen_debug_str_of_data(data_id))
            return False

        # TODO: x direct access to node_info of target_node at global_put
        ChordUtil.dprint("global_put_3," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(target_node.node_info) + ","
                         + ChordUtil.gen_debug_str_of_data(data_id))

        return True

    def put(self, data_id : int, value_str : str) -> bool:
        ChordUtil.dprint("put_0," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_data(data_id))

        if self.is_alive == False:
            # 処理の合間でkillされてしまっていた場合の考慮
            # 何もしないで終了する
            ChordUtil.dprint("put_0_5," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                             + "REQUEST_RECEIVED_BUT_I_AM_ALREADY_DEAD")
            return False

        # 担当範囲（predecessorのidと自身のidの間）のデータであるかのチェック処理を加える
        # そこに収まっていなかった場合、一定時間後リトライが行われるようエラーを返す.
        # リクエストを受けるという実装も可能だが、stabilize処理で predecessor が生きて
        # いるノードとなるまで下手にデータを持たない方が、データ配置の整合性を壊すリスクが
        # 減りそうな気がするので、そうする
        if self.node_info.predecessor_info == None:
            return False
        # Chordネットワークを右回りにたどった時に、データの id (data_id) が predecessor の node_id から
        # 自身の node_id の間に位置する場合、そのデータは自身の担当だが、そうではない場合
        if not ChordUtil.exist_between_two_nodes_right_mawari(cast(NodeInfo,self.node_info.predecessor_info).node_id, self.node_info.node_id, data_id):
            return False

        if self.node_info.lock_of_succ_infos.acquire(timeout=gval.LOCK_ACQUIRE_TIMEOUT) == False:
            # 今回は失敗としてしまう
            ChordUtil.dprint("put_1," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                             + "LOCK_ACQUIRE_TIMEOUT")
            return False
        try:
            with self.node_info.lock_of_datastore:
                self.data_store.store_new_data(data_id, value_str)
                self.data_store.distribute_replica()
        finally:
            self.node_info.lock_of_succ_infos.release()

        ChordUtil.dprint("put_4," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_data(data_id) + "," + value_str)

        return True

    # global_getで取得しようとしたKeyが探索したノードに存在なかった場合に、当該ノードから
    # predecessorを辿ってリカバリを試みる処理をくくり出したもの
    def global_get_recover_prev(self, data_id : int) -> Tuple[str, Optional['ChordNode']]:
        if self.node_info.lock_of_pred_info.acquire(timeout=gval.LOCK_ACQUIRE_TIMEOUT) == False:
            ChordUtil.dprint("global_get_recover_prev_0," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                             + "LOCK_ACQUIRE_TIMEOUT")
            return ChordNode.QUERIED_DATA_NOT_FOUND_STR, None
        try:
            if self.node_info.predecessor_info == None:
                ChordUtil.dprint("global_get_recover_prev_1,predecessor is None")
                return ChordNode.QUERIED_DATA_NOT_FOUND_STR, None
            ret = ChordUtil.get_node_by_address(cast(NodeInfo, self.node_info.predecessor_info).address_str)
            if (ret.is_ok):
                cur_predecessor : 'ChordNode' = cast('ChordNode', ret.result)
                got_value_str = cur_predecessor.endpoints.grpc__get(data_id, for_recovery=True)
            else:  # ret.is_ok == False
                if cast(int,ret.err_code) == ErrorCode.NodeIsDownedException_CODE:
                    # ここでは何も対処はしない
                    ChordUtil.dprint("global_get_recover_prev_2,NODE_IS_DOWNED")
                    return ChordNode.QUERIED_DATA_NOT_FOUND_STR, None
                else: #cast(int,ret.err_code) == ErrorCode.InternalControlFlowException_CODE
                    # join処理中のノードにアクセスしようとしてしまった場合に内部的にraiseされる例外
                    ChordUtil.dprint("global_get_recover_prev_3,TARGET_NODE_DOES_NOT_EXIST_EXCEPTION_IS_OCCURED")
                    return ChordNode.QUERIED_DATA_NOT_FOUND_STR, None

            ChordUtil.dprint("global_get_recover_prev_4," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                             + ChordUtil.gen_debug_str_of_data(data_id))
            if got_value_str != ChordNode.QUERIED_DATA_NOT_FOUND_STR:
                # データが円環上でIDが小さくなっていく方向（反時計時計回りの方向）を前方とした場合に
                # 前方に位置するpredecessorを辿ることでデータを取得することができた
                # TODO: x direct access to node_info of cur_predecessor at global_get
                ChordUtil.dprint("global_get_recover_prev_5,"
                                 + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                                 + "data found at predecessor,"
                                 + ChordUtil.gen_debug_str_of_node(cur_predecessor.node_info))
                return got_value_str, cur_predecessor
            else:
                # できなかった
                # TODO: x direct access to node_info of cur_predecessor at global_get
                ChordUtil.dprint("global_get_recover_prev_6,"
                                 + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                                 + "data not found at predecessor,"
                                 + ChordUtil.gen_debug_str_of_node(cur_predecessor.node_info))
                return ChordNode.QUERIED_DATA_NOT_FOUND_STR, cur_predecessor
        finally:
            self.node_info.lock_of_pred_info.release()

        # 他の例外の発生ででここに到達した
        return ChordNode.QUERIED_DATA_NOT_FOUND_STR, None

    # 得られた value の文字列を返す
    # データの取得に失敗した場合は ChordNode.QUERIED_DATA_NOT_FOUND_STR を返す
    # 取得対象のデータが削除済みのデータであった場合は DataStore.DELETED_ENTRY_MARKING_STR を返す
    # TODO: 現状の実装では、データの取得に失敗した場合、そのエントリが過去にputされていないためなのか、システム側の都合による
    #       ものなのかは区別がつかない.
    #       実システムでは一定回数リトライを行い、それでもダメな場合は ChordNode.QUERIED_DATA_NOT_FOUND_STR を返すという
    #       形にしなければならない at global_get
    def global_get(self, data_id : int) -> str:
        ChordUtil.dprint("global_get_0," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_data(data_id))

                         ret = self.router.find_successor(data_id)
        if (ret.is_ok):
            target_node: 'ChordNode' = cast('ChordNode', ret.result)
            got_value_str = target_node.endpoints.grpc__get(data_id)
        else:
            # ret.err_code == ErrorCode.AppropriateNodeNotFoundException_CODE || ret.err_code == ErrorCode.InternalControlFlowException_CODE
            # || ret.err_code == ErrorCode.NodeIsDownedException_CODE

            # 適切なノードを得ることができなかった、もしくは、内部エラーが発生した

            # リトライに必要な情報をクラス変数に設定しておく
            ChordNode.need_getting_retry_data_id = data_id
            ChordNode.need_getting_retry_node = self

            ChordUtil.dprint("global_get_0_1,FIND_NODE_FAILED," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                             + ChordUtil.gen_debug_str_of_data(data_id))
            # 処理を終える
            return ChordNode.OP_FAIL_DUE_TO_FIND_NODE_FAIL_STR

        is_data_got_on_recovery = False
        # 返ってきた値が ChordNode.QUERIED_DATA_NOT_FOUND_STR だった場合、target_nodeから
        # 一定数の predecessorを辿ってそれぞれにも data_id に対応するデータを持っていないか問い合わせるようにする
        if got_value_str == ChordNode.QUERIED_DATA_NOT_FOUND_STR:
            tried_node_num = 0
            # 最初は処理の都合上、最初にgetをかけたノードを設定する
            cur_predecessor : 'ChordNode' = target_node
            while tried_node_num < ChordNode.GLOBAL_GET_NEAR_NODES_TRY_MAX_NODES:
                ChordUtil.dprint("global_get_1," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_data(data_id) + ","
                                 + got_value_str + "," + str(tried_node_num))

                got_value_str, tmp_cur_predecessor =  cur_predecessor.endpoints.grpc__global_get_recover_prev(data_id)
                if got_value_str != ChordNode.QUERIED_DATA_NOT_FOUND_STR:
                    is_data_got_on_recovery = True
                    break
                else:
                    tried_node_num += 1

                if tmp_cur_predecessor != None:
                    cur_predecessor = cast('ChordNode', tmp_cur_predecessor)

        # 返ってきた値が ChordNode.QUERIED_DATA_NOT_FOUND_STR だった場合、target_nodeから
        # 一定数の successor を辿ってそれぞれにも data_id に対応するデータを持っていないか問い合わせるようにする
        if got_value_str == ChordNode.QUERIED_DATA_NOT_FOUND_STR:
            tried_node_num = 0
            # 最初は処理の都合上、最初にgetをかけたノードを設定する
            cur_successor = target_node
            while tried_node_num < ChordNode.GLOBAL_GET_NEAR_NODES_TRY_MAX_NODES:
                ChordUtil.dprint("global_get_2," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_data(data_id) + ","
                                 + got_value_str + "," + str(tried_node_num))

                got_value_str, tmp_cur_successor =  cur_successor.endpoints.grpc__global_get_recover_succ(data_id)
                if got_value_str != ChordNode.QUERIED_DATA_NOT_FOUND_STR:
                    is_data_got_on_recovery = True
                    break
                else:
                    tried_node_num += 1

                if tmp_cur_successor != None:
                    cur_successor = cast('ChordNode', tmp_cur_successor)

        # リトライを試みたであろう時の処理
        if ChordNode.need_getting_retry_data_id != -1:
            if got_value_str != ChordNode.QUERIED_DATA_NOT_FOUND_STR:
                # リトライに成功した
                ChordUtil.dprint("global_get_2_6,retry of global_get is succeeded")
                # リトライは不要なためクリア
                ChordNode.need_getting_retry_data_id = -1
                ChordNode.need_getting_retry_node = None
            else:
                # リトライに失敗した（何もしない）
                ChordUtil.dprint("global_get_2_7,retry of global_get is failed")
                pass

        # 取得に失敗した場合はリトライに必要な情報をクラス変数に設定しておく
        if got_value_str == ChordNode.QUERIED_DATA_NOT_FOUND_STR:
            ChordNode.need_getting_retry_data_id = data_id
            ChordNode.need_getting_retry_node = self

        if is_data_got_on_recovery == True:
            # リカバリ処理でデータを取得した場合は自身のデータストアにもその値を保持しておく
            self.data_store.store_new_data(data_id, got_value_str)

        # TODO: x direct access to node_info of target_node at global_get
        ChordUtil.dprint("global_get_3," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
              + ChordUtil.gen_debug_str_of_node(target_node.node_info) + ","
              + ChordUtil.gen_debug_str_of_data(data_id) + "," + got_value_str)
        return got_value_str

    # 得られた value の文字列を返す
    def get(self, data_id : int, for_recovery = False) -> str:
        if self.is_alive == False:
            # 処理の合間でkillされてしまっていた場合の考慮
            # 何もしないで終了する
            ChordUtil.dprint("get_0," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                             + "REQUEST_RECEIVED_BUT_I_AM_ALREADY_DEAD")
            return ChordNode.OP_FAIL_DUE_TO_FIND_NODE_FAIL_STR

        if self.node_info.predecessor_info == None:
            # まだpredecessorが設定されれていなかった場合の考慮
            ChordUtil.dprint("get_0_5," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                             + "REQUEST_RECEIVED_BUT_I_CAN_NOT_KNOW_TANTOU_RANGE")
            return ChordNode.QUERIED_DATA_NOT_FOUND_STR

        ret = self.data_store.get(data_id)
        if (ret.is_ok):
            di_entry: DataIdAndValue = cast(DataIdAndValue, ret.result)
        else:  # ret.err_code == ErrorCode.KeyError_CODE
            err_str = ChordNode.QUERIED_DATA_NOT_FOUND_STR
            ChordUtil.dprint("get_1," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                             + ChordUtil.gen_debug_str_of_data(data_id) + "," + err_str)
            return err_str

        # Chordネットワークを右回りにたどった時に、データの id (data_id) がpredecessorの node_id から
        # 自身の node_id の間に位置した.
        # つまり、自身の担当ID範囲であった
        if ChordUtil.exist_between_two_nodes_right_mawari(cast('NodeInfo', self.node_info.predecessor_info).node_id,
                                                          self.node_info.node_id,
                                                          data_id) or for_recovery == True:
            # 担当ノード（マスター）のデータであったか、担当ノードとしてgetを受け付けたがデータを持っていなかったために
            # 周囲のノードに当該データを持っていないか問い合わせる処理を行っていた場合
            ret_value_str = di_entry.value_data
            ChordUtil.dprint("get_2," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                             + ChordUtil.gen_debug_str_of_data(data_id) + "," + ret_value_str)
        else:
            # 自身の担当範囲のIDのデータでは無かった
            # 該当IDのデータを保持していたとしてもレプリカであるので返さずにエラー文字列を返す
            ret_value_str = self.QUERIED_DATA_NOT_FOUND_STR

            ChordUtil.dprint("get_3," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                             + ChordUtil.gen_debug_str_of_data(data_id) + "," + ret_value_str)

        ChordUtil.dprint("get_4," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_data(data_id) + "," + ret_value_str)

        return ret_value_str

    # 指定されたデータが存在した場合は true を返し、そうでない場合は false を返す
    # TODO: global_getとglobal_putを呼び出しているがそれぞれで発見したノードが異なった場合
    #       を考慮すると、もう少し手のこんだ実装を行わなければならないかもしれない.
    #       また、global_getを何度かリトライすることでデータが見つかる場合が存在することを考える
    #       と、global_getもしくはglobal_putをある程度の回数呼び出す必要があるかもしれないが、
    #       当然、このRPCのレスポンスタイムは大きく落ちるためどうすべきか悩ましい.
    def global_delete(self, data_id : int) -> bool:
        cur_val = self.global_get(data_id)
        self.global_put(data_id, DataStore.DELETED_ENTRY_MARKING_STR)
        return not (cur_val == ChordNode.QUERIED_DATA_NOT_FOUND_STR
                    or cur_val == DataStore.DELETED_ENTRY_MARKING_STR)

    def pass_node_info(self) -> 'NodeInfo':
        return self.node_info.get_partial_deepcopy()

    # TODO: 実システムでのみ利用される. 他ノードのChordNodeオブジェクトはデフォルトで
    #       successor_info_listが空リストとなっているので、その内容をrpc呼び出しを
    #       行って取得したデータで埋める
    def fill_succ_info_list(self):
        if self.node_info.lock_of_succ_infos.acquire(timeout=gval.LOCK_ACQUIRE_TIMEOUT) == False:
            ChordUtil.dprint("fill_succ_info_list_1," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                             + "LOCK_ACQUIRE_TIMEOUT")
        try:
            self.node_info.successor_info_list = self.endpoints.grpc__pass_successor_list()
        finally:
            self.node_info.lock_of_succ_infos.release()
*/

use std::sync::atomic::{AtomicIsize, AtomicBool};
use std::sync::{Arc, Mutex};
use std::cell::{RefMut, RefCell, Ref};
use std::borrow::Borrow;
use std::sync::atomic::Ordering;

use parking_lot::{ReentrantMutex, const_reentrant_mutex};

use crate::gval;
use crate::node_info;
use crate::chord_util;
use crate::stabilizer;
use crate::router;
use crate::data_store;
use crate::endpoints;

//type ArRmRs<T> = Arc<ReentrantMutex<RefCell<T>>>;
type ArMu<T> = Arc<Mutex<T>>;

pub const QUERIED_DATA_NOT_FOUND_STR : &str = "QUERIED_DATA_WAS_NOT_FOUND";
pub const OP_FAIL_DUE_TO_FIND_NODE_FAIL_STR : &str = "OPERATION_FAILED_DUE_TO_FINDING_NODE_FAIL";

// global_get内で探索した担当ノードにgetをかけて、データを持っていないと
// レスポンスがあった際に、持っていないか辿っていくノードの一方向における上限数
pub const GLOBAL_GET_NEAR_NODES_TRY_MAX_NODES : i32 = 5;

/*
// global_getでの取得が NOT_FOUNDになった場合はこのクラス変数に格納して次のget処理の際にリトライさせる
// なお、本シミュレータの設計上、このフィールドは一つのデータだけ保持できれば良い
pub static mut need_getting_retry_data_id : AtomicIsize = AtomicIsize::new(-1);

lazy_static! {
    pub static ref need_getting_retry_node : ArRmRs<Option<ChordNode>> 
        = Arc::new(const_reentrant_mutex(RefCell::new(None)));
}

// global_put が router.find_successorでの例外発生で失敗した場合にこのクラス変数に格納して次のput処理の際にリトライさせる
// なお、本シミュレータの設計上、このフィールドは一つのデータだけ保持できれば良い
pub static mut need_put_retry_data_id : AtomicIsize = AtomicIsize::new(-1);

lazy_static! {
    pub static ref need_put_retry_data_value : ArRmRs<String> 
        = Arc::new(const_reentrant_mutex(RefCell::new("".to_string())));
}

lazy_static! {
    pub static ref need_put_retry_node : ArRmRs<Option<ChordNode>> 
        = Arc::new(const_reentrant_mutex(RefCell::new(None)));
}
*/


/*
//シミュレータの神々が利用するのはコンストラクタではなくこちらのファクトリメソッド
pub fn new_and_join(tyukai_node_address: String, first_node: bool) -> ArRmRs<ChordNode> {
    let new_node = ArRmRs_new!(ChordNode::new());
    {
        unsafe{
            gval::already_born_node_num.fetch_add(1, Ordering::Relaxed);
        }

        let new_node_refcell = get_refcell_from_arc_with_locking!(new_node);
        let new_node_refmut = get_ref_from_refcell!(new_node_refcell);
        let new_node_ni_refcell = get_refcell_from_arc_with_locking!(new_node_refmut.node_info);
        let new_node_ni_refmut = get_refmut_from_refcell!(new_node_ni_refcell);

        // ミリ秒精度のUNIXTIMEから自身のアドレスにあたる文字列と、Chordネットワーク上でのIDを決定する
        new_node_ni_refmut.address_str = chord_util::gen_address_str();
        new_node_ni_refmut.node_id = chord_util::hash_str_to_int(&new_node_ni_refmut.address_str);

        unsafe{
            new_node_ni_refmut.born_id = gval::already_born_node_num.load(Ordering::Relaxed) as i32;
        }

        // シミュレーション時のみ必要なフィールド（実システムでは不要）
        new_node_refmut.is_alive.store(true, Ordering::Relaxed);
        // TODO: (rust) 実際にKVSとしてデータのput、getを行うようになったらこのフラグは
        //              falseで初期化し、適切な箇所でtrueに変更すること
        new_node_refmut.is_join_op_finished.store(true, Ordering::Relaxed);

        let new_node_copied_for_succlist : node_info::NodeInfo;
        let new_node_copied_for_pred_info : node_info::NodeInfo;
        {
            //new_node_copied_for_succlist = node_info::get_partial_deepcopy(new_node_ni_ref);
            new_node_copied_for_succlist = (*new_node_ni_refmut).clone();
            //new_node_copied_for_pred_info = node_info::get_partial_deepcopy(new_node_ni_ref);
            new_node_copied_for_pred_info = (*new_node_ni_refmut).clone();
        }
        //let new_node_ni_refmut = get_refmut_from_refcell!(new_node_ni_refcell);

        if first_node {
            //with self.node_info.lock_of_pred_info, self.node_info.lock_of_succ_infos:

            // 最初の1ノードの場合

            // successorとpredecessorは自身として終了する
            //let succ_list_target_idx = new_node_ni_refmut.successor_info_list.len() - 1;
            new_node_ni_refmut.successor_info_list.push(new_node_copied_for_succlist);
            //new_node_ni_refmut.predecessor_info[0] = new_node_copied_for_pred_info;
            new_node_ni_refmut.set_pred_info(new_node_copied_for_pred_info);

            // 最初の1ノードなので、joinメソッド内で行われるsuccessor からの
            // データの委譲は必要ない

            return Arc::clone(&new_node);
        }
    }

    // first_node == false の場合
    stabilizer::join(Arc::clone(&new_node), &tyukai_node_address);

    return Arc::clone(&new_node);
}
*/

/*
# ミリ秒精度のUNIXTIMEから自身のアドレスにあたる文字列と、Chordネットワーク上でのIDを決定する
self.node_info.address_str = ChordUtil.gen_address_str()
self.node_info.node_id = ChordUtil.hash_str_to_int(self.node_info.address_str)

gval.already_born_node_num += 1
self.node_info.born_id = gval.already_born_node_num

# シミュレーション時のみ必要なフィールド（実システムでは不要）
self.is_alive = True

# join処理が完了していない状態で global_get, global_put, stablize処理, kill処理 がシミュレータの
# 大本から呼び出されないようにするためのフラグ
self.is_join_op_finished = False

if first_node:
    with self.node_info.lock_of_pred_info, self.node_info.lock_of_succ_infos:
        # 最初の1ノードの場合

        # successorとpredecessorは自身として終了する
        self.node_info.successor_info_list.append(self.node_info.get_partial_deepcopy())
        self.node_info.predecessor_info = self.node_info.get_partial_deepcopy()

        # 最初の1ノードなので、joinメソッド内で行われるsuccessor からの
        # データの委譲は必要ない

        return
else:
    self.stabilizer.join(node_address)
*/
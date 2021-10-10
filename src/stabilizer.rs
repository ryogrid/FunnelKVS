use std::sync::{Arc, Mutex};
use std::cell::{RefMut, RefCell, Ref};
use std::sync::atomic::Ordering;
use std::borrow::{Borrow, BorrowMut};

use parking_lot::{ReentrantMutex, const_reentrant_mutex};

use crate::gval;
use crate::chord_node;
use crate::node_info;
use crate::chord_util;
use crate::endpoints;
use crate::data_store;
use crate::router;

//type ArRmRs<T> = Arc<ReentrantMutex<RefCell<T>>>;
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

/*
# successor_info_listの長さをチェックし、規定長を越えていた場合余剰なノードにレプリカを
# 削除させた上で、リストから取り除く
# TODO: InternalExp at check_successor_list_length
def check_successor_list_length(self) -> PResult[bool]:
    if self.existing_node.node_info.lock_of_succ_infos.acquire(timeout=gval.LOCK_ACQUIRE_TIMEOUT) == False:
        ChordUtil.dprint("check_successor_list_length_0," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                         + "LOCK_ACQUIRE_TIMEOUT")
        return PResult.Err(False, ErrorCode.InternalControlFlowException_CODE)

    try:
        ChordUtil.dprint(
            "check_successor_list_length_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
            + str(len(self.existing_node.node_info.successor_info_list)))

        if len(self.existing_node.node_info.successor_info_list) > gval.SUCCESSOR_LIST_NORMAL_LEN:
            list_len = len(self.existing_node.node_info.successor_info_list)
            delete_elem_list : List['NodeInfo'] = []
            for idx in range(gval.SUCCESSOR_LIST_NORMAL_LEN, list_len):
                # successor_info_listからエントリが削除された場合、rangeで得られる数字列全てに要素がない
                # 状態が起こるため、最新のlengthでチェックし直す
                if idx >= len(self.existing_node.node_info.successor_info_list):
                    break

                ChordUtil.dprint(
                    "check_successor_list_length_2," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                    + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[idx])
                    + str(len(self.existing_node.node_info.successor_info_list)))

                delete_elem_list.append(self.existing_node.node_info.successor_info_list[idx])

            # 上のループで削除すると決まった要素を取り除く
            for elem in delete_elem_list:
                self.existing_node.node_info.successor_info_list.remove(elem)

        return PResult.Ok(True)
    finally:
        self.existing_node.node_info.lock_of_succ_infos.release()
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

/*
    # 経路表の情報を他ノードから強制的に設定する.
    # joinメソッドの中で、secondノードがfirstノードに対してのみ用いるものであり、他のケースで利用してはならない
    def set_routing_infos_force(self, predecessor_info : 'NodeInfo', successor_info_0 : 'NodeInfo', ftable_enry_0 : 'NodeInfo'):
        with self.existing_node.node_info.lock_of_pred_info, self.existing_node.node_info.lock_of_succ_infos:
            self.existing_node.node_info.predecessor_info = predecessor_info
            self.existing_node.node_info.successor_info_list[0] = successor_info_0
            self.existing_node.node_info.finger_table[0] = ftable_enry_0
*/

// TODO: (rustr) comment-outed at spiking RPC impl 
/*
// node_addressに対応するノードに問い合わせを行い、教えてもらったノードをsuccessorとして設定する
pub fn join(new_node: ArMu<node_info::NodeInfo>, tyukai_node_address: &String){
    //with self.existing_node.node_info.lock_of_pred_info, new_node_ni_refmut.lock_of_succ_infos:

    let mut is_second_node:bool = false;

    //println!("join {:?}", tyukai_node_address);
    // 実装上例外は発生しない.
    // また実システムでもダウンしているノードの情報が与えられることは想定しない
    let tyukai_node = chord_util::get_node_info_by_address(tyukai_node_address).unwrap();

    let successor: ArMu<node_info::NodeInfo>;


    // // new_nodeのNodeInfoオブジェクトのクリティカルセクションを開始する
    // let new_node_ni_lock;
    // let new_node_ni_lock_keeper;

    // // TODO: (rust) 実システムに照らして考えると、successorのロックをとっているのはおかしいのでどうにかする必要あり
    // // successorのNodeInfoオブジェクトのクリティカルセクションを開始する
    // let successor_ni_lock;
    // let successor_ni_lock_keeper;


    {
        let tyukai_node_refcell = get_refcell_from_arc_with_locking!(tyukai_node);
        let tyukai_node_ref = get_ref_from_refcell!(tyukai_node_refcell);
        let tyukai_node_ni_refcell = get_refcell_from_arc_with_locking!(tyukai_node_ref.node_info);
        let tyukai_node_ni_ref = get_ref_from_refcell!(tyukai_node_ni_refcell);        

        //new_node_refcell = get_refcell_from_arc_with_locking!(new_node);
        {
            let new_node_refcell = get_refcell_from_arc_with_locking!(new_node);
            let new_node_ref = get_ref_from_refcell!(new_node_refcell);
            let new_node_ni_refcell = get_refcell_from_arc_with_locking!(new_node_ref.node_info);
            let new_node_ni_ref = get_ref_from_refcell!(new_node_ni_refcell);


            // // new_nodeのNodeInfoオブジェクトのクリティカルセクションを開始する
            // new_node_ni_lock = chord_util::get_lock_obj("ninfo", &new_node_ni_ref.address_str);
            // new_node_ni_lock_keeper = get_refcell_from_arc_with_locking!(new_node_ni_lock);


            // TODO: (rust) x direct access to node_info of tyukai_node at join
            chord_util::dprint(&("join_1,".to_string() + chord_util::gen_debug_str_of_node(Some(new_node_ni_ref)).as_str() + ","
                                + chord_util::gen_debug_str_of_node(Some(tyukai_node_ni_ref)).as_str()));

            // 仲介ノードに自身のsuccessorになるべきノードを探してもらう

            // TODO: find_successor call at join
            successor = match endpoints::grpc__find_successor(
                Arc::clone(&tyukai_node), tyukai_node_ref, tyukai_node_ni_ref, new_node_ni_ref.node_id) {
                    Err(_e) => { // ret.err_code == ErrorCode.AppropriateNodeNotFoundException_CODE || ret.err_code == ErrorCode.InternalControlFlowException_CODE || ret.err_code == ErrorCode.NodeIsDownedException_CODE
                        // リトライに必要な情報を記録しておく
                        // TODO: (rust) リトライの対応は後回し
                        // need_join_retry_node = self.existing_node;
                        // need_join_retry_tyukai_node = tyukai_node;

                        // 自ノードの情報、仲介ノードの情報
                        // TODO: (rust) x direct access to node_info of tyukai_node at join
                        chord_util::dprint(
                            &("join_2,RETRY_IS_NEEDED,".to_string() + chord_util::gen_debug_str_of_node(Some(new_node_ni_ref)).as_str() + ","
                            + chord_util::gen_debug_str_of_node(Some(tyukai_node_ni_ref)).as_str()));
                        return
                    },
                    Ok(got_node) => {
                        //TODO: (rust) リトライ対応は後回し
                        //need_join_retry_node = None;
                        got_node
                    }
            }
            // grpc__find_successorの呼び出しのために &Ref<NodeInfo>が必要であったが
            // 後続の処理では mutable な参照が必要となるためここで無効化する
        }   

        // mutableな参照を借用し直す
        let new_node_refcell = get_refcell_from_arc_with_locking!(new_node);
        let new_node_refmut = get_refmut_from_refcell!(new_node_refcell);
        let new_node_ni_refcell = get_refcell_from_arc_with_locking!(new_node_refmut.node_info);
        let new_node_ni_refmut = get_refmut_from_refcell!(new_node_ni_refcell);        

        let successor_refcell = get_refcell_from_arc_with_locking!(successor);
        let successor_ref = get_ref_from_refcell!(successor_refcell);
        let successor_ni_refcell = get_refcell_from_arc_with_locking!(successor_ref.node_info);
        let successor_ni_ref = get_ref_from_refcell!(successor_ni_refcell);


        // // successorのNodeInfoオブジェクトのクリティカルセクションを開始する
        // successor_ni_lock = chord_util::get_lock_obj("ninfo", &successor_ni_ref.address_str);
        // successor_ni_lock_keeper = get_refcell_from_arc_with_locking!(successor_ni_lock);


        // TODO: (rust) for debug
        if new_node_ni_refmut.node_id == successor_ni_ref.node_id {
            chord_util::dprint(&("join_2_5,".to_string() + chord_util::gen_debug_str_of_node(Some(new_node_ni_refmut)).as_str() + ","
                            + chord_util::gen_debug_str_of_node(Some(tyukai_node_ni_ref)).as_str() + ","
                            + chord_util::gen_debug_str_of_node(Some(&new_node_ni_refmut.successor_info_list[0])).as_str() + ",FOUND_NODE_IS_SAME_WITH_SELF_NODE!!!"));
        }

        // TODO: (rust) x direct access to node_info of predecessor at join
        if tyukai_node_ni_ref.node_id == tyukai_node_ni_ref.successor_info_list[0].node_id {
            // secondノードの場合の考慮 (仲介ノードは必ずfirst node)

            //predecessor = tyukai_node;

            // 2ノードでsuccessorでもpredecessorでも、チェーン構造で正しい環が構成されるよう強制的に全て設定してしまう
            // secondノードの場合の考慮 (仲介ノードは必ずfirst node)
            is_second_node = true;
            new_node_ni_refmut.successor_info_list.push((*tyukai_node_ni_ref).clone());
            new_node_ni_refmut.set_pred_info((*tyukai_node_ni_ref).clone());

            // mutableな参照が必要な都合により、後続のコードで残りの処理を行う
        }else{
            // TODO: (rust) x direct access to node_info of successor at join
            //let succ_infos_len = new_node_ni_refmut.successor_info_list.len();
            new_node_ni_refmut.successor_info_list.push((*successor_ni_ref).clone());
        }

        // finger_tableのインデックス0は必ずsuccessorになるはずなので、設定しておく
        new_node_ni_refmut.finger_table[0] = Some(new_node_ni_refmut.successor_info_list[0].clone());
    }

    {
        // mutableな参照を借用し直す
        let new_node_refcell = get_refcell_from_arc_with_locking!(new_node);
        let new_node_refmut = get_refmut_from_refcell!(new_node_refcell);
        let new_node_ni_refcell = get_refcell_from_arc_with_locking!(new_node_refmut.node_info);
        let new_node_ni_refmut = get_refmut_from_refcell!(new_node_ni_refcell);    

        if is_second_node {
            // secondノードの場合の考慮の続き
            endpoints::grpc__set_routing_infos_force(
                Arc::clone(&tyukai_node),
                (*new_node_ni_refmut).clone(),
                (*new_node_ni_refmut).clone(),
                (*new_node_ni_refmut).clone()
            );
        }

        let tyukai_node_refcell = get_refcell_from_arc_with_locking!(tyukai_node);
        let tyukai_node_ref = get_ref_from_refcell!(tyukai_node_refcell);
        let tyukai_node_ni_refcell = get_refcell_from_arc_with_locking!(tyukai_node_ref.node_info);
        let tyukai_node_ni_ref = get_ref_from_refcell!(tyukai_node_ni_refcell);
    }

    if is_second_node {
        let new_node_refcell = get_refcell_from_arc_with_locking!(new_node);
        let new_node_refmut = get_refmut_from_refcell!(new_node_refcell);
        let new_node_ni_refcell = get_refcell_from_arc_with_locking!(new_node_refmut.node_info);
        let new_node_ni_refmut = get_refmut_from_refcell!(new_node_ni_refcell);

        let tyukai_node_refcell = get_refcell_from_arc_with_locking!(tyukai_node);
        let tyukai_node_ref = get_ref_from_refcell!(tyukai_node_refcell);
        let tyukai_node_ni_refcell = get_refcell_from_arc_with_locking!(tyukai_node_ref.node_info);
        let tyukai_node_ni_ref = get_ref_from_refcell!(tyukai_node_ni_refcell);

        // secondノードの場合の考慮の続き
        chord_util::dprint(&("join_3,".to_string() + chord_util::gen_debug_str_of_node(Some(new_node_ni_refmut)).as_str() + ","
                            + chord_util::gen_debug_str_of_node(Some(tyukai_node_ni_ref)).as_str() + ","
                            + chord_util::gen_debug_str_of_node(Some(&new_node_ni_refmut.successor_info_list[0])).as_str()));
    } else {
        // successorと、successorノードの情報だけ適切なものとする
        // TODO: check_predecessor call at join

        let new_node_ni;
        {
            let new_node_refcell = get_refcell_from_arc_with_locking!(new_node);
            let new_node_refmut = get_refmut_from_refcell!(new_node_refcell);
            let new_node_ni_refcell = get_refcell_from_arc_with_locking!(new_node_refmut.node_info);
            let new_node_ni_ref = get_refmut_from_refcell!(new_node_ni_refcell);
            new_node_ni = (*new_node_ni_ref).clone();
        }

        match endpoints::grpc__check_predecessor(Arc::clone(&successor), new_node_ni){
            Err(_e) => {  // ret.err_code == ErrorCode.InternalControlFlowException_CODE
                // リトライに必要な情報を記録しておく
                // TODO: (rust) リトライの対応は後回し
                //need_join_retry_node = self.existing_node;
                //need_join_retry_tyukai_node = tyukai_node;

                let new_node_refcell = get_refcell_from_arc_with_locking!(new_node);
                let new_node_refmut = get_refmut_from_refcell!(new_node_refcell);
                let new_node_ni_refcell = get_refcell_from_arc_with_locking!(new_node_refmut.node_info);
                let new_node_ni_refmut = get_refmut_from_refcell!(new_node_ni_refcell);    
        
                let tyukai_node_refcell = get_refcell_from_arc_with_locking!(tyukai_node);
                let tyukai_node_ref = get_ref_from_refcell!(tyukai_node_refcell);
                let tyukai_node_ni_refcell = get_refcell_from_arc_with_locking!(tyukai_node_ref.node_info);
                let tyukai_node_ni_ref = get_ref_from_refcell!(tyukai_node_ni_refcell);
                
                // 既に値を設定してしまっている場合にリトライ時に問題が生じることを考慮し、
                // 内容をリセットしておく
                new_node_ni_refmut.successor_info_list = vec![];

                // 自ノードの情報、仲介ノードの情報
                // TODO: (rust) x direct access to node_info of tyukai_node at join
                chord_util::dprint(&("join_3,RETRY_IS_NEEDED,".to_string() + chord_util::gen_debug_str_of_node(
                    Some(new_node_ni_refmut)).as_str() + ","
                                    + chord_util::gen_debug_str_of_node(Some(tyukai_node_ni_ref)).as_str()));
                //chord_util::dprint(traceback.format_exc());
                return
            },
            Ok(_dummy_bool) => {
                let new_node_refcell = get_refcell_from_arc_with_locking!(new_node);
                let new_node_refmut = get_refmut_from_refcell!(new_node_refcell);
                let new_node_ni_refcell = get_refcell_from_arc_with_locking!(new_node_refmut.node_info);
                let new_node_ni_refmut = get_refmut_from_refcell!(new_node_ni_refcell);    
        
                let tyukai_node_refcell = get_refcell_from_arc_with_locking!(tyukai_node);
                let tyukai_node_ref = get_ref_from_refcell!(tyukai_node_refcell);
                let tyukai_node_ni_refcell = get_refcell_from_arc_with_locking!(tyukai_node_ref.node_info);
                let tyukai_node_ni_ref = get_ref_from_refcell!(tyukai_node_ni_refcell);
                
                chord_util::dprint(&("join_4,".to_string() + chord_util::gen_debug_str_of_node(Some(new_node_ni_refmut)).as_str() + ","
                + chord_util::gen_debug_str_of_node(Some(tyukai_node_ni_ref)).as_str() + ","
                + chord_util::gen_debug_str_of_node(Some(&new_node_ni_refmut.successor_info_list[0])).as_str()));                
            },
        }

        let new_node_refcell = get_refcell_from_arc_with_locking!(new_node);
        let new_node_refmut = get_refmut_from_refcell!(new_node_refcell);
        let new_node_ni_refcell = get_refcell_from_arc_with_locking!(new_node_refmut.node_info);
        let new_node_ni_refmut = get_refmut_from_refcell!(new_node_ni_refcell);    

        // successor_info_listを埋めておく
        // TODO: pass_successor_list call at join
        let succ_list_of_succ = endpoints::grpc__pass_successor_list(Arc::clone(&successor));
        let list_len = succ_list_of_succ.len() as i32;
        for idx in 0..(gval::SUCCESSOR_LIST_NORMAL_LEN - 1) {
            if idx < list_len {
                new_node_ni_refmut.successor_info_list.push(
                    succ_list_of_succ[idx as usize].clone());
            }
        }
    }

    // successorから自身が担当することになるID範囲のデータの委譲を受け、格納する

    // TODO: (rust) put, getのコードがまだ存在しないので、KVSで保持しているデータの委譲のコードは後回し
    // // TODO: delegate_my_tantou_data call at join
    // let tantou_data_list = endpoints::grpc__delegate_my_tantou_data(
    //     Arc::clone(&successor), new_node_ni_refmut.node_id);

    //with self.existing_node.node_info.lock_of_datastore:

    // TODO: (rust) ひとまず、putとgetの対応は後回しなので、ここの処理の対応も後回し
    // for key_value in tantou_data_list {
    //     new_node_refmut.data_store.store_new_data(key_value.data_id, key_value.value_data);
    // }
    // // 残りのレプリカに関する処理は stabilize処理のためのスレッドに別途実行させる
    // new_node_refmut.tqueue.append_task(TaskQueue.JOIN_PARTIAL);
    // unsafe{
    //     gval::is_waiting_partial_join_op_exists.store(true, Ordering::Relaxed);
    // }

    //chord_util::dprint_routing_info(self.existing_node, sys._getframe().f_code.co_name)
}
*/

/*
    # node_addressに対応するノードに問い合わせを行い、教えてもらったノードをsuccessorとして設定する
    def join(self, node_address : str):
        with self.existing_node.node_info.lock_of_pred_info, self.existing_node.node_info.lock_of_succ_infos:
            # 実装上例外は発生しない.
            # また実システムでもダウンしているノードの情報が与えられることは想定しない
            #tyukai_node = ChordUtil.get_node_by_address(node_address)
            tyukai_node = cast('ChordNode', ChordUtil.get_node_by_address(node_address).result)
            # TODO: x direct access to node_info of tyukai_node at join
            ChordUtil.dprint("join_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + ChordUtil.gen_debug_str_of_node(tyukai_node.node_info))

            # 仲介ノードに自身のsuccessorになるべきノードを探してもらう

            # TODO: find_successor call at join
            ret = tyukai_node.endpoints.grpc__find_successor(self.existing_node.node_info.node_id)
            if (ret.is_ok):
                successor : 'ChordNode' = cast('ChordNode', ret.result)
                # リトライは不要なので、本メソッドの呼び出し元がリトライ処理を行うかの判断に用いる
                # フィールドをリセットしておく
                Stabilizer.need_join_retry_node = None
            else:  # ret.err_code == ErrorCode.AppropriateNodeNotFoundException_CODE || ret.err_code == ErrorCode.InternalControlFlowException_CODE || ret.err_code == ErrorCode.NodeIsDownedException_CODE
                # リトライに必要な情報を記録しておく
                Stabilizer.need_join_retry_node = self.existing_node
                Stabilizer.need_join_retry_tyukai_node = tyukai_node

                # 自ノードの情報、仲介ノードの情報
                # TODO: x direct access to node_info of tyukai_node at join
                ChordUtil.dprint(
                    "join_2,RETRY_IS_NEEDED," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                    + ChordUtil.gen_debug_str_of_node(tyukai_node.node_info))
                return

            # TODO: x direct access to node_info of successor at join
            self.existing_node.node_info.successor_info_list.append(successor.node_info.get_partial_deepcopy())

            # finger_tableのインデックス0は必ずsuccessorになるはずなので、設定しておく
            self.existing_node.node_info.finger_table[0] = self.existing_node.node_info.successor_info_list[0].get_partial_deepcopy()

            # TODO: x direct access to node_info of tyukai_node at join
            if tyukai_node.node_info.node_id == tyukai_node.node_info.successor_info_list[0].node_id:
                # secondノードの場合の考慮 (仲介ノードは必ずfirst node)

                predecessor = tyukai_node

                # 2ノードでsuccessorでもpredecessorでも、チェーン構造で正しい環が構成されるよう強制的に全て設定してしまう
                # TODO: x direct access to node_info of predecessor at join
                self.existing_node.node_info.predecessor_info = predecessor.node_info.get_partial_deepcopy()

                tyukai_node.endpoints.grpc__set_routing_infos_force(
                    self.existing_node.node_info.get_partial_deepcopy(),
                    self.existing_node.node_info.get_partial_deepcopy(),
                    self.existing_node.node_info.get_partial_deepcopy()
                )

                # TODO: x direct access to node_info of tyukai_node at join
                ChordUtil.dprint("join_3," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(tyukai_node.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]))
            else:
                # successorと、successorノードの情報だけ適切なものとする
                # TODO: check_predecessor call at join
                ret2 = successor.endpoints.grpc__check_predecessor(self.existing_node.node_info)
                if (ret2.is_ok):
                    pass
                else:  # ret.err_code == ErrorCode.InternalControlFlowException_CODE
                    # リトライに必要な情報を記録しておく
                    Stabilizer.need_join_retry_node = self.existing_node
                    Stabilizer.need_join_retry_tyukai_node = tyukai_node

                    # 既に値を設定してしまっている場合を考慮し、内容をリセットしておく
                    self.existing_node.node_info.successor_info_list = []

                    # 自ノードの情報、仲介ノードの情報
                    # TODO: x direct access to node_info of tyukai_node at join
                    ChordUtil.dprint("join_3,RETRY_IS_NEEDED," + ChordUtil.gen_debug_str_of_node(
                        self.existing_node.node_info) + ","
                                     + ChordUtil.gen_debug_str_of_node(tyukai_node.node_info))
                    ChordUtil.dprint(traceback.format_exc())
                    return PResult.Err(False, cast(int, ret2.err_code))

                # successor_info_listを埋めておく
                # TODO: pass_successor_list call at join
                succ_list_of_succ: List[NodeInfo] = successor.endpoints.grpc__pass_successor_list()
                list_len = len(succ_list_of_succ)
                for idx in range(0, gval.SUCCESSOR_LIST_NORMAL_LEN - 1):
                    if idx < list_len:
                        self.existing_node.node_info.successor_info_list.append(
                            succ_list_of_succ[idx].get_partial_deepcopy())

            # successorから自身が担当することになるID範囲のデータの委譲を受け、格納する

            # TODO: delegate_my_tantou_data call at join
            tantou_data_list: List[KeyValue] = successor.endpoints.grpc__delegate_my_tantou_data(
                self.existing_node.node_info.node_id)

            with self.existing_node.node_info.lock_of_datastore:
                for key_value in tantou_data_list:
                    self.existing_node.data_store.store_new_data(cast(int, key_value.data_id), key_value.value_data)

            # 残りのレプリカに関する処理は stabilize処理のためのスレッドに別途実行させる
            self.existing_node.tqueue.append_task(TaskQueue.JOIN_PARTIAL)
            gval.is_waiting_partial_join_op_exists = True

            ChordUtil.dprint_routing_info(self.existing_node, sys._getframe().f_code.co_name)
*/

// TODO: (rust) need implement stabilize_successor_inner_fill_succ_list
fn stabilize_successor_inner_fill_succ_list(){
    // 故障ノードの発生が無い前提の段階では実装は不要、のはず
}

/*
# ロックは呼び出し元のstabilize_successor_innerでとってある前提
# TODO: DownedExp at stabilize_successor_inner_fill_succ_list
def stabilize_successor_inner_fill_succ_list(self) -> PResult[bool]:
    # 本メソッド呼び出しでsuccessorとして扱うノードはsuccessorListからダウンしているノードを取り除いた上で
    # successor_info_list[0]となったノードとする
    ChordUtil.dprint("stabilize_successor_inner_fill_succ_list_0," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info))

    if self.existing_node.is_alive == False:
        # 処理の合間でkillされてしまっていた場合の考慮
        # 何もしないで終了する
        ChordUtil.dprint(
            "stabilize_successor_innner_fill_succ_list_0," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
            + "REQUEST_RECEIVED_BUT_I_AM_ALREADY_DEAD")
        #raise NodeIsDownedExceptiopn()
        return PResult.Err(False, ErrorCode.NodeIsDownedException_CODE)

    successor_list_tmp: List['NodeInfo'] = []
    for idx in range(len(self.existing_node.node_info.successor_info_list)):
        # try:
            #if ChordUtil.is_node_alive(self.existing_node.node_info.successor_info_list[idx].address_str):
        ret = ChordUtil.is_node_alive(self.existing_node.node_info.successor_info_list[idx].address_str)
        if (ret.is_ok):
            if cast(bool, ret.result) == True:
                successor_list_tmp.append(self.existing_node.node_info.successor_info_list[idx])
            else: # == False
                ChordUtil.dprint("stabilize_successor_inner_fill_succ_list_1,SUCCESSOR_IS_DOWNED,"
                                 + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(
                    self.existing_node.node_info.successor_info_list[idx]))
        else:  # ret.err_code == ErrorCode.InternalControlFlowException_CODE
            # joinの中から呼び出された際に、successorを辿って行った結果、一周してjoin処理中のノードを get_node_by_addressしようと
            # した際に発生してしまうので、ここで対処する
            # join処理中のノードのpredecessor, sucessorはjoin処理の中で適切に設定されているはずなので、後続の処理を行わず successor[0]を返す
            # TODO: なお、join処理中のノードがsuccessor_info_listに入っていた場合も同様に例外がraiseされるケースがある
            continue

    if len(successor_list_tmp) == 0:
        # successorListの全てのノードを当たっても、生きているノードが存在しなかった場合
        # 起きてはいけない状況なので例外を投げてプログラムを終了させる
        ChordUtil.dprint("stabilize_successor_inner_fill_succ_list_3,,"
                         + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                         + str(len(self.existing_node.node_info.successor_info_list)) + ","
                         + str(self.existing_node.node_info.successor_info_list),
                         flush=True)
        raise Exception("Maybe some parameters related to fault-tolerance of Chord network are not appropriate")
    else:
        self.existing_node.node_info.successor_info_list = successor_list_tmp

    return PResult.Ok(True)
*/

// TODO: (rust) need implement stabilize_successor_inner_fix_chain func
pub fn stabilize_successor_inner_fix_chain(self_node: ArMu<node_info::NodeInfo>, successor: ArMu<node_info::NodeInfo>) -> Result<bool, chord_util::GeneralError> {
    //故障ノードが発生しない前提であれば、check_predecessorの呼び出しに置き換えて構わない、はず
    return Ok(true);
}

/*
# TODO: InternalExp, NodeIsDownedExp at stabilize_successor_inner_fix_chain
# ロックは呼び出し元のstabilize_successor_innerでとってある前提
def stabilize_successor_inner_fix_chain(self, successor : 'ChordNode') -> PResult[bool]:
    # TODO: direct access to predecessor_info of successor at stabilize_successor_inner_fix_chain

    # predecessor_infoが未設定の場合の考慮
    if successor.node_info.predecessor_info == None:
        pred_id_of_successor = -1
        # 以下の呼び出しにより、successorの predecessor_info には自ノードが設定される
        successor.endpoints.grpc__check_predecessor(self.existing_node.node_info)
    else:
        pred_id_of_successor = cast('NodeInfo', successor.node_info.predecessor_info).node_id

    ChordUtil.dprint(
        "stabilize_successor_inner_fix_chain_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
        + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]) + ","
        + str(pred_id_of_successor))

    # predecessor_infoが未設定であったが、grpc_check_predecessorによって設定された場合のため再度
    # pred_id_of_successorを取得する
    # なお、後続の処理を進める意味は無いが、ひとまずそのまま進める
    pred_id_of_successor = cast('NodeInfo', successor.node_info.predecessor_info).node_id

    if self.existing_node.is_alive == False:
        # 処理の合間でkillされてしまっていた場合の考慮
        # 何もしないで終了する
        ChordUtil.dprint(
            "stabilize_successor_innner_fix_chain_1_5," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
            + "REQUEST_RECEIVED_BUT_I_AM_ALREADY_DEAD")
        return PResult.Err(False, ErrorCode.NodeIsDownedException_CODE)

    # successorが認識している predecessor の情報をチェックさせて、適切なものに変更させたり、把握していない
    # 自身のsuccessor[0]になるべきノードの存在が判明した場合は 自身の successor[0] をそちらに張り替える.
    # なお、下のパターン1から3という記述は以下の資料による説明に基づく
    # https://www.slideshare.net/did2/chorddht
    if (pred_id_of_successor == self.existing_node.node_info.node_id):
        # パターン1
        # 特に訂正は不要
        ChordUtil.dprint(
            "stabilize_successor_inner_fix_chain_2," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
            + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]) + ","
            + str(pred_id_of_successor))
    else:
        # 以下、パターン2およびパターン3に対応する処理

        # 自身がsuccessorにとっての正しいpredecessorでないか確認を要請し必要であれば
        # 情報を更新してもらう
        # 注: successorが認識していた predecessorがダウンしていた場合、下の呼び出しにより後続でcheck_predecessorを
        #     を呼び出すまでもなく、successorのpredecessorは自身になっている. 従って後続でノードダウン検出した場合の
        #     check_predecessorの呼び出しは不要であるが呼び出しは行うようにしておく
        ret = successor.endpoints.grpc__check_predecessor(self.existing_node.node_info)
        if (ret.is_ok):
            pass
        else:  # ret.err_code == ErrorCode.InternalControlFlowException_CODE
            # joinの中から呼び出された際に、successorを辿って行った結果、一周してjoin処理中のノードを get_node_by_addressしようと
            # した際にcheck_predecessorで発生する場合があるので、ここで対処する
            # join処理中のノードのpredecessor, sucessorはjoin処理の中で適切に設定されているはずなの特に処理は不要
            ChordUtil.dprint(
                "stabilize_successor_inner_fix_chain_3," + ChordUtil.gen_debug_str_of_node(
                    self.existing_node.node_info) + ","
                + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]))

        # TODO: x direct access to node_info of successor at stabilize_successor_inner_fix_chain
        distance_unknown = ChordUtil.calc_distance_between_nodes_left_mawari(successor.node_info.node_id,
                                                                             pred_id_of_successor)
        # TODO: x direct access to node_info of successor at stabilize_successor_inner_fix_chain
        distance_me = ChordUtil.calc_distance_between_nodes_left_mawari(successor.node_info.node_id,
                                                                        self.existing_node.node_info.node_id)
        if distance_unknown < distance_me:
            # successorの認識しているpredecessorが自身ではなく、かつ、そのpredecessorが
            # successorから自身に対して前方向にたどった場合の経路中に存在する場合
            # 自身の認識するsuccessorの情報を更新する

            def handle_err() -> PResult[bool]:
                # 例外発生時は張り替えを中止する
                #   - successorは変更しない
                #   - この時点でのsuccessor[0]が認識するpredecessorを自身とする(successr[0]のcheck_predecessorを呼び出す)

                # successor[0]の変更は行わず、ダウンしていたノードではなく自身をpredecessorとするよう(間接的に)要請する
                # TODO: check_predecessor call at stabilize_successor_inner_fix_chain
                ret2 = successor.endpoints.grpc__check_predecessor(self.existing_node.node_info)
                if (ret2.is_ok):
                    pass
                else:  # ret.err_code == ErrorCode.InternalControlFlowException_CODE
                    ChordUtil.dprint("stabilize_successor_inner_fix_chain_4," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                        + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]))
                    return PResult.Err(False, cast(int, ret2.err_code))

                ChordUtil.dprint("stabilize_successor_inner_fix_chain_5," + ChordUtil.gen_debug_str_of_node(
                    self.existing_node.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(
                    self.existing_node.node_info.successor_info_list[0]))

                return PResult.Err(False, ErrorCode.InternalControlFlowException_CODE)

            # TODO: x direct access to predecessor_info of successor at stabilize_successor_inner_fix_chain
            ret3 = ChordUtil.get_node_by_address(cast('NodeInfo', successor.node_info.predecessor_info).address_str)
            if (ret3.is_ok):
                new_successor: 'ChordNode' = cast('ChordNode', ret3.result)
            else:  # ret.err_code == ErrorCode.InternalControlFlowException_CODE
                return handle_err()

            # TODO: x direct access to node_info of new_successor at stabilize_successor_inner_fix_chain
            self.existing_node.node_info.successor_info_list.insert(0,
                                                                    new_successor.node_info.get_partial_deepcopy())

            # 新たなsuccesorに対して担当データのレプリカを渡す
            tantou_data_list: List[DataIdAndValue] = \
                self.existing_node.data_store.get_all_tantou_data()
            # TODO: receive_replica call at stabilize_successor_inner_fix_chain
            new_successor.endpoints.grpc__receive_replica(tantou_data_list)

            # successorListから溢れたノードがいた場合、自ノードの担当データのレプリカを削除させ、successorListから取り除く
            # (この呼び出しの中でsuccessorListからのノード情報の削除も行われる)

            ret4 = self.check_successor_list_length()
            if (ret.is_ok):
                pass
            else:  # ret.err_code == ErrorCode.InternalControlFlowException_CODE
                return handle_err()

            # 新たなsuccessorに対して自身がpredecessorでないか確認を要請し必要であれ
            # ば情報を更新してもらう
            # TODO: check_predecessor call at stabilize_successor_inner_fix_chain
            ret5 = new_successor.endpoints.grpc__check_predecessor(self.existing_node.node_info)
            if (ret5.is_ok):
                pass
            else:  # ret.err_code == ErrorCode.InternalControlFlowException_CODE
                return handle_err()

            # TODO: x direct access to node_info of new_successor at stabilize_successor_inner_fix_chain
            ChordUtil.dprint("stabilize_successor_inner_fix_chain_3," + ChordUtil.gen_debug_str_of_node(
                self.existing_node.node_info) + ","
                             + ChordUtil.gen_debug_str_of_node(
                self.existing_node.node_info.successor_info_list[0]) + ","
                             + ChordUtil.gen_debug_str_of_node(new_successor.node_info))

    return PResult.Ok(True)
*/

// TODO: (rust) need implement stabilize_successor_inner func
pub fn stabilize_successor_inner(self_node: ArMu<node_info::NodeInfo>) -> Result<Option<node_info::NodeInfo>, chord_util::GeneralError>{
    return Ok(None);
}

/*
#  ノードダウンしておらず、チェーンの接続関係が正常 (predecessorの情報が適切でそのノードが生きている)
#  なノードで、諸々の処理の結果、self の successor[0] となるべきノードであると確認されたノードを返す.
#　注: この呼び出しにより、self.existing_node.node_info.successor_info_list[0] は更新される
#  規約: 呼び出し元は、selfが生きていることを確認した上で本メソッドを呼び出さなければならない
# TODO: InternalExp, DownedExp at stabilize_successor_inner
def stabilize_successor_inner(self) -> PResult[Optional['NodeInfo']]:
    if self.existing_node.node_info.lock_of_pred_info.acquire(timeout=gval.LOCK_ACQUIRE_TIMEOUT) == False:
        ChordUtil.dprint("stabilize_successor_inner_0_0," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                         + "LOCK_ACQUIRE_TIMEOUT")
        return PResult.Err(None, ErrorCode.InternalControlFlowException_CODE)
    if self.existing_node.node_info.lock_of_succ_infos.acquire(timeout=gval.LOCK_ACQUIRE_TIMEOUT) == False:
        self.existing_node.node_info.lock_of_pred_info.release()
        ChordUtil.dprint("find_successor_inner_0_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                         + "LOCK_ACQUIRE_TIMEOUT")
        return PResult.Err(None, ErrorCode.InternalControlFlowException_CODE)

    if self.existing_node.is_alive == False:
        # 処理の合間でkillされてしまっていた場合の考慮
        # 何もしないで終了する
        self.existing_node.node_info.lock_of_succ_infos.release()
        self.existing_node.node_info.lock_of_pred_info.release()
        ChordUtil.dprint(
            "stabilize_successor_innner_0_2," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
            + "REQUEST_RECEIVED_BUT_I_AM_ALREADY_DEAD")
        return PResult.Err(None, ErrorCode.InternalControlFlowException_CODE)

    ChordUtil.dprint_routing_info(self.existing_node, sys._getframe().f_code.co_name)

    try:
        # self.existing_node.node_info.successor_info_listを生きているノードだけにする

        ret = self.stabilize_successor_inner_fill_succ_list()
        if (ret.is_ok):
            pass
        else:  # ret.err_code == ErrorCode.NodeIsDownedException_CODE
            return PResult.Err(None, cast(int, ret.err_code))

        succ_addr = self.existing_node.node_info.successor_info_list[0].address_str

        ret2 = ChordUtil.is_node_alive(succ_addr)
        if (ret2.is_ok):
            if cast(bool, ret2.result) == False:
                # raise InternalControlFlowException("successor[0] is downed.")
                return PResult.Err(None, ErrorCode.InternalControlFlowException_CODE)
        else:  # ret.err_code == ErrorCode.InternalControlFlowException_CODE
            return PResult.Err(None, cast(int, ret2.err_code))


        # TODO: ひとまずここでの get_node_by_address の呼び出しはエラーを変えさない前提とする at stabilize_successor_inner
        successor = cast('ChordNode', ChordUtil.get_node_by_address(succ_addr).result)

        ret3 = self.stabilize_successor_inner_fix_chain(successor)
        if (ret3.is_ok):
            pass
        else:  # ret.err_code == ErrorCode.InternalControlFlowException_CODE || ret.err_code == ErrorCode.NodeIsDownedException_CODE
            return PResult.Err(None, cast(int, ret3.err_code))

        return PResult.Ok(self.existing_node.node_info.successor_info_list[0].get_partial_deepcopy())
    finally:
        self.existing_node.node_info.lock_of_succ_infos.release()
        self.existing_node.node_info.lock_of_pred_info.release()
*/

// TODO: (rustr) comment-outed at spiking RPC impl
// TODO: (rust) need implement stabilize_successor func
/*
pub fn stabilize_successor(self_node: ArMu<node_info::NodeInfo>) -> Result<bool, chord_util::GeneralError>{
    let self_node_refcell = get_refcell_from_arc_with_locking!(self_node);
    let self_node_ref = get_ref_from_refcell!(self_node_refcell);
    let self_node_ni_refcell = get_refcell_from_arc_with_locking!(self_node_ref.node_info);
    let self_node_ni_refmut = get_refmut_from_refcell!(self_node_ni_refcell);


    // // self_nodeのNodeInfoオブジェクトのクリティカルセクションを開始する
    // let self_node_ni_lock = chord_util::get_lock_obj("ninfo", &self_node_ni_refmut.address_str);
    // let self_node_ni_lock_keeper = get_refcell_from_arc_with_locking!(self_node_ni_lock);    
    // let successor_ni_lock;
    // let successor_ni_lock_keeper;
    // // new_successor_objのNodeInfoオブジェクトのクリティカルセクションを開始する
    // let new_successor_obj_ni_lock: ArRmRs<bool>;
    // let new_successor_obj_lock_keeper: &RefCell<bool>;


    // if self.existing_node.node_info.lock_of_pred_info.acquire(timeout=gval.LOCK_ACQUIRE_TIMEOUT) == False:
    //     chord_util::dprint("stabilize_successor_0_0," + chord_util::gen_debug_str_of_node(self.existing_node.node_info) + ","
    //                      + "LOCK_ACQUIRE_TIMEOUT")
    //     return PResult.Err(False, ErrorCode.InternalControlFlowException_CODE)
    // if self.existing_node.node_info.lock_of_succ_infos.acquire(timeout=gval.LOCK_ACQUIRE_TIMEOUT) == False:
    //     self.existing_node.node_info.lock_of_pred_info.release()
    //     chord_util::dprint("stabilize_successor_0_1," + chord_util::gen_debug_str_of_node(self.existing_node.node_info) + ","
    //                      + "LOCK_ACQUIRE_TIMEOUT")
    //     return PResult.Err(False, ErrorCode.InternalControlFlowException_CODE)

    chord_util::dprint(&("stabilize_successor_1,".to_string() + chord_util::gen_debug_str_of_node(Some(self_node_ni_refmut)).as_str() + ","
          + chord_util::gen_debug_str_of_node(Some(&self_node_ni_refmut.successor_info_list[0])).as_str()));

    // firstノードだけが存在する状況で、firstノードがself_nodeであった場合に対する考慮
    if self_node_ni_refmut.predecessor_info.len() == 0 && self_node_ni_refmut.node_id == self_node_ni_refmut.successor_info_list[0].node_id {
        chord_util::dprint(&("stabilize_successor_1_5,".to_string() + chord_util::gen_debug_str_of_node(Some(self_node_ni_refmut)).as_str() + ","
                         + chord_util::gen_debug_str_of_node(Some(&self_node_ni_refmut.successor_info_list[0])).as_str()));

        // secondノードがjoinしてきた際にチェーン構造は2ノードで適切に構成されるように
        // なっているため、ここでは何もせずに終了する

        return Ok(true);

        // // secondノードがjoin済みであれば、当該ノードのstabilize_successorによって
        // // secondノードがpredecessorとして設定されているはずなので、succesorをそちら
        // // に張り替える
        // self_node_ni_ref.successor_info_list[0] = self_node_ni_ref.predecessor_info[0].clone();
        // // finger_tableのインデックス0は必ずsuccessorになるはずなので、設定しておく
        // self_node_ni_ref.finger_table[0] = Some(self_node_ni_ref.successor_info_list[0].clone());
    }

    // 自身のsuccessorに、当該ノードが認識しているpredecessorを訪ねる
    // 自身が保持している successor_infoのミュータブルなフィールドは最新の情報でない
    // 場合があるため、successorのChordNodeオブジェクトを引いて、そこから最新のnode_info
    // の参照を得る
    let mut is_successor_has_no_pred = false;
    let successor;

    let ret = chord_util::get_node_info_by_address(&self_node_ni_refmut.successor_info_list[0].address_str);
    {
        // TODO: 故障ノードが発生しない前提であれば get_node_by_addressがエラーとなることはない・・・はず
        successor = ret.unwrap();
        let successor_refcell = get_refcell_from_arc_with_locking!(successor);
        let successor_ref = get_ref_from_refcell!(successor_refcell);
        let successor_ni_refcell = get_refcell_from_arc_with_locking!(successor_ref.node_info);
        let successor_ni_ref = get_ref_from_refcell!(successor_ni_refcell);
        let successor_info = successor_ni_ref;


        // // successorのNodeInfoオブジェクトのクリティカルセクションを開始する
        // successor_ni_lock = chord_util::get_lock_obj("ninfo", &successor_ni_ref.address_str);
        // successor_ni_lock_keeper = get_refcell_from_arc_with_locking!(successor_ni_lock);

        // 2ノードで環が構成されている場合に、お互いがstabilize_successorを呼び出した場合にデッドロック
        // してしまうケースを避けるための考慮
        if self_node_ni_refmut.node_id == self_node_ni_refmut.successor_info_list[0].node_id {
            // predecessor と successorが同一であり、firstノードの場合は上の方で既に抜けているので
            // 2ノードの場合

            chord_util::dprint(&("stabilize_successor_1_7,".to_string() + chord_util::gen_debug_str_of_node(Some(self_node_ni_refmut)).as_str() + ","
            + chord_util::gen_debug_str_of_node(Some(&self_node_ni_refmut.successor_info_list[0])).as_str()));

            return Ok(true);
        }

        if successor_info.predecessor_info.len() == 0 {
            is_successor_has_no_pred = true;


            if self_node_ni_refmut.node_id == successor_info.node_id {
                //何故か、自身がsuccessorリストに入ってしまっているのでとりあえず抜ける
                //抜けないと多重borrowでpanicしてしまうので
                chord_util::dprint(&("WARN!!!".to_string()));
                return Ok(true);
            }

            // 下のif文内で本来出力すべきだが、こちらに書いた方がラクなのでここにおいておく
            chord_util::dprint(&("stabilize_successor_2,".to_string() + chord_util::gen_debug_str_of_node(Some(self_node_ni_refmut)).as_str() + ","
            + chord_util::gen_debug_str_of_node(Some(&self_node_ni_refmut.successor_info_list[0])).as_str()));
        }
    }

    // successor_info = self.node_info.successor_info
    if is_successor_has_no_pred {
        // successor が predecessor を未設定であった場合は自身を predecessor として保持させて
        // 処理を終了する(check_predecessor関数により行う)
        //successor_info.predecessor_info.insert(0, (*self_node_ni_ref).clone()); //node_info::partial_clone_from_ref());
        check_predecessor(Arc::clone(&successor),  (*self_node_ni_refmut).clone());

        return Ok(true);
    }

    let successor_info_addr;
    let pred_id_of_successor;
    {
        let successor_refcell = get_refcell_from_arc_with_locking!(successor);
        let successor_ref = get_ref_from_refcell!(successor_refcell);
        let successor_ni_refcell = get_refcell_from_arc_with_locking!(successor_ref.node_info);
        let successor_ni_ref = get_ref_from_refcell!(successor_ni_refcell);
        let successor_info = successor_ni_ref;
        successor_info_addr = successor_info.address_str.clone();

        chord_util::dprint(&("stabilize_successor_3,".to_string() + chord_util::gen_debug_str_of_node(Some(self_node_ni_refmut)).as_str() + ","
                        + chord_util::gen_debug_str_of_node(Some(&successor_info.successor_info_list[0])).as_str()));

        pred_id_of_successor = successor_info.predecessor_info[0].node_id;

        chord_util::dprint(&("stabilize_successor_3_5,".to_string() + &format!("{:X}", pred_id_of_successor)));
    }

    // 下のパターン1から3という記述は以下の資料による説明に基づく
    // https://www.slideshare.net/did2/chorddht
    if pred_id_of_successor == self_node_ni_refmut.node_id {
        // パターン1
        // 特に訂正は不要なので処理を終了する
        return Ok(true);
    }else{
        // 以下、パターン2およびパターン3に対応する処理

        // 自身がsuccessorにとっての正しいpredecessorでないか確認を要請し必要であれば
        // 情報を更新してもらう
        // 事前チェックによって避けられるかもしれないが、常に実行する
        let successor_obj = chord_util::get_node_info_by_address(&successor_info_addr).unwrap();

        if self_node_ni_refmut.address_str == successor_info_addr {
            //何故か、自身がsuccessorリストに入ってしまっているのでとりあえず抜ける
            //抜けないと多重borrowでpanicしてしまうので
            chord_util::dprint(&("WARN!!!".to_string()));
            return Ok(true);
        }

        check_predecessor(Arc::clone(&successor_obj), (*self_node_ni_refmut).clone());

        let successor_obj_refcell = get_refcell_from_arc_with_locking!(successor_obj);
        let successor_obj_ref = get_ref_from_refcell!(successor_obj_refcell);
        let successor_obj_ni_refcell = get_refcell_from_arc_with_locking!(successor_obj_ref.node_info);
        let successor_obj_ni_ref = get_ref_from_refcell!(successor_obj_ni_refcell);

        let distance_unknown = chord_util::calc_distance_between_nodes_left_mawari(successor_obj_ni_ref.node_id, pred_id_of_successor);
        let distance_me = chord_util::calc_distance_between_nodes_left_mawari(successor_obj_ni_ref.node_id, self_node_ni_refmut.node_id);
        if distance_unknown < distance_me {
            // successorの認識しているpredecessorが自身ではなく、かつ、そのpredecessorが
            // successorから自身に対して前方向にたどった場合の経路中に存在する場合
            // 自身の認識するsuccessorの情報を更新する

            self_node_ni_refmut.successor_info_list[0] = successor_obj_ni_ref.predecessor_info[0].clone();

            // 新たなsuccessorに対して自身がpredecessorでないか確認を要請し必要であれ
            // ば情報を更新してもらう
            let new_successor_obj = chord_util::get_node_info_by_address(&self_node_ni_refmut.successor_info_list[0].address_str).unwrap();
            if self_node_ni_refmut.node_id == self_node_ni_refmut.successor_info_list[0].node_id {
                //何故か、自身がsuccessorリストに入ってしまっているのでとりあえず抜ける
                //抜けないと多重borrowでpanicしてしまうので
                chord_util::dprint(&("WARN!!!".to_string()));
                return Ok(true);
            }            
            
            let new_successor_obj_refcell = get_refcell_from_arc_with_locking!(new_successor_obj);
            let new_successor_obj_ref = get_ref_from_refcell!(new_successor_obj_refcell);
            let new_successor_obj_ni_refcell = get_refcell_from_arc_with_locking!(new_successor_obj_ref.node_info);
            let new_successor_obj_ni_ref = get_ref_from_refcell!(new_successor_obj_ni_refcell);

            // // new_successor_objのNodeInfoオブジェクトのクリティカルセクションを開始する
            // let new_successor_obj_ni_lock = chord_util::get_lock_obj("ninfo", &new_successor_obj_ni_ref.address_str);
            // let new_successor_obj_lock_keeper = get_refcell_from_arc_with_locking!(new_successor_obj_ni_lock);

            check_predecessor(Arc::clone(&new_successor_obj), (*self_node_ni_refmut).clone());

            chord_util::dprint(&("stabilize_successor_4,".to_string() + chord_util::gen_debug_str_of_node(Some(&self_node_ni_refmut)).as_str() + ","
                             + chord_util::gen_debug_str_of_node(Some(&self_node_ni_refmut.successor_info_list[0])).as_str() + ","
                             + chord_util::gen_debug_str_of_node(Some(&new_successor_obj_ni_ref)).as_str()));

            return Ok(true);
        }
    }

    return Ok(true);
    // finally:
    //     self.existing_node.node_info.lock_of_succ_infos.release()
    //     self.existing_node.node_info.lock_of_pred_info.release()    
}
*/

/*
# successorおよびpredicessorに関するstabilize処理を行う
def stabilize_successor(self):
    ChordUtil.dprint("stabilize_successor_1," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
          + ChordUtil.gen_debug_str_of_node(self.node_info.successor_info))

    # firstノードに対する考慮（ノード作成時に自身をsuccesorに設定しているために自身だけ
    # でsuccessorチェーンのループを作ったままになってしまうことを回避する）
    if self.node_info.predecessor_info != None and (self.node_info.node_id == self.node_info.successor_info.node_id):
        ChordUtil.dprint("stabilize_successor_1_5," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(self.node_info.successor_info))
        # secondノードがjoin済みであれば、当該ノードのstabilize_successorによって
        # secondノードがpredecessorとして設定されているはずなので、succesorをそちら
        # に張り替える
        self.node_info.successor_info = self.node_info.predecessor_info.get_partial_deepcopy()
        # finger_tableのインデックス0は必ずsuccessorになるはずなので、設定しておく
        self.node_info.finger_table[0] = self.node_info.successor_info.get_partial_deepcopy()

    # 自身のsuccessorに、当該ノードが認識しているpredecessorを訪ねる
    # 自身が保持している successor_infoのミュータブルなフィールドは最新の情報でない
    # 場合があるため、successorのChordNodeオブジェクトを引いて、そこから最新のnode_info
    # の参照を得る
    successor = ChordUtil.get_node_by_address(self.node_info.successor_info.address_str)
    successor_info = successor.node_info
    # successor_info = self.node_info.successor_info
    if successor_info.predecessor_info == None:
        # successor が predecessor を未設定であった場合は自身を predecessor として保持させて
        # 処理を終了する
        successor_info.predecessor_info = self.node_info.get_partial_deepcopy()

        ChordUtil.dprint("stabilize_successor_2," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(self.node_info.successor_info))
        return

    ChordUtil.dprint("stabilize_successor_3," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                     + ChordUtil.gen_debug_str_of_node(self.node_info.successor_info))

    pred_id_of_successor = successor_info.predecessor_info.node_id

    ChordUtil.dprint("stabilize_successor_3_5," + hex(pred_id_of_successor))

    # 下のパターン1から3という記述は以下の資料による説明に基づく
    # https://www.slideshare.net/did2/chorddht
    if(pred_id_of_successor == self.node_info.node_id):
        # パターン1
        # 特に訂正は不要なので処理を終了する
        return
    else:
        # 以下、パターン2およびパターン3に対応する処理

        # 自身がsuccessorにとっての正しいpredecessorでないか確認を要請し必要であれば
        # 情報を更新してもらう
        # 事前チェックによって避けられるかもしれないが、常に実行する
        successor_obj = ChordUtil.get_node_by_address(successor_info.address_str)
        successor_obj.check_predecessor(self.node_info.node_id, self.node_info)

        distance_unknown = ChordUtil.calc_distance_between_nodes_left_mawari(successor_obj.node_info.node_id, pred_id_of_successor)
        distance_me = ChordUtil.calc_distance_between_nodes_left_mawari(successor_obj.node_info.node_id, self.node_info.node_id)
        if distance_unknown < distance_me:
            # successorの認識しているpredecessorが自身ではなく、かつ、そのpredecessorが
            # successorから自身に対して前方向にたどった場合の経路中に存在する場合
            # 自身の認識するsuccessorの情報を更新する

            self.node_info.successor_info = successor_obj.node_info.predecessor_info.get_partial_deepcopy()

            # 新たなsuccessorに対して自身がpredecessorでないか確認を要請し必要であれ
            # ば情報を更新してもらう
            new_successor_obj = ChordUtil.get_node_by_address(self.node_info.successor_info.address_str)
            new_successor_obj.check_predecessor(self.node_info.node_id, self.node_info)

            ChordUtil.dprint("stabilize_successor_4," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                             + ChordUtil.gen_debug_str_of_node(self.node_info.successor_info) + ","
                             + ChordUtil.gen_debug_str_of_node(new_successor_obj.node_info))
*/

// TODO: (rust)途中まで書いた故障ノード対応版stabilize_successor
//             故障ノードが発生する想定の実装とする場合はこちらの続きを書く必要がある
/*
// successorListに関するstabilize処理を行う
// コメントにおいては、successorListの構造を意識した記述の場合、一番近いsuccessorを successor[0] と
// 記述し、以降に位置するノードは近い順に successor[idx] と記述する
// TODO: (rust) need implement stabilize_successor func (node down supported)
pub fn stabilize_successor(self_node: ArRmRs<chord_node::ChordNode>) -> Result<bool, chord_util::GeneralError>{
    let self_node_refcell = get_refcell_from_arc_with_locking!(self_node);
    let self_node_ref = get_ref_from_refcell!(self_node_refcell);
    let self_node_ni_refcell = get_refcell_from_arc_with_locking!(self_node_ref.node_info);
    let self_node_ni_ref = get_ref_from_refcell!(self_node_ni_refcell);

    // if self.existing_node.node_info.lock_of_pred_info.acquire(timeout=gval.LOCK_ACQUIRE_TIMEOUT) == False:
    //     chord_util::dprint("stabilize_successor_0_0," + chord_util::gen_debug_str_of_node(self.existing_node.node_info) + ","
    //                      + "LOCK_ACQUIRE_TIMEOUT")
    //     return PResult.Err(False, ErrorCode.InternalControlFlowException_CODE)
    // if self.existing_node.node_info.lock_of_succ_infos.acquire(timeout=gval.LOCK_ACQUIRE_TIMEOUT) == False:
    //     self.existing_node.node_info.lock_of_pred_info.release()
    //     chord_util::dprint("stabilize_successor_0_1," + chord_util::gen_debug_str_of_node(self.existing_node.node_info) + ","
    //                      + "LOCK_ACQUIRE_TIMEOUT")
    //     return PResult.Err(False, ErrorCode.InternalControlFlowException_CODE)


    // TODO: (rust) 現状、ノード故障は想定しない
    // if self_node_ref.is_alive == false {
    //     // 処理の合間でkillされてしまっていた場合の考慮
    //     // 何もしないで終了する
    //     /*
    //     self.existing_node.node_info.lock_of_succ_infos.release();
    //     self.existing_node.node_info.lock_of_pred_info.release();
    //     */
    //     chord_util::dprint("stabilize_successor_0_2," + chord_util::gen_debug_str_of_node(Some(self_node_ni_ref)) + ","
    //                      + "REQUEST_RECEIVED_BUT_I_AM_ALREADY_DEAD");
    //     return Ok(true);
    // }


    // TODO (rust) 現状、故障ノードの発生は想定しないため、successorは1ノードで構わない（下のwhile分の終わりまでの話）

    // //try:
    // chord_util::dprint("stabilize_successor_1," + chord_util::gen_debug_str_of_node(Some(self_node_ni_ref)) + ","
    //         + chord_util::gen_debug_str_of_node(Some(&self_node_ni_ref.successor_info_list[0])));

    // // 後続のノード（successorや、successorのsuccessor ....）を辿っていき、
    // // downしているノードをよけつつ、各ノードの接続関係を正常に修復していきつつ、
    // // self.existing_node.node_info.successor_info_list に最大で gval.SUCCESSOR_LIST_NORMAL_LEN個
    // // のノード情報を詰める.
    // // 処理としては successor 情報を1ノード分しか保持しない設計であった際のstabilize_successorを
    // // successorList内のノードに順に呼び出して、stabilize処理を行わせると同時に、そのノードのsuccessor[0]
    // // を返答させるといったものである.

    // // 最終的に self.existing_node.node_info.successor_info_listに上書きするリスト
    // let updated_list: Vec<node_info::NodeInfo> = vec![];

    // // 最初は自ノードを指定してそのsuccessor[0]を取得するところからスタートする
    // let cur_node : ArRmRs<chord_node::ChordNode> = self_node;

    // // TODO: (rust) NodeInfoの参照から実体のCloneを作れるようにNodeInfoにTraitを実装できる気がする
    // let last_node_info : node_info::NodeInfo = self_node_ni_ref;

    // let tried_getting_succ_cnt = 0;
    // let exception_occured = false;
    // let cur_backup_succ_list = &self_node_ni_ref.successor_info_list;
    // // 正常に、もしくは正常な、successor_info_listに入れるべきノードが取得できなかった
    // // 場合に、バックアップとして利用する cur_backup_succ_listの参照すべきインデックス
    // let cur_backup_node_info_idx = 0;
    // while len(updated_list) < gval::SUCCESSOR_LIST_NORMAL_LEN && tried_getting_succ_cnt < gval::TRYING_GET_SUCC_TIMES_LIMIT {
    //     //try:
    //     let cur_node_info: node_info::NodeInfo;
    //     if exception_occured == false {
    //         // TODO: stabilize_successor_inner call at stabilize_successor
    //         //cur_node_info : 'NodeInfo' = cur_node.endpoints.grpc__stabilize_successor_inner()
    //         let ret = endpoints::grpc__stabilize_successor_inner(cur_node);
    //         if ret.is_ok {
    //             cur_node_info = cast('NodeInfo', ret.result);
    //         }else{  // ret.err_code == ErrorCode.InternalControlFlowException_CODE || ret.err_code == ErrorCode.NodeIsDownedException_CODE
    //             // cur_nodeがjoin中のノードでget_node_by_addressで例外が発生してしまったか、
    //             // ロックの取得でタイムアウトが発生した
    //             // あるいは、cur_node(selfの場合もあれば他ノードの場合もある)が、生存している状態が通常期待される
    //             // ところで、node_kill_th の処理がノードをダウン状態にしてしまった
    //             chord_util::dprint(
    //                 &("stabilize_successor_4,".to_string() + chord_util::gen_debug_str_of_node(Some(self_node_ni_ref)).as_str()
    //                 + ",TARGET_NODE_DOES_NOT_EXIST_EXCEPTION_IS_RAISED"));
    //             exception_occured = true;
    //             cur_backup_node_info_idx += 1;
    //             tried_getting_succ_cnt += 1;                    
    //             continue;
    //         }
    //     }else{
    //         cur_node_info = last_node_info;
    //     }

    //     if cur_node_info.node_id == self_node_ni_ref.node_id || exception_occured == true {
    //         // 返ってきたノード情報は無視し、元々持っている node_info_list内のノードを返ってきたノード情報として扱う
    //         // 長さが足りなかった場合はあきらめる
    //         if cur_backup_node_info_idx + 1 < len(cur_backup_succ_list) {
    //             chord_util::dprint(&("stabilize_successor_2,RETURNED_NODE_SAME_AS_SELF,".to_string() + chord_util::gen_debug_str_of_node(Some(self_node_ni_ref)).as_str() + ","
    //                                 + chord_util::gen_debug_str_of_node(Some(&cur_node_info)).as_str()));
    //             // 返ってきたノードが適切なものでなかったという意味で、正常にループが回った場合や、例外処理が行われた
    //             // 場合のインクリメントに加えて、インクリメントを行っておかなくてはならない
    //             cur_backup_node_info_idx += 1;
    //             cur_node_info = cur_backup_succ_list[cur_backup_node_info_idx].clone();
    //         }else{
    //             chord_util::dprint("stabilize_successor_2_5,RETURNED_NODE_SAME_AS_SELF_AND_END_SEARCH_SUCCESSOR," + chord_util::gen_debug_str_of_node(self.existing_node.node_info) + ","
    //                                 + chord_util::gen_debug_str_of_node(Some(&cur_node_info)));
    //             break;
    //         }
    //     }

    //     chord_util::dprint(&("stabilize_successor_3,".to_string() + chord_util::gen_debug_str_of_node(Some(self_node_ni_ref)).as_str() + ","
    //                         + chord_util::gen_debug_str_of_node(Some(&cur_node_info)).as_str() + ","
    //                         + str(exception_occured)));

    //     let ret2 = chord_util::get_node_by_address(&cur_node_info.address_str);
    //     if ret2.is_ok {
    //         cur_node = cast('ChordNode', ret2.result);
    //     }else{  // ret.err_code == ErrorCode.InternalControlFlowException_CODE || ret.err_code == ErrorCode.NodeIsDownedException_CODE
    //         // cur_nodeがjoin中のノードでget_node_by_addressで例外が発生してしまったか、
    //         // ロックの取得でタイムアウトが発生した
    //         // あるいは、cur_node(selfの場合もあれば他ノードの場合もある)が、生存している状態が通常期待される
    //         // ところで、node_kill_th の処理がノードをダウン状態にしてしまった
    //         chord_util::dprint(
    //             "stabilize_successor_4," + chord_util::gen_debug_str_of_node(self.existing_node.node_info)
    //             + ",TARGET_NODE_DOES_NOT_EXIST_EXCEPTION_IS_RAISED");
    //         exception_occured = true;
    //         cur_backup_node_info_idx += 1;
    //         tried_getting_succ_cnt += 1;                
    //         continue;
    //     }
    //     // cur_nodeが取得できたということは、cur_nodeはダウンしていない
    //     // チェーンを辿っている中で、生存しているノードが得られた場合は、辿っていく中で
    //     // 例外が発生した際などに、チェーンを辿らずにノード情報を得るために用いる successorのリスト
    //     // をそちらに置き換える
    //     // TODO: pass_successor_list call at stabilize_successor
    //     cur_backup_succ_list = endpoints::grpc__pass_successor_list();
    //     // 利用するリストが置き換わったので、それに合わせてインデックスをリセットする
    //     // finally節で +1 するので筋悪ではあるが、-1にしておく
    //     cur_backup_node_info_idx = -1;

    //     chord_util::dprint(
    //         "stabilize_successor_3_5," + chord_util::gen_debug_str_of_node(self.existing_node.node_info) + ","
    //         + chord_util::gen_debug_str_of_node(cur_node_info) + ","
    //         + str(cur_backup_succ_list) + ","
    //         + str(cur_backup_node_info_idx) + ","
    //         + str(exception_occured));

    //     updated_list.append(cur_node_info);
    //     exception_occured = false;
    //     last_node_info = cur_node_info;

    //     cur_backup_node_info_idx += 1;
    //     tried_getting_succ_cnt += 1;

    //     // finally:
    //     //     // 正常に処理が終わる場合、例外処理が発生した場合のいずれも1だけインクリメントが必要なため
    //     //     // ここでそれを行う
    //     //     cur_backup_node_info_idx += 1;
    //     //     tried_getting_succ_cnt += 1;
    // }


    // TODO: (rust) successorが1ノードだけだった際と同様の処理を書く必要あり (stabilize_successor)
    
    let successor = chord_util::get_node_by_address(&self_node_ni_ref.successor_info_list[0].address_str);
    // TODO: 故障が発生しない想定においてはget_node_by_addressは必ず成功する、はず
    check_predecessor(self_node, successor.unwrap());

    if len(updated_list) == 0 {
        // first node の場合の考慮
        // successor_info_listを更新しないで処理を終える
        //pass
    } else {
        self_node_ni_ref.successor_info_list = updated_list;
    }

    chord_util::dprint("stabilize_successor_5," + chord_util::gen_debug_str_of_node(Some(self_node_ni_ref)) + ","
                    + str(self_node_ni_ref.successor_info_list));

    return Ok(true);

    // finally:
    //     self.existing_node.node_info.lock_of_succ_infos.release()
    //     self.existing_node.node_info.lock_of_pred_info.release()    
}
*/

/*
# successorListに関するstabilize処理を行う
# コメントにおいては、successorListの構造を意識した記述の場合、一番近いsuccessorを successor[0] と
# 記述し、以降に位置するノードは近い順に successor[idx] と記述する
# TODO: InternalExp at stabilize_successor
def stabilize_successor(self) -> PResult[bool]:
    if self.existing_node.node_info.lock_of_pred_info.acquire(timeout=gval.LOCK_ACQUIRE_TIMEOUT) == False:
        ChordUtil.dprint("stabilize_successor_0_0," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                         + "LOCK_ACQUIRE_TIMEOUT")
        return PResult.Err(False, ErrorCode.InternalControlFlowException_CODE)
    if self.existing_node.node_info.lock_of_succ_infos.acquire(timeout=gval.LOCK_ACQUIRE_TIMEOUT) == False:
        self.existing_node.node_info.lock_of_pred_info.release()
        ChordUtil.dprint("stabilize_successor_0_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                         + "LOCK_ACQUIRE_TIMEOUT")
        return PResult.Err(False, ErrorCode.InternalControlFlowException_CODE)

    if self.existing_node.is_alive == False:
        # 処理の合間でkillされてしまっていた場合の考慮
        # 何もしないで終了する
        self.existing_node.node_info.lock_of_succ_infos.release()
        self.existing_node.node_info.lock_of_pred_info.release()
        ChordUtil.dprint("stabilize_successor_0_2," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                         + "REQUEST_RECEIVED_BUT_I_AM_ALREADY_DEAD")
        return PResult.Ok(True)

    try:
        ChordUtil.dprint("stabilize_successor_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
              + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]))

        # 後続のノード（successorや、successorのsuccessor ....）を辿っていき、
        # downしているノードをよけつつ、各ノードの接続関係を正常に修復していきつつ、
        # self.existing_node.node_info.successor_info_list に最大で gval.SUCCESSOR_LIST_NORMAL_LEN個
        # のノード情報を詰める.
        # 処理としては successor 情報を1ノード分しか保持しない設計であった際のstabilize_successorを
        # successorList内のノードに順に呼び出して、stabilize処理を行わせると同時に、そのノードのsuccessor[0]
        # を返答させるといったものである.

        # 最終的に self.existing_node.node_info.successor_info_listに上書きするリスト
        updated_list : List['NodeInfo'] = []

        # 最初は自ノードを指定してそのsuccessor[0]を取得するところからスタートする
        cur_node : 'ChordNode' = self.existing_node
        last_node_info : 'NodeInfo' = self.existing_node.node_info

        tried_getting_succ_cnt = 0
        exception_occured = False
        cur_backup_succ_list = self.existing_node.node_info.successor_info_list
        # 正常に、もしくは正常な、successor_info_listに入れるべきノードが取得できなかった
        # 場合に、バックアップとして利用する cur_backup_succ_listの参照すべきインデックス
        cur_backup_node_info_idx = 0
        while len(updated_list) < gval.SUCCESSOR_LIST_NORMAL_LEN and tried_getting_succ_cnt < gval.TRYING_GET_SUCC_TIMES_LIMIT:
            try:
                if exception_occured == False:
                    # TODO: stabilize_successor_inner call at stabilize_successor
                    #cur_node_info : 'NodeInfo' = cur_node.endpoints.grpc__stabilize_successor_inner()
                    ret = cur_node.endpoints.grpc__stabilize_successor_inner()
                    if (ret.is_ok):
                        cur_node_info : 'NodeInfo' = cast('NodeInfo', ret.result)
                    else:  # ret.err_code == ErrorCode.InternalControlFlowException_CODE || ret.err_code == ErrorCode.NodeIsDownedException_CODE
                        # cur_nodeがjoin中のノードでget_node_by_addressで例外が発生してしまったか、
                        # ロックの取得でタイムアウトが発生した
                        # あるいは、cur_node(selfの場合もあれば他ノードの場合もある)が、生存している状態が通常期待される
                        # ところで、node_kill_th の処理がノードをダウン状態にしてしまった
                        ChordUtil.dprint(
                            "stabilize_successor_4," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info)
                            + ",TARGET_NODE_DOES_NOT_EXIST_EXCEPTION_IS_RAISED")
                        exception_occured = True
                        continue
                else:
                    cur_node_info : 'NodeInfo' = last_node_info

                if cur_node_info.node_id == self.existing_node.node_info.node_id or exception_occured == True:
                    # 返ってきたノード情報は無視し、元々持っている node_info_list内のノードを返ってきたノード情報として扱う
                    # 長さが足りなかった場合はあきらめる
                    if cur_backup_node_info_idx + 1 < len(cur_backup_succ_list):
                        ChordUtil.dprint("stabilize_successor_2,RETURNED_NODE_SAME_AS_SELF," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                         + ChordUtil.gen_debug_str_of_node(cur_node_info))
                        # 返ってきたノードが適切なものでなかったという意味で、正常にループが回った場合や、例外処理が行われた
                        # 場合のインクリメントに加えて、インクリメントを行っておかなくてはならない
                        cur_backup_node_info_idx += 1
                        cur_node_info = cur_backup_succ_list[cur_backup_node_info_idx].get_partial_deepcopy()
                    else:
                        ChordUtil.dprint("stabilize_successor_2_5,RETURNED_NODE_SAME_AS_SELF_AND_END_SEARCH_SUCCESSOR," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                         + ChordUtil.gen_debug_str_of_node(cur_node_info))
                        break

                ChordUtil.dprint("stabilize_successor_3," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(cur_node_info) + ","
                                 + str(exception_occured))

                ret2 = ChordUtil.get_node_by_address(cur_node_info.address_str)
                if (ret2.is_ok):
                    cur_node: 'ChordNode' = cast('ChordNode', ret2.result)
                else:  # ret.err_code == ErrorCode.InternalControlFlowException_CODE || ret.err_code == ErrorCode.NodeIsDownedException_CODE
                    # cur_nodeがjoin中のノードでget_node_by_addressで例外が発生してしまったか、
                    # ロックの取得でタイムアウトが発生した
                    # あるいは、cur_node(selfの場合もあれば他ノードの場合もある)が、生存している状態が通常期待される
                    # ところで、node_kill_th の処理がノードをダウン状態にしてしまった
                    ChordUtil.dprint(
                        "stabilize_successor_4," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info)
                        + ",TARGET_NODE_DOES_NOT_EXIST_EXCEPTION_IS_RAISED")
                    exception_occured = True
                    continue

                # cur_nodeが取得できたということは、cur_nodeはダウンしていない
                # チェーンを辿っている中で、生存しているノードが得られた場合は、辿っていく中で
                # 例外が発生した際などに、チェーンを辿らずにノード情報を得るために用いる successorのリスト
                # をそちらに置き換える
                # TODO: pass_successor_list call at stabilize_successor
                cur_backup_succ_list = cur_node.endpoints.grpc__pass_successor_list()
                # 利用するリストが置き換わったので、それに合わせてインデックスをリセットする
                # finally節で +1 するので筋悪ではあるが、-1にしておく
                cur_backup_node_info_idx = -1

                ChordUtil.dprint(
                    "stabilize_successor_3_5," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                    + ChordUtil.gen_debug_str_of_node(cur_node_info) + ","
                    + str(cur_backup_succ_list) + ","
                    + str(cur_backup_node_info_idx) + ","
                    + str(exception_occured))

                updated_list.append(cur_node_info)
                exception_occured = False
                last_node_info = cur_node_info
            finally:
                # 正常に処理が終わる場合、例外処理が発生した場合のいずれも1だけインクリメントが必要なため
                # ここでそれを行う
                cur_backup_node_info_idx += 1
                tried_getting_succ_cnt += 1

        if len(updated_list) == 0:
            # first node の場合の考慮
            # successor_info_listを更新しないで処理を終える
            pass
        else:
            self.existing_node.node_info.successor_info_list = updated_list

        ChordUtil.dprint("stabilize_successor_5," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                     + str(self.existing_node.node_info.successor_info_list))

        return PResult.Ok(True)
    finally:
        self.existing_node.node_info.lock_of_succ_infos.release()
        self.existing_node.node_info.lock_of_pred_info.release()
*/

// TODO: (rustr) comment-outed at spiking RPC impl
/*
// FingerTableに関するstabilize処理を行う
// 一回の呼び出しで1エントリを更新する
// FingerTableのエントリはこの呼び出しによって埋まっていく
// TODO: InternalExp at stabilize_finger_table
// TODO: 注 -> (rust) このメソッドの呼び出し時はexisting_nodeのnode_infoの参照は存在しない状態としておくこと
pub fn stabilize_finger_table(existing_node: ArMu<node_info::NodeInfo>, exnode_ref: &Ref<node_info::NodeInfo>, idx: i32) -> Result<bool, chord_util::GeneralError> {
    // if self.existing_node.node_info.lock_of_pred_info.acquire(timeout=gval.LOCK_ACQUIRE_TIMEOUT) == False:
    //     ChordUtil.dprint("stabilize_finger_table_0_0," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
    //                      + "LOCK_ACQUIRE_TIMEOUT")
    //     return PResult.Err(False, ErrorCode.InternalControlFlowException_CODE)
    // if self.existing_node.node_info.lock_of_succ_infos.acquire(timeout=gval.LOCK_ACQUIRE_TIMEOUT) == False:
    //     self.existing_node.node_info.lock_of_pred_info.release()
    //     ChordUtil.dprint("stabilize_finger_table_0_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
    //                      + "LOCK_ACQUIRE_TIMEOUT")
    //     return PResult.Err(False, ErrorCode.InternalControlFlowException_CODE)
    let find_rslt: Result<ArMu<node_info::NodeInfo>, chord_util::GeneralError>;
    let exnode_ni_refcell = get_refcell_from_arc_with_locking!(exnode_ref.node_info);

    let exnode_id: u32;


    // let exnode_ni_lock;
    // let exnode_ni_lock_keeper;
    // let found_node_ni_lock;
    // let found_node_ni_lock_keeper;

    {
        let exnode_ni_ref = get_ref_from_refcell!(exnode_ni_refcell);

        // // exnodeのNodeInfoオブジェクトのクリティカルセクションを開始する        
        // exnode_ni_lock = chord_util::get_lock_obj("ninfo", &exnode_ni_ref.address_str);
        // exnode_ni_lock_keeper = get_refcell_from_arc_with_locking!(exnode_ni_lock);

        exnode_id = exnode_ni_ref.node_id;

        if exnode_ref.is_alive.load(Ordering::Relaxed) == false {
            // 処理の合間でkillされてしまっていた場合の考慮
            // 何もしないで終了する
            chord_util::dprint(&("stabilize_finger_table_0_2,".to_string() + chord_util::gen_debug_str_of_node(Some(exnode_ni_ref)).as_str() + ","
                            + "REQUEST_RECEIVED_BUT_I_AM_ALREADY_DEAD"));
            return Ok(true);
        }

        //chord_util::dprint_routing_info(self.existing_node, sys._getframe().f_code.co_name);

        chord_util::dprint(&("stabilize_finger_table_1,".to_string() + chord_util::gen_debug_str_of_node(Some(exnode_ni_ref)).as_str()));

        // FingerTableの各要素はインデックスを idx とすると 2^IDX 先のIDを担当する、もしくは
        // 担当するノードに最も近いノードが格納される
        let update_id = chord_util::overflow_check_and_conv((exnode_ni_ref.node_id as u64) + (2i32.pow(idx as u32) as u64));
        find_rslt = router::find_successor(existing_node, exnode_ref, exnode_ni_ref, update_id);
    }
    
    match find_rslt {
        Err(err_code) => {
            // ret.err_code == ErrorCode.AppropriateNodeNotFoundException_Code || ret.err_code == ErrorCode.InternalControlFlowException_CODE
            //  || ret.err_code == ErrorCode.NodeIsDownedException_CODE

            // 適切な担当ノードを得ることができなかった
            // 今回のエントリの更新はあきらめるが、例外の発生原因はおおむね見つけたノードがダウンしていた
            // ことであるので、更新対象のエントリには None を設定しておく
            let exnode_ni_refmut = get_refmut_from_refcell!(exnode_ni_refcell);
            exnode_ni_refmut.finger_table[idx as usize] = None;
            chord_util::dprint(&("stabilize_finger_table_2_5,NODE_IS_DOWNED,".to_string()
                + chord_util::gen_debug_str_of_node(Some(exnode_ni_refmut)).as_str()));
                
            return Ok(true);
        },
        Ok(found_node_arrmrs) => {
            // TODO: (rust) x direct access to node_info of found_node at stabilize_finger_table

            //見つかったノードが自分自身であった場合に借用の競合を避けるため回りくどいことをする
            let found_node_ni_cloned: node_info::NodeInfo;
            {
                let found_node_refcell = get_refcell_from_arc_with_locking!(found_node_arrmrs);
                let found_node_ref = get_ref_from_refcell!(found_node_refcell);
            
                let found_node_ni_refcell = get_refcell_from_arc_with_locking!(found_node_ref.node_info);
                let found_node_ni_ref = get_ref_from_refcell!(found_node_ni_refcell);


                // // found_nodeのNodeInfoオブジェクトのクリティカルセクションを開始する
                // found_node_ni_lock = chord_util::get_lock_obj("ninfo", &found_node_ni_ref.address_str);
                // found_node_ni_lock_keeper = get_refcell_from_arc_with_locking!(found_node_ni_lock);


                found_node_ni_cloned = (*found_node_ni_ref).clone();
            }

            //exnode_ni_refmut.finger_table[idx as usize] = Some(node_info::get_partial_deepcopy(found_node_ni_ref));
            let exnode_ni_refmut = get_refmut_from_refcell!(exnode_ni_refcell);
            exnode_ni_refmut.finger_table[idx as usize] = Some(found_node_ni_cloned.clone());

            // TODO: (rust) x direct access to node_info of found_node at stabilize_finger_table
            chord_util::dprint(&("stabilize_finger_table_3,".to_string() 
                    + chord_util::gen_debug_str_of_node(Some(exnode_ni_refmut)).as_str() + ","
                    + chord_util::gen_debug_str_of_node(Some(&found_node_ni_cloned)).as_str()));

            return Ok(true);
        }
    }
}
*/

/*
# FingerTableに関するstabilize処理を行う
# 一回の呼び出しで1エントリを更新する
# FingerTableのエントリはこの呼び出しによって埋まっていく
# TODO: InternalExp at stabilize_finger_table
def stabilize_finger_table(self, idx) -> PResult[bool]:
    if self.existing_node.node_info.lock_of_pred_info.acquire(timeout=gval.LOCK_ACQUIRE_TIMEOUT) == False:
        ChordUtil.dprint("stabilize_finger_table_0_0," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                         + "LOCK_ACQUIRE_TIMEOUT")
        return PResult.Err(False, ErrorCode.InternalControlFlowException_CODE)
    if self.existing_node.node_info.lock_of_succ_infos.acquire(timeout=gval.LOCK_ACQUIRE_TIMEOUT) == False:
        self.existing_node.node_info.lock_of_pred_info.release()
        ChordUtil.dprint("stabilize_finger_table_0_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                         + "LOCK_ACQUIRE_TIMEOUT")
        return PResult.Err(False, ErrorCode.InternalControlFlowException_CODE)

    if self.existing_node.is_alive == False:
        # 処理の合間でkillされてしまっていた場合の考慮
        # 何もしないで終了する
        self.existing_node.node_info.lock_of_succ_infos.release()
        self.existing_node.node_info.lock_of_pred_info.release()
        if self.existing_node.is_alive == False:
            ChordUtil.dprint(
                "stabilize_finger_table_0_2," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                + "REQUEST_RECEIVED_BUT_I_AM_ALREADY_DEAD")
            return PResult.Ok(True)

    try:
        ChordUtil.dprint_routing_info(self.existing_node, sys._getframe().f_code.co_name)

        ChordUtil.dprint("stabilize_finger_table_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info))

        # FingerTableの各要素はインデックスを idx とすると 2^IDX 先のIDを担当する、もしくは
        # 担当するノードに最も近いノードが格納される
        update_id = ChordUtil.overflow_check_and_conv(self.existing_node.node_info.node_id + 2**idx)
        ret = self.existing_node.router.find_successor(update_id)
        if (ret.is_ok):
            found_node : 'ChordNode' = cast('ChordNode', ret.result)
        else:
            # ret.err_code == ErrorCode.AppropriateNodeNotFoundException_Code || ret.err_code == ErrorCode.InternalControlFlowException_CODE
            #  || ret.err_code == ErrorCode.NodeIsDownedException_CODE

            # 適切な担当ノードを得ることができなかった
            # 今回のエントリの更新はあきらめるが、例外の発生原因はおおむね見つけたノードがダウンしていた
            # ことであるので、更新対象のエントリには None を設定しておく
            self.existing_node.node_info.finger_table[idx] = None
            ChordUtil.dprint("stabilize_finger_table_2_5,NODE_IS_DOWNED," + ChordUtil.gen_debug_str_of_node(
                self.existing_node.node_info))
            return PResult.Ok(True)

        # TODO: x direct access to node_info of found_node at stabilize_finger_table
        self.existing_node.node_info.finger_table[idx] = found_node.node_info.get_partial_deepcopy()

        # TODO: x direct access to node_info of found_node at stabilize_finger_table
        ChordUtil.dprint("stabilize_finger_table_3," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(found_node.node_info))

        return PResult.Ok(True)
    finally:
        self.existing_node.node_info.lock_of_succ_infos.release()
        self.existing_node.node_info.lock_of_pred_info.release()
*/

/*
// 自ノードの持っている successor_info_listの deep copy を返す
pub fn pass_successor_list(self_node: ArRmRs<chord_node::ChordNode>) -> Vec<node_info::NodeInfo> {
    //return [ node_info.get_partial_deepcopy() for node_info in self.existing_node.node_info.successor_info_list];

    let self_node_refcell = get_refcell_from_arc_with_locking!(self_node);
    let self_node_ref = get_ref_from_refcell!(self_node_refcell);
    let self_node_ni_refcell = get_refcell_from_arc_with_locking!(self_node_ref.node_info);
    let self_node_ni_ref = get_ref_from_refcell!(self_node_ni_refcell);

    let mut ret_vec: Vec<node_info::NodeInfo> = vec![];
    // TODO: (rust) &NodeInfoへのclone呼び出しでNodeInfoの実体のコピーを返すようにTraitを実装すれば（多分できる）
    //              returnするまでに2回コピーが発生するオーバヘッドは避けられるはず
    for ni in &self_node_ni_ref.successor_info_list {
        ret_vec.push((*ni).clone());
    }
    return ret_vec.clone();
}
*/

/*
# 自ノードの持っている successor_info_listの deep copy を返す
def pass_successor_list(self) -> List['NodeInfo']:
    return [ node_info.get_partial_deepcopy() for node_info in self.existing_node.node_info.successor_info_list]
*/

// TODO: (rustr) comment-outed at spiking RPC impl
/*
// caller_node が自身の正しい predecessor でないかチェックし、そうであった場合、経路表の情報を更新する
// 本メソッドはstabilize処理の中で用いられる
// Attention: InternalControlFlowException を raiseする場合がある
// TODO: InternalExp at check_predecessor
pub fn check_predecessor(self_node: ArMu<node_info::NodeInfo>, caller_node_ni: &node_info::NodeInfo) -> Result<bool, chord_util::GeneralError> {
    // if self.existing_node.node_info.lock_of_pred_info.acquire(timeout=gval.LOCK_ACQUIRE_TIMEOUT) == False:
    //     ChordUtil.dprint("check_predecessor_0," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
    //                      + "LOCK_ACQUIRE_TIMEOUT")
    //     return PResult.Err(False, ErrorCode.InternalControlFlowException_CODE)

    //ChordUtil.dprint_routing_info(self.existing_node, sys._getframe().f_code.co_name)
    let self_node_refcell = get_refcell_from_arc_with_locking!(self_node);
    let self_node_ref = get_ref_from_refcell!(self_node_refcell);
    let self_node_ni_refcell = get_refcell_from_arc_with_locking!(self_node_ref.node_info);
    let self_node_ni_refmut = get_refmut_from_refcell!(self_node_ni_refcell);


    // // exnodeのNodeInfoオブジェクトのクリティカルセクションを開始する        
    // let self_node_ni_lock = chord_util::get_lock_obj("ninfo", &self_node_ni_refmut.address_str);
    // let self_node_ni_lock_keeper = get_refcell_from_arc_with_locking!(self_node_ni_lock);
    // // caller_nodeのNodeInfoオブジェクトのクリティカルセクションを開始する        
    // let caller_node_ni_lock = chord_util::get_lock_obj("ninfo", &caller_node_ni.address_str);
    // let caller_node_ni_lock_keeper = get_refcell_from_arc_with_locking!(caller_node_ni_lock);

    // let caller_node_refcell = get_refcell_from_arc_with_locking!(caller_node);
    // let caller_node_ref = get_ref_from_refcell!(caller_node_refcell);
    // let caller_node_ni_refcell = get_refcell_from_arc_with_locking!(caller_node_ref.node_info);
    // let caller_node_ni_ref = get_ref_from_refcell!(caller_node_ni_refcell);

    if self_node_ni_refmut.predecessor_info.len() == 0 {
        // predecesorが設定されていなければ無条件にチェックを求められたノードを設定する
        self_node_ni_refmut.set_pred_info(caller_node_ni.clone());
        chord_util::dprint(&("check_predecessor_1,".to_string() + chord_util::gen_debug_str_of_node(Some(self_node_ni_refmut)).as_str() + ","
            + chord_util::gen_debug_str_of_node(Some(&caller_node_ni)).as_str() + ","
            + chord_util::gen_debug_str_of_node(Some(&self_node_ni_refmut.successor_info_list[0])).as_str()));
        return Ok(true);
    }

    chord_util::dprint(&("check_predecessor_2,".to_string() + chord_util::gen_debug_str_of_node(Some(self_node_ni_refmut)).as_str() + ","
            + chord_util::gen_debug_str_of_node(Some(&self_node_ni_refmut.successor_info_list[0])).as_str()));

    // TODO: (rustr) まだノードダウンの考慮は不要
    // // この時点で認識している predecessor がノードダウンしていないかチェックする
    // let is_pred_alived = match chord_util::is_node_alive(&self_node_ni_refmut.predecessor_info[0].address_str) {
    //     Err(_e) => false, // err_code == ErrorCode.InternalControlFlowException_CODE
    //     Ok(is_alive) => is_alive
    // };
    // if is_pred_alived {
    //     let distance_check = chord_util::calc_distance_between_nodes_left_mawari(self_node_ni_refmut.node_id, caller_node_ni.node_id);
    //     let distance_cur = chord_util::calc_distance_between_nodes_left_mawari(self_node_ni_refmut.node_id,
    //                                                                         self_node_ni_refmut.predecessor_info[0].node_id);
    //     // 確認を求められたノードの方が現在の predecessor より predecessorらしければ
    //     // 経路表の情報を更新する
    //     if distance_check < distance_cur {
    //         self_node_ni_refmut.set_pred_info( caller_node_ni.clone());
    //         chord_util::dprint(&("check_predecessor_3,".to_string() + chord_util::gen_debug_str_of_node(Some(self_node_ni_refmut)).as_str() + ","
    //                 + chord_util::gen_debug_str_of_node(Some(&self_node_ni_refmut.successor_info_list[0])).as_str() + ","
    //                 + chord_util::gen_debug_str_of_node(Some(&self_node_ni_refmut.predecessor_info[0])).as_str()));
    //     }
    // } else { // predecessorがダウンしていた場合は無条件でチェックを求められたノードをpredecessorに設定する
    //     self_node_ni_refmut.set_pred_info(caller_node_ni.clone());
    // }

    return Ok(true)
}
*/



/*        
    # id が自身の正しい predecessor でないかチェックし、そうであった場合、経路表の情報を更新する
    # 本メソッドはstabilize処理の中で用いられる
    # Attention: InternalControlFlowException を raiseする場合がある
    # TODO: InternalExp at check_predecessor
    def check_predecessor(self, node_info : 'NodeInfo') -> PResult[bool]:
        if self.existing_node.node_info.lock_of_pred_info.acquire(timeout=gval.LOCK_ACQUIRE_TIMEOUT) == False:
            ChordUtil.dprint("check_predecessor_0," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                + "LOCK_ACQUIRE_TIMEOUT")
            return PResult.Err(False, ErrorCode.InternalControlFlowException_CODE)

        ChordUtil.dprint_routing_info(self.existing_node, sys._getframe().f_code.co_name)

        try:
            if self.existing_node.node_info.predecessor_info == None:
                # predecesorが設定されていなければ無条件にチェックを求められたノードを設定する
                self.existing_node.node_info.predecessor_info = node_info.get_partial_deepcopy()
                ChordUtil.dprint("check_predecessor_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                    + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]))

            ChordUtil.dprint("check_predecessor_2," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                    + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]))

            # この時点で認識している predecessor がノードダウンしていないかチェックする
            ret = ChordUtil.is_node_alive(cast('NodeInfo', self.existing_node.node_info.predecessor_info).address_str)
            if (ret.is_ok):
                is_pred_alived : bool = cast(bool, ret.result)
            else:  # ret.err_code == ErrorCode.InternalControlFlowException_CODE
                is_pred_alived : bool = False

            if is_pred_alived:
                distance_check = ChordUtil.calc_distance_between_nodes_left_mawari(self.existing_node.node_info.node_id, node_info.node_id)
                distance_cur = ChordUtil.calc_distance_between_nodes_left_mawari(self.existing_node.node_info.node_id,
                                                                                    cast('NodeInfo',self.existing_node.node_info.predecessor_info).node_id)

                # 確認を求められたノードの方が現在の predecessor より predecessorらしければ
                # 経路表の情報を更新する
                if distance_check < distance_cur:
                    self.existing_node.node_info.predecessor_info = node_info.get_partial_deepcopy()

                    ChordUtil.dprint("check_predecessor_3," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                            + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]) + ","
                            + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.predecessor_info))
            else: # predecessorがダウンしていた場合は無条件でチェックを求められたノードをpredecessorに設定する
                self.existing_node.node_info.predecessor_info = node_info.get_partial_deepcopy()

            return PResult.Ok(True)
        finally:
            self.existing_node.node_info.lock_of_pred_info.release()
*/       
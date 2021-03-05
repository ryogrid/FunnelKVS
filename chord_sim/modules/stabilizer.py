# coding:utf-8

from typing import Dict, List, Optional, cast, TYPE_CHECKING

import sys
import modules.gval as gval
from .chord_util import ChordUtil, KeyValue, NodeIsDownedExceptiopn, AppropriateNodeNotFoundException, \
    InternalControlFlowException, StoredValueEntry, NodeInfoPointer, DataIdAndValue
from .taskqueue import TaskQueue

if TYPE_CHECKING:
    from .node_info import NodeInfo
    from .chord_node import ChordNode

class Stabilizer:

    # join が router.find_successorでの例外発生で失敗した場合にこのクラス変数に格納して次のjoin処理の際にリトライさせる
    # なお、本シミュレータの設計上、このフィールドは一つのデータだけ保持できれば良い
    need_join_retry_node : Optional['ChordNode'] = None
    need_join_retry_tyukai_node: Optional['ChordNode'] = None

    def __init__(self, existing_node : 'ChordNode'):
        self.existing_node : 'ChordNode' = existing_node

    # 自ノードの持っている successor_info_listの deep copy を返す
    def pass_successor_list(self) -> List['NodeInfo']:
        return [ node_info.get_partial_deepcopy() for node_info in self.existing_node.node_info.successor_info_list]

    # successor_info_listの長さをチェックし、規定長を越えていた場合余剰なノードにレプリカを
    # 削除させた上で、リストから取り除く
    def check_replication_redunduncy(self):
        if self.existing_node.node_info.lock_of_succ_infos.acquire(timeout=gval.LOCK_ACQUIRE_TIMEOUT) == False:
            ChordUtil.dprint("check_replication_redunduncy_0," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + "LOCK_ACQUIRE_TIMEOUT")
            raise InternalControlFlowException("gettting lock of succcessor_info_list is timedout.")

        try:
            ChordUtil.dprint(
                "check_replication_redunduncy_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                + str(len(self.existing_node.node_info.successor_info_list)))

            if len(self.existing_node.node_info.successor_info_list) > gval.SUCCESSOR_LIST_NORMAL_LEN:
                list_len = len(self.existing_node.node_info.successor_info_list)
                for idx in range(gval.SUCCESSOR_LIST_NORMAL_LEN, list_len):
                    # successor_info_listからエントリが削除された場合、rangeで得られる数字列全てに要素がない
                    # 状態が起こるため、最新のlengthでチェックし直す
                    if idx >= len(self.existing_node.node_info.successor_info_list):
                        break
                    node_info = self.existing_node.node_info.successor_info_list[idx]
                    try:
                        successor_node : ChordNode = ChordUtil.get_node_by_address(node_info.address_str)
                        successor_node.data_store.delete_replica(self.existing_node.node_info)
                        ChordUtil.dprint(
                            "check_replication_redunduncy_2," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                            + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[idx])
                            + str(len(self.existing_node.node_info.successor_info_list)))
                        # 余剰となったノードを successorListから取り除く
                        self.existing_node.node_info.successor_info_list.remove(node_info)
                    except NodeIsDownedExceptiopn:
                        # 余剰ノードがダウンしていた場合はここでは何も対処しない
                        ChordUtil.dprint(
                            "check_replication_redunduncy_3," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                            + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[idx])
                            + str(len(self.existing_node.node_info.successor_info_list)) + ",NODE_IS_DOWNED")
                        # ダウンしているので、レプリカを削除させることはできないが、それが取得されてしまうことも無いため
                        # 特にレプリカに関するケアは行わず、余剰となったノードとして successorListから取り除く
                        self.existing_node.node_info.successor_info_list.remove(node_info)
                        continue
        finally:
            self.existing_node.node_info.lock_of_succ_infos.release()

    # node_addressに対応するノードに問い合わせを行い、教えてもらったノードをsuccessorとして設定する
    def join(self, node_address : str):
        # TODO: joinする時点でロックが他のスレッドにとられていることは無いはず？
        with self.existing_node.node_info.lock_of_pred_info, self.existing_node.node_info.lock_of_succ_infos:
            # 実装上例外は発生しない.
            # また実システムでもダウンしているノードの情報が与えられることは想定しない
            tyukai_node = ChordUtil.get_node_by_address(node_address)
            ChordUtil.dprint("join_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + ChordUtil.gen_debug_str_of_node(tyukai_node.node_info))

            try:
                # 仲介ノードに自身のsuccessorになるべきノードを探してもらう
                successor = tyukai_node.router.find_successor(self.existing_node.node_info.node_id)
                # リトライは不要なので、本メソッドの呼び出し元がリトライ処理を行うかの判断に用いる
                # フィールドをリセットしておく
                Stabilizer.need_join_retry_node = None
            except AppropriateNodeNotFoundException:
                # リトライに必要な情報を記録しておく
                Stabilizer.need_join_retry_node = self.existing_node
                Stabilizer.need_join_retry_tyukai_node = tyukai_node

                # 自ノードの情報、仲介ノードの情報
                ChordUtil.dprint("join_2,RETRY_IS_NEEDED," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(tyukai_node.node_info))
                return

            self.existing_node.node_info.successor_info_list.append(successor.node_info.get_partial_deepcopy())

            # finger_tableのインデックス0は必ずsuccessorになるはずなので、設定しておく
            self.existing_node.node_info.finger_table[0] = self.existing_node.node_info.successor_info_list[0].get_partial_deepcopy()

            if tyukai_node.node_info.node_id == tyukai_node.node_info.successor_info_list[0].node_id:
                # secondノードの場合の考慮 (仲介ノードは必ずfirst node)

                predecessor = tyukai_node

                # 2ノードでsuccessorでもpredecessorでも、チェーン構造で正しい環が構成されるよう強制的に全て設定してしまう
                self.existing_node.node_info.predecessor_info = predecessor.node_info.get_partial_deepcopy()
                tyukai_node.node_info.predecessor_info = self.existing_node.node_info.get_partial_deepcopy()
                tyukai_node.node_info.successor_info_list[0] = self.existing_node.node_info.get_partial_deepcopy()
                # fingerテーブルの0番エントリも強制的に設定する
                tyukai_node.node_info.finger_table[0] = self.existing_node.node_info.get_partial_deepcopy()


                ChordUtil.dprint("join_3," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(tyukai_node.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]))
            else:
                # 強制的に自身を既存のチェーンに挿入する
                # successorは predecessorの 情報を必ず持っていることを前提とする
                self.existing_node.node_info.predecessor_info = cast('NodeInfo', successor.node_info.predecessor_info).get_partial_deepcopy()
                successor.node_info.predecessor_info = self.existing_node.node_info.get_partial_deepcopy()

                #TODO: 下のコメントアウトはデバッグのため。コメントアウトしても現象に変化がなければ、元に戻すこと！
                #      at join

                # # successor_info_listを埋めておく
                # succ_list_of_succ: List[NodeInfo] = successor.stabilizer.pass_successor_list()
                # list_len = len(succ_list_of_succ)
                # for idx in range(0, gval.SUCCESSOR_LIST_NORMAL_LEN - 1):
                #     if idx < list_len:
                #         self.existing_node.node_info.successor_info_list.append(
                #             succ_list_of_succ[idx].get_partial_deepcopy())

                # 例外発生時は取得を試みたノードはダウンしているが、無視してpredecessorに設定したままにしておく.
                # 不正な状態に一時的になるが、predecessorをsuccessor_info_listに持つノードが
                # stabilize_successorを実行した時点で解消されるはず
                try:
                    predecessor = ChordUtil.get_node_by_address(cast('NodeInfo', self.existing_node.node_info.predecessor_info).address_str)
                    predecessor.node_info.successor_info_list.insert(0, self.existing_node.node_info.get_partial_deepcopy())

                    # self.existing_node.stabilizer.stabilize_successor()

                    ChordUtil.dprint("join_4," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                     + ChordUtil.gen_debug_str_of_node(tyukai_node.node_info) + ","
                                     + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]) + ","
                                     + ChordUtil.gen_debug_str_of_node(predecessor.node_info))
                except NodeIsDownedExceptiopn:
                    # ここでは特に何も対処しない
                    ChordUtil.dprint("join_5,NODE_IS_DOWNED" + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                     + ChordUtil.gen_debug_str_of_node(tyukai_node.node_info) + ","
                                     + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]))
                    pass

            # successorから自身が担当することになるID範囲のデータの委譲を受け、格納する
            tantou_data_list: List[KeyValue] = successor.data_store.delegate_my_tantou_data(
                self.existing_node.node_info.node_id, False)

            with self.existing_node.node_info.lock_of_datastore:
                for key_value in tantou_data_list:
                    self.existing_node.data_store.store_new_data(cast(int, key_value.data_id), key_value.value_data)

            # 残りのレプリカに関する処理は stabilize処理のためのスレッドに別途実行させる
            self.existing_node.tqueue.append_task(TaskQueue.JOIN_PARTIAL)

            ChordUtil.dprint_routing_info(self.existing_node, sys._getframe().f_code.co_name)

    # join処理のうちレプリカに関する処理を分割したもの
    # stabilize処理を行うスレッドによって一度だけ(失敗した場合はカウントしないとして)実行される
    def partial_join_op(self):
        if self.existing_node.node_info.lock_of_pred_info.acquire(timeout=gval.LOCK_ACQUIRE_TIMEOUT) == False:
            ChordUtil.dprint(
                "partial_join_op_0," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                + "LOCK_ACQUIRE_TIMEOUT")
            raise InternalControlFlowException("gettting lock of predecessor_info is timedout.")
        if self.existing_node.node_info.lock_of_succ_infos.acquire(timeout=gval.LOCK_ACQUIRE_TIMEOUT) == False:
            self.existing_node.node_info.lock_of_pred_info.release()
            ChordUtil.dprint(
                "partial_join_op_2," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                + "LOCK_ACQUIRE_TIMEOUT")
            raise InternalControlFlowException("gettting lock of succcessor_info_list is timedout.")

        ChordUtil.dprint_routing_info(self.existing_node, sys._getframe().f_code.co_name)

        try:
            # successor[0] から委譲を受けたデータを successorList 内の全ノードにレプリカとして配る
            tantou_data_list : List[StoredValueEntry] = self.existing_node.data_store.master2data_idx[str(self.existing_node.node_info.node_id)]
            for node_info in self.existing_node.node_info.successor_info_list:
                try:
                    node = ChordUtil.get_node_by_address(node_info.address_str)
                    ChordUtil.dprint("partial_join_op_3," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                     + ChordUtil.gen_debug_str_of_node(node_info) + "," + str(len(self.existing_node.node_info.successor_info_list)))
                    node.data_store.receive_replica(
                        self.existing_node.node_info,
                        [DataIdAndValue(data_id = cast('int', data.data_id), value_data=data.value_data) for data in tantou_data_list]
                    )
                except (NodeIsDownedExceptiopn, InternalControlFlowException):
                    # ノードがダウンしていた場合等は無視して次のノードに進む.
                    # ノードダウンに関する対処とそれに関連したレプリカの適切な配置はそれぞれ stabilize処理 と
                    # put処理 の中で後ほど行われるためここでは対処しない
                    # (ただし、レプリカが当該ノードに存在しない状態が短くない時間発生する可能性はある)
                    ChordUtil.dprint("partial_join_op_4,NODE_IS_DOWNED or InternalControlFlowException" + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                     + ChordUtil.gen_debug_str_of_node(node_info))
                    continue

            if self.existing_node.node_info.predecessor_info != None:
                self_predecessor_info : NodeInfo = cast('NodeInfo', self.existing_node.node_info.predecessor_info)
                try:
                    # predecessorが非Noneであれば当該ノードの担当データをレプリカとして保持しておかなければならないため
                    # データを渡してもらい、格納する
                    self_predeessor_node = ChordUtil.get_node_by_address(self_predecessor_info.address_str)
                    pred_tantou_datas : List[DataIdAndValue] = self_predeessor_node.data_store.pass_tantou_data_for_replication()
                    for iv_entry in pred_tantou_datas:
                        self.existing_node.data_store.store_new_data(iv_entry.data_id,
                                                                     iv_entry.value_data,
                                                                     master_info=self_predecessor_info.get_partial_deepcopy()
                                                                     )
                    ChordUtil.dprint("partial_join_op_5," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                     + ChordUtil.gen_debug_str_of_node(self_predeessor_node.node_info) + "," + str(len(pred_tantou_datas)))

                    # TODO: NOT_FONDが継続する状態の調査のために check_replication_redunduncy の呼び出しをコメントアウトしている
                    #       根本原因が解決したら、元に戻すこと at partial_join_op

                    # predecessor が非Noneであれば、当該predecessorのsuccessor_info_listの長さが標準を越えてしまって
                    # いる場合があるため、そのチェックと、越えていた場合の余剰のノードからレプリカを全て削除させる処理を呼び出す
                    # (この呼び出しの中で successorListからの余剰ノード情報削除も行われる）
                    self_predeessor_node.stabilizer.check_replication_redunduncy()
                except (NodeIsDownedExceptiopn, InternalControlFlowException):
                    ChordUtil.dprint("partial_join_op_6,NODE_IS_DOWNED or InternalControlFlowException" + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                     + ChordUtil.gen_debug_str_of_node(node_info))
                    # ノードがダウンしていた場合等は無視して先に進む.
                    # ノードダウンに関する対処とそれに関連したレプリカの適切な配置はそれぞれ stabilize処理 と
                    # put処理 の中で後ほど行われるためここでは対処しない
                    # (ただし、レプリカが本ノードに存在しない状態が短くない時間発生する可能性はある)
                    pass

            # successorから保持している全てのレプリカを受け取り格納する（successorよりは前に位置することになるため、
            # 基本的にsuccessorが保持しているレプリカは自身も全て保持している状態とならなければならない）
            try:
                successor : ChordNode = ChordUtil.get_node_by_address(self.existing_node.node_info.successor_info_list[0].address_str)
                passed_all_replica: Dict[NodeInfo, List[DataIdAndValue]] = successor.data_store.pass_all_replica(self.existing_node.node_info)
                self.existing_node.data_store.store_replica_of_several_masters(passed_all_replica)
            except:
                # ノードがダウンしていた場合等は無視して先に進む.
                # ノードダウンに関する対処とそれに関連したレプリカの適切な配置はそれぞれ stabilize処理 と
                # put処理 の中で後ほど行われるためここでは対処しない
                # (ただし、レプリカが本ノードに存在しない状態が短くない時間発生する可能性はある)
                ChordUtil.dprint(
                    "partial_join_op_7,NODE_IS_DOWNED or InternalControlFlowException" + ChordUtil.gen_debug_str_of_node(
                        self.existing_node.node_info) + ","
                    + ChordUtil.gen_debug_str_of_node(node_info))

            # join処理が全て終わった
            self.existing_node.is_join_op_finished = True

            # 自ノードの情報、仲介ノードの情報、successorとして設定したノードの情報
            ChordUtil.dprint("partial_join_op_8," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]))
        except KeyError:
            # まだ put されていないことを意味するので、無視して正常終了する
            ChordUtil.dprint("partial_join_op_9," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + "SUCCESS_WITH_NO_DELEGATED_DATA")
            self.existing_node.is_join_op_finished = True
        finally:
            self.existing_node.node_info.lock_of_succ_infos.release()
            self.existing_node.node_info.lock_of_pred_info.release()

    # id が自身の正しい predecessor でないかチェックし、そうであった場合、経路表の情報を更新する
    # 本メソッドはstabilize処理の中で用いられる
    # Attention: InternalControlFlowException を raiseする場合がある
    def check_predecessor(self, id : int, node_info : 'NodeInfo'):
        if self.existing_node.node_info.lock_of_pred_info.acquire(timeout=gval.LOCK_ACQUIRE_TIMEOUT) == False:
            ChordUtil.dprint("check_predecessor_0," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + "LOCK_ACQUIRE_TIMEOUT")
            raise InternalControlFlowException("gettting lock of predecessor_info is timedout.")

        ChordUtil.dprint_routing_info(self.existing_node, sys._getframe().f_code.co_name)

        try:
            ChordUtil.dprint("check_predecessor_2," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                  + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]))

            # この時点で認識している predecessor がノードダウンしていないかチェックする
            is_pred_alived = ChordUtil.is_node_alive(cast('NodeInfo', self.existing_node.node_info.predecessor_info).address_str)

            if is_pred_alived:
                distance_check = ChordUtil.calc_distance_between_nodes_left_mawari(self.existing_node.node_info.node_id, id)
                distance_cur = ChordUtil.calc_distance_between_nodes_left_mawari(self.existing_node.node_info.node_id, cast('NodeInfo',self.existing_node.node_info.predecessor_info).node_id)

                # 確認を求められたノードの方が現在の predecessor より predecessorらしければ
                # 経路表の情報を更新する
                if distance_check < distance_cur:
                    self.existing_node.node_info.predecessor_info = node_info.get_partial_deepcopy()

                    ChordUtil.dprint("check_predecessor_3," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                          + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]) + ","
                          + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.predecessor_info))
            else: # predecessorがダウンしていた場合は無条件でチェックを求められたノードをpredecessorに設定する
                self.existing_node.node_info.predecessor_info = node_info.get_partial_deepcopy()
        finally:
            self.existing_node.node_info.lock_of_pred_info.release()

    #  ノードダウンしておらず、チェーンの接続関係が正常 (predecessorの情報が適切でそのノードが生きている)
    #  なノードで、諸々の処理の結果、self の successor[0] となるべきノードであると確認されたノードを返す.
    #　注: この呼び出しにより、self.existing_node.node_info.successor_info_list[0] は更新される
    #  規約: 呼び出し元は、selfが生きていることを確認した上で本メソッドを呼び出さなければならない
    def stabilize_successor_inner(self) -> 'NodeInfo':
        if self.existing_node.node_info.lock_of_pred_info.acquire(timeout=gval.LOCK_ACQUIRE_TIMEOUT) == False:
            ChordUtil.dprint("stabilize_successor_inner_0_0," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + "LOCK_ACQUIRE_TIMEOUT")
            raise InternalControlFlowException("gettting lock of predecessor_info is timedout.")
        if self.existing_node.node_info.lock_of_succ_infos.acquire(timeout=gval.LOCK_ACQUIRE_TIMEOUT) == False:
            self.existing_node.node_info.lock_of_pred_info.release()
            ChordUtil.dprint("find_successor_inner_0_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + "LOCK_ACQUIRE_TIMEOUT")
            raise InternalControlFlowException("gettting lock of succcessor_info_list is timedout.")

        ChordUtil.dprint_routing_info(self.existing_node, sys._getframe().f_code.co_name)

        try:
            # 本メソッド呼び出しでsuccessorとして扱うノードはsuccessorListからダウンしているノードを取り除いた上で
            # successor_info_list[0]となったノードとする
            ChordUtil.dprint("stabilize_successor_inner_0," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info))

            #successor : 'ChordNode'
            successor_list_tmp : List['NodeInfo'] = []
            for idx in range(len(self.existing_node.node_info.successor_info_list)):
                try:
                    if ChordUtil.is_node_alive(self.existing_node.node_info.successor_info_list[idx].address_str):
                        successor_list_tmp.append(self.existing_node.node_info.successor_info_list[idx])
                    else:
                        ChordUtil.dprint("stabilize_successor_inner_1,SUCCESSOR_IS_DOWNED,"
                                         + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                         + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[idx]))
                except InternalControlFlowException:
                    # joinの中から呼び出された際に、successorを辿って行った結果、一周してjoin処理中のノードを get_node_by_addressしようと
                    # した際に発生してしまうので、ここで対処する
                    # join処理中のノードのpredecessor, sucessorはjoin処理の中で適切に設定されているはずなので、後続の処理を行わず successor[0]を返す
                    # TODO: なお、join処理中のノードがsuccessor_info_listに入っていた場合も同様に例外がraiseされるケースがある
                    continue
                    #return self.existing_node.node_info.successor_info_list[0].get_partial_deepcopy()

            if len(successor_list_tmp) == 0:
                # successorListの全てのノードを当たっても、生きているノードが存在しなかった場合
                # 起きてはいけない状況なので例外を投げてプログラムを終了させる
                ChordUtil.dprint("stabilize_successor_inner_1_1,,"
                                 + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                 + str(len(self.existing_node.node_info.successor_info_list)) + ","
                                 + str(self.existing_node.node_info.successor_info_list),
                                 flush=True)
                raise Exception("Maybe some parameters related to fault-tolerance of Chord network are not appropriate")
            else:
                self.existing_node.node_info.successor_info_list = successor_list_tmp
                successor =  ChordUtil.get_node_by_address(self.existing_node.node_info.successor_info_list[0].address_str)

            # # 生存が確認されたノードを successor[0] として設定する
            # self.existing_node.node_info.successor_info_list[0] = successor.node_info.get_partial_deepcopy()

            ChordUtil.dprint("stabilize_successor_inner_1_2," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]))

            pred_id_of_successor = cast('NodeInfo', successor.node_info.predecessor_info).node_id

            ChordUtil.dprint("stabilize_successor_inner_2," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]) + ","
                             + str(pred_id_of_successor))

            # successorが認識している predecessor の情報をチェックさせて、適切なものに変更させたり、把握していない
            # 自身のsuccessor[0]になるべきノードの存在が判明した場合は 自身の successor[0] をそちらに張り替える.
            # なお、下のパターン1から3という記述は以下の資料による説明に基づく
            # https://www.slideshare.net/did2/chorddht
            if(pred_id_of_successor == self.existing_node.node_info.node_id):
                # パターン1
                # 特に訂正は不要
                ChordUtil.dprint("stabilize_successor_inner_3," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]) + ","
                                 + str(pred_id_of_successor))
            else:
                # 以下、パターン2およびパターン3に対応する処理

                try:
                    # 自身がsuccessorにとっての正しいpredecessorでないか確認を要請し必要であれば
                    # 情報を更新してもらう
                    # 注: successorが認識していた predecessorがダウンしていた場合、下の呼び出しにより後続でcheck_predecessorを
                    #     を呼び出すまでもなく、successorのpredecessorは自身になっている. 従って後続でノードダウン検出した場合の
                    #     check_predecessorの呼び出しは不要であるが呼び出しは行うようにしておく
                    successor.stabilizer.check_predecessor(self.existing_node.node_info.node_id, self.existing_node.node_info)

                    distance_unknown = ChordUtil.calc_distance_between_nodes_left_mawari(successor.node_info.node_id, pred_id_of_successor)
                    distance_me = ChordUtil.calc_distance_between_nodes_left_mawari(successor.node_info.node_id, self.existing_node.node_info.node_id)
                    if distance_unknown < distance_me:
                        # successorの認識しているpredecessorが自身ではなく、かつ、そのpredecessorが
                        # successorから自身に対して前方向にたどった場合の経路中に存在する場合
                        # 自身の認識するsuccessorの情報を更新する

                        try:
                            new_successor = ChordUtil.get_node_by_address(cast('NodeInfo', successor.node_info.predecessor_info).address_str)
                            self.existing_node.node_info.successor_info_list.insert(0, new_successor.node_info.get_partial_deepcopy())

                            # 新たなsuccesorに対して担当データのレプリカを渡す
                            tantou_data_list : List[DataIdAndValue] = \
                                self.existing_node.data_store.get_all_replica_by_master_node(self.existing_node.node_info.node_id)
                            new_successor.data_store.receive_replica(self.existing_node.node_info.get_partial_deepcopy(),
                                                                     tantou_data_list, replace_all=True)

                            # TODO: NOT_FONDが継続する状態の調査のために check_replication_redunduncy の呼び出しをコメントアウトしている
                            #       根本原因が解決したら、元に戻すこと at stabilize_successor_inner

                            # successorListから溢れたノードがいた場合、自ノードの担当データのレプリカを削除させ、successorListから取り除く
                            # (この呼び出しの中でsuccessorListからのノード情報の削除も行われる)
                            self.check_replication_redunduncy()

                            # 新たなsuccessorに対して自身がpredecessorでないか確認を要請し必要であれ
                            # ば情報を更新してもらう
                            new_successor.stabilizer.check_predecessor(self.existing_node.node_info.node_id, self.existing_node.node_info)

                            ChordUtil.dprint("stabilize_successor_inner_4," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                             + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]) + ","
                                             + ChordUtil.gen_debug_str_of_node(new_successor.node_info))
                        except (NodeIsDownedExceptiopn, InternalControlFlowException):
                            # 例外発生時は張り替えを中止する
                            #   - successorは変更しない
                            #   - この時点でのsuccessor[0]が認識するpredecessorを自身とする(successr[0]のcheck_predecessorを呼び出す)

                            # successor[0]の変更は行わず、ダウンしていたノードではなく自身をpredecessorとするよう(間接的に)要請する
                            successor.stabilizer.check_predecessor(self.existing_node.node_info.node_id, self.existing_node.node_info)
                            ChordUtil.dprint("stabilize_successor_inner_5," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                             + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]))

                except InternalControlFlowException:
                    # joinの中から呼び出された際に、successorを辿って行った結果、一周してjoin処理中のノードを get_node_by_addressしようと
                    # した際にcheck_predecessorで発生する場合があるので、ここで対処する
                    # join処理中のノードのpredecessor, sucessorはjoin処理の中で適切に設定されているはずなの特に処理は不要であり
                    # 本メソッドは元々の successor[0] を返せばよい
                    ChordUtil.dprint("stabilize_successor_inner_6," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                     + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]))

            return self.existing_node.node_info.successor_info_list[0].get_partial_deepcopy()
        finally:
            self.existing_node.node_info.lock_of_succ_infos.release()
            self.existing_node.node_info.lock_of_pred_info.release()

    # successorListに関するstabilize処理を行う
    # コメントにおいては、successorListの構造を意識した記述の場合、一番近いsuccessorを successor[0] と
    # 記述し、以降に位置するノードは近い順に successor[idx] と記述する
    def stabilize_successor(self):
        if self.existing_node.node_info.lock_of_pred_info.acquire(timeout=gval.LOCK_ACQUIRE_TIMEOUT) == False:
            ChordUtil.dprint("stabilize_successor_0_0," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + "LOCK_ACQUIRE_TIMEOUT")
            raise InternalControlFlowException("gettting lock of predecessor_info is timedout.")
        if self.existing_node.node_info.lock_of_succ_infos.acquire(timeout=gval.LOCK_ACQUIRE_TIMEOUT) == False:
            self.existing_node.node_info.lock_of_pred_info.release()
            ChordUtil.dprint("stabilize_successor_0_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + "LOCK_ACQUIRE_TIMEOUT")
            raise InternalControlFlowException("gettting lock of succcessor_info_list is timedout.")
        try:
            ChordUtil.dprint("stabilize_successor_0," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
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

            while len(updated_list) < gval.SUCCESSOR_LIST_NORMAL_LEN:
                try:
                    cur_node_info : 'NodeInfo' = cur_node.stabilizer.stabilize_successor_inner()
                    ChordUtil.dprint("stabilize_successor_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                     + ChordUtil.gen_debug_str_of_node(cur_node_info))
                    # TODO: succesorの中に、自ノードをsuccessorと持っているノードがいる場合がある問題の暫定対処
                    #       として、以下の条件が成立しても、自身のsuccessorListの内容を参照するなどして、返ってきた自ノードは
                    #       無視して処理を継続するようにすれば良いのではないだろうか
                    if cur_node_info.node_id == self.existing_node.node_info.node_id:
                        # Chordネットワークに (downしていない状態で) 存在するノード数が gval.SUCCESSOR_LIST_NORMAL_LEN
                        # より少ない場合 successorをたどっていった結果、自ノードにたどり着いてしまうため、その場合は規定の
                        # ノード数を満たしていないが、successor_info_list の更新処理は終了する
                        ChordUtil.dprint("stabilize_successor_2," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                         + ChordUtil.gen_debug_str_of_node(cur_node_info))
                        if len(updated_list) == 0:
                            # first node の場合の考慮
                            # second node が 未joinの場合、successsor[0] がリストに存在しない状態となってしまうため
                            # その場合のみ、updated_list で self.existing_node.node_info.successor_info_listを上書きせずにreturnする
                            ChordUtil.dprint("stabilize_successor_2_5," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                             + ChordUtil.gen_debug_str_of_node(cur_node_info))
                            return

                        break


                    cur_node = ChordUtil.get_node_by_address(cur_node_info.address_str)
                    updated_list.append(cur_node_info)
                except (InternalControlFlowException, NodeIsDownedExceptiopn):
                    # cur_nodeがjoin中のノードでget_node_by_addressで例外が発生してしまったか、
                    # ロックの取得でタイムアウトが発生した
                    # あるいは、cur_node(selfの場合もあれば他ノードの場合もある)が、生存している状態が通常期待される
                    # ところで、node_kill_th の処理がノードをダウン状態にしてしまった
                    if len(updated_list) == 0:
                        # この場合は、successsor_info_listを更新することなく処理を終了する
                        ChordUtil.dprint("stabilize_successor_3," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                         + ChordUtil.gen_debug_str_of_node(cur_node_info)
                                         + ",STABILIZE_FAILED_DUE_TO_TARGET_NODE_DOES_NOT_EXIST_EXCEPTION_IS_RAISED")
                        return
                    else:
                        # TODO: successor_info_listの[1]あたりがダウンしていた場合、ずっとsuccessor_info_listの長さが1になってしまう
                        #       ことが起こるのかも。最初にダウンしているものだけpassして、successor_info_listのアップデート候補を作る用にすればよい？
                        #       そこはstabilize_successor_innerの中の修正で対応すべき？
                        #       in stabilize_successor

                        # この場合は規定数を満たしてないはずだが、作成済みのリストで successor_info_listを更新してしまう
                        ChordUtil.dprint("stabilize_successor_3," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                         + ChordUtil.gen_debug_str_of_node(cur_node_info)
                                         + ",STABILIZE_FAILED_DUE_TO_TARGET_NODE_DOES_NOT_EXIST_EXCEPTION_IS_RAISED")
                        break

            self.existing_node.node_info.successor_info_list = updated_list
            ChordUtil.dprint("stabilize_successor_5," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                         + str(self.existing_node.node_info.successor_info_list))
        finally:
            self.existing_node.node_info.lock_of_succ_infos.release()
            self.existing_node.node_info.lock_of_pred_info.release()

    # FingerTableに関するstabilize処理を行う
    # 一回の呼び出しで1エントリを更新する
    # FingerTableのエントリはこの呼び出しによって埋まっていく
    def stabilize_finger_table(self, idx):
        if self.existing_node.node_info.lock_of_pred_info.acquire(timeout=gval.LOCK_ACQUIRE_TIMEOUT) == False:
            ChordUtil.dprint("stabilize_finger_table_0_0," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + "LOCK_ACQUIRE_TIMEOUT")
            raise InternalControlFlowException("gettting lock of predecessor_info is timedout.")
        if self.existing_node.node_info.lock_of_succ_infos.acquire(timeout=gval.LOCK_ACQUIRE_TIMEOUT) == False:
            self.existing_node.node_info.lock_of_pred_info.release()
            ChordUtil.dprint("stabilize_finger_table_0_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + "LOCK_ACQUIRE_TIMEOUT")
            raise InternalControlFlowException("gettting lock of succcessor_info_list is timedout.")

        try:
            ChordUtil.dprint_routing_info(self.existing_node, sys._getframe().f_code.co_name)

            ChordUtil.dprint("stabilize_finger_table_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info))

            # FingerTableの各要素はインデックスを idx とすると 2^IDX 先のIDを担当する、もしくは
            # 担当するノードに最も近いノードが格納される
            update_id = ChordUtil.overflow_check_and_conv(self.existing_node.node_info.node_id + 2**idx)
            try:
                found_node = self.existing_node.router.find_successor(update_id)
            except AppropriateNodeNotFoundException:
                # 適切な担当ノードを得ることができなかった
                # 今回のエントリの更新はあきらめるが、例外の発生原因はおおむね見つけたノードがダウンしていた
                # ことであるので、更新対象のエントリには None を設定しておく
                self.existing_node.node_info.finger_table[idx] = None
                ChordUtil.dprint("stabilize_finger_table_2_5,NODE_IS_DOWNED," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info))
                return

            self.existing_node.node_info.finger_table[idx] = found_node.node_info.get_partial_deepcopy()

            ChordUtil.dprint("stabilize_finger_table_3," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + ChordUtil.gen_debug_str_of_node(found_node.node_info))
        finally:
            self.existing_node.node_info.lock_of_succ_infos.release()
            self.existing_node.node_info.lock_of_pred_info.release()
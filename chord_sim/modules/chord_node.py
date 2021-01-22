# coding:utf-8

from typing import Dict, List, Optional, cast

import modules.gval as gval
from .node_info import NodeInfo
from .data_store import DataStore
from .stabilizer import Stabilizer
from .router import Router
from .chord_util import ChordUtil, KeyValue, NodeIsDownedExceptiopn, AppropriateNodeNotFoundException, \
    TargetNodeDoesNotExistException, StoredValueEntry, NodeInfoPointer, DataIdAndValue

class ChordNode:
    QUERIED_DATA_NOT_FOUND_STR = "QUERIED_DATA_WAS_NOT_FOUND"
    OP_FAIL_DUE_TO_FIND_NODE_FAIL_STR = "OPERATION_FAILED_DUE_TO_FINDING_NODE_FAIL"

    # global_get内で探索した担当ノードにgetをかけて、データを持っていないと
    # レスポンスがあった際に、持っていないか辿っていくノードの一方向における上限数
    GLOBAL_GET_NEAR_NODES_TRY_MAX_NODES = 5

    # global_getでの取得が NOT_FOUNDになった場合はこのクラス変数に格納して次のget処理の際にリトライさせる
    # なお、本シミュレータの設計上、このフィールドは一つのデータだけ保持できれば良い
    need_getting_retry_data_id : int = -1
    need_getting_retry_node : Optional['ChordNode'] = None

    # global_put が router.find_successorでの例外発生で失敗した場合にこのクラス変数に格納して次のput処理の際にリトライさせる
    # なお、本シミュレータの設計上、このフィールドは一つのデータだけ保持できれば良い
    need_put_retry_data_id : int = -1
    need_put_retry_data_value : str = ""
    need_put_retry_node : Optional['ChordNode'] = None

    # join が router.find_successorでの例外発生で失敗した場合にこのクラス変数に格納して次のjoin処理の際にリトライさせる
    # なお、本シミュレータの設計上、このフィールドは一つのデータだけ保持できれば良い
    need_join_retry_node : Optional['ChordNode'] = None
    need_join_retry_tyukai_node: Optional['ChordNode'] = None

    # join処理もコンストラクタで行ってしまう
    def __init__(self, node_address: str, first_node=False):
        self.node_info : NodeInfo = NodeInfo()

        self.data_store : DataStore = DataStore(self)
        self.stabilizer : Stabilizer = Stabilizer(self)
        self.router : Router = Router(self)


        # ミリ秒精度のUNIXTIMEから自身のアドレスにあたる文字列と、Chorネットワーク上でのIDを決定する
        self.node_info.address_str = ChordUtil.gen_address_str()
        self.node_info.node_id = ChordUtil.hash_str_to_int(self.node_info.address_str)

        gval.already_born_node_num += 1
        self.node_info.born_id = gval.already_born_node_num

        # シミュレーション時のみ必要なフィールド（実システムでは不要）
        self.is_alive = True

        if first_node:
            # 最初の1ノードの場合

            # successorとpredecessorは自身として終了する
            self.node_info.successor_info_list.append(self.node_info.get_partial_deepcopy())
            self.node_info.predecessor_info = self.node_info.get_partial_deepcopy()

            # 最初の1ノードなので、joinメソッド内で行われるsuccessor からの
            # データの委譲は必要ない

            return
        else:
            self.join(node_address)

    # TODO: successor_info_listの長さをチェックし、規定長を越えていた場合
    #       余剰なノードにレプリカを削除させた上で、リストから取り除く
    #       実装には delete_replica メソッドを用いればよい
    #       check_replication_redunduncy
    def check_replication_redunduncy(self):
        raise Exception("not implemented yet")

    # node_addressに対応するノードに問い合わせを行い、教えてもらったノードをsuccessorとして設定する
    def join(self, node_address : str):
        # 実装上例外は発生しない.
        # また実システムでもダウンしているノードの情報が与えられることは想定しない
        tyukai_node = ChordUtil.get_node_by_address(node_address)
        ChordUtil.dprint("join_1," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(tyukai_node.node_info))

        try:
            # 仲介ノードに自身のsuccessorになるべきノードを探してもらう
            successor = tyukai_node.router.find_successor(self.node_info.node_id)
            # リトライは不要なので、本メソッドの呼び出し元がリトライ処理を行うかの判断に用いる
            # フィールドをリセットしておく
            ChordNode.need_join_retry_node = None
        except AppropriateNodeNotFoundException:
            # リトライに必要な情報を記録しておく
            ChordNode.need_join_retry_node = self
            ChordNode.need_join_retry_tyukai_node = tyukai_node

            # 自ノードの情報、仲介ノードの情報
            ChordUtil.dprint("join_2,RETRY_IS_NEEDED," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                             + ChordUtil.gen_debug_str_of_node(tyukai_node.node_info))
            return

        self.node_info.successor_info_list.append(successor.node_info.get_partial_deepcopy())

        # successorから自身が担当することになるID範囲のデータの委譲を受け、格納する
        tantou_data_list : List[KeyValue] = successor.data_store.delegate_my_tantou_data(self.node_info.node_id, False)
        for key_value in tantou_data_list:
            self.data_store.store_new_data(cast(int, key_value.data_id), key_value.value_data)

        # finger_tableのインデックス0は必ずsuccessorになるはずなので、設定しておく
        self.node_info.finger_table[0] = self.node_info.successor_info_list[0].get_partial_deepcopy()

        if tyukai_node.node_info.node_id == tyukai_node.node_info.successor_info_list[0].node_id:
            # secondノードの場合の考慮 (仲介ノードは必ずfirst node)

            predecessor = tyukai_node

            # 2ノードでsuccessorでもpredecessorでも、チェーン構造で正しい環が構成されるよう強制的に全て設定してしまう
            self.node_info.predecessor_info = predecessor.node_info.get_partial_deepcopy()
            tyukai_node.node_info.predecessor_info = self.node_info.get_partial_deepcopy()
            tyukai_node.node_info.successor_info_list[0] = self.node_info.get_partial_deepcopy()
            # fingerテーブルの0番エントリも強制的に設定する
            tyukai_node.node_info.finger_table[0] = self.node_info.get_partial_deepcopy()


            ChordUtil.dprint("join_3," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                             + ChordUtil.gen_debug_str_of_node(tyukai_node.node_info) + ","
                             + ChordUtil.gen_debug_str_of_node(self.node_info.successor_info_list[0]))
        else:
            # 強制的に自身を既存のチェーンに挿入する
            # successorは predecessorの 情報を必ず持っていることを前提とする
            self.node_info.predecessor_info = cast(NodeInfo, successor.node_info.predecessor_info).get_partial_deepcopy()
            successor.node_info.predecessor_info = self.node_info.get_partial_deepcopy()

            # 例外発生時は取得を試みたノードはダウンしているが、無視してpredecessorに設定したままにしておく.
            # 不正な状態に一時的になるが、predecessorをsuccessor_info_listに持つノードが
            # stabilize_successorを実行した時点で解消されるはず
            try:
                predecessor = ChordUtil.get_node_by_address(cast(NodeInfo, self.node_info.predecessor_info).address_str)
                predecessor.node_info.successor_info_list.insert(0, self.node_info.get_partial_deepcopy())

                # successorListを埋めておく
                self.stabilizer.stabilize_successor()

                ChordUtil.dprint("join_4," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(tyukai_node.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(self.node_info.successor_info_list[0]) + ","
                                 + ChordUtil.gen_debug_str_of_node(predecessor.node_info))
            except NodeIsDownedExceptiopn:
                ChordUtil.dprint("join_5,FIND_NODE_FAILED" + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(tyukai_node.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(self.node_info.successor_info_list[0]))
                pass

        # TODO: 委譲を受けたデータをsuccessorList内の全ノードにレプリカとして配る.
        #       receive_replicaメソッドを利用する
        #       on join

        # TODO: predecessorが非Noneであれば、当該predecessorの担当データをレプリカとして保持するため受け取る.
        #       pass_tantou_data_for_replicationメソッドを利用する

        # TODO: predecessorが非Noneであれば、当該predecessorのsuccessor_info_listの長さが標準を越えてしまって
        #       いる場合があるため、そのチェックと越えていた場合の余剰のノードからレプリカを全て削除させる処理を
        #       呼び出す
        #       check_replication_redunduncyメソッドを利用する
        #       on join

        # TODO: successorから保持している全てのレプリカを受け取る（successorよりは前に位置することになるため、
        #       基本的に全てのレプリカを保持している状態とならなければならない）
        #       pass_all_replicaメソッドを利用する
        #       on join

        # 自ノードの情報、仲介ノードの情報、successorとして設定したノードの情報
        ChordUtil.dprint("join_5," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(tyukai_node.node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(self.node_info.successor_info_list[0]))

    def global_put(self, data_id : int, value_str : str) -> bool:
        try:
            target_node = self.router.find_successor(data_id)
            # リトライは不要であったため、リトライ用情報の存在を判定するフィールドを
            # 初期化しておく
            ChordNode.need_put_retry_data_id = -1
        except AppropriateNodeNotFoundException:
            # 適切なノードを得られなかったため次回呼び出し時にリトライする形で呼び出しを
            # うけられるように情報を設定しておく
            ChordNode.need_put_retry_data_id = data_id
            ChordNode.need_put_retry_node = self
            ChordUtil.dprint("global_put_1,RETRY_IS_NEEDED" + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                             + ChordUtil.gen_debug_str_of_data(data_id))
            return False

        target_node.put(data_id, value_str)
        ChordUtil.dprint("global_put_2," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(target_node.node_info) + ","
                         + ChordUtil.gen_debug_str_of_data(data_id))

        return True

    def put(self, data_id : int, value_str : str):
        ChordUtil.dprint("put_0," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_data(data_id))

        self.data_store.store_new_data(data_id, value_str)

        # レプリカを successorList内のノードに渡す
        # なお、新規ノードのjoin時のレプリカのコピーにおいて、predecessorのさらに前に位置するノードが
        # 担当するデータのレプリカは考慮されないため、successorList内のノードで自身の保持データのレプリカ
        # 全てを保持していないノードが存在する場合があるため、receive_replicaメソッド呼び出し時に返ってくる
        # レプリカの保持数が、全件となっていない場合は全て保持させる
        for succ_info in self.node_info.successor_info_list:
            try:
                succ_node : ChordNode = ChordUtil.get_node_by_address(succ_info.address_str)
            except NodeIsDownedExceptiopn:
                # stabilize_successor等を経ていずれ正常な状態に
                # なるため、ここでは何もせずに次のノードに移る
                ChordUtil.dprint("put_1," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_data(data_id) + ","
                                 + ChordUtil.gen_debug_str_of_node(succ_info))
                continue

            has_replica_cnt = succ_node.data_store.receive_replica(self.node_info.get_partial_deepcopy(),[
                                                                     DataIdAndValue(
                                                                       data_id=data_id,
                                                                       value_data=value_str
                                                                     )])
            correct_replica_cnt = self.data_store.get_replica_cnt_by_master_node(self.node_info.node_id)

            # レプリカを渡したノードが保持しているべき自ノードのレプリカを全て保持していない場合
            # 全て保持させる
            if has_replica_cnt != correct_replica_cnt:
                replica_list = self.data_store.get_all_replica_by_master_node(self.node_info.node_id)
                succ_node.data_store.receive_replica(self.node_info.get_partial_deepcopy(), replica_list, replace_all=True)
                ChordUtil.dprint("put_2," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_data(data_id) + ","
                                 + ChordUtil.gen_debug_str_of_node(succ_info))

            ChordUtil.dprint("put_3," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                             + ChordUtil.gen_debug_str_of_data(data_id) + ","
                             + ChordUtil.gen_debug_str_of_node(succ_info))

        ChordUtil.dprint("put_4," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_data(data_id))

    # 得られた value の文字列を返す
    def global_get(self, data_id : int) -> str:
        ChordUtil.dprint("global_get_0," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_data(data_id))

        try:
            target_node = self.router.find_successor(data_id)
        except AppropriateNodeNotFoundException:
            # 適切なノードを得ることができなかった

            # リトライに必要な情報をクラス変数に設定しておく
            ChordNode.need_getting_retry_data_id = data_id
            ChordNode.need_getting_retry_node = self

            ChordUtil.dprint("global_get_3,FIND_NODE_FAILED," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                             + ChordUtil.gen_debug_str_of_data(data_id))
            # 処理を終える
            return ChordNode.OP_FAIL_DUE_TO_FIND_NODE_FAIL_STR

        got_value_str = target_node.get(data_id)

        # 返ってきた値が ChordNode.QUERIED_DATA_NOT_FOUND_STR だった場合、target_nodeから
        # 一定数の predecessorを辿ってそれぞれにも data_id に対応するデータを持っていないか問い合わせるようにする
        if got_value_str == ChordNode.QUERIED_DATA_NOT_FOUND_STR:
            tried_node_num = 0
            # 最初は処理の都合上、最初にgetをかけたノードを設定する
            cur_predecessor = target_node
            while tried_node_num < ChordNode.GLOBAL_GET_NEAR_NODES_TRY_MAX_NODES:
                if cur_predecessor.node_info.predecessor_info == None:
                    ChordUtil.dprint("global_get_1,predecessor is None")
                    break
                try:
                    cur_predecessor = ChordUtil.get_node_by_address(cast(NodeInfo,cur_predecessor.node_info.predecessor_info).address_str)
                except NodeIsDownedExceptiopn:
                    ChordUtil.dprint("global_get_1,NODE_IS_DOWNED")
                    break

                got_value_str = cur_predecessor.get(data_id)
                tried_node_num += 1
                ChordUtil.dprint("global_get_1," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(cur_predecessor.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_data(data_id) + ","
                                 + got_value_str + "," + str(tried_node_num))
                if got_value_str != ChordNode.QUERIED_DATA_NOT_FOUND_STR:
                    # データが円環上でIDが小さくなっていく方向（反時計時計回りの方向）を前方とした場合に
                    # 前方に位置するpredecessorを辿ることでデータを取得することができた
                    ChordUtil.dprint("global_get_1_1," + "data found at predecessor,"
                                     + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                                     + ChordUtil.gen_debug_str_of_node(cur_predecessor.node_info))
                    break
                else:
                    # できなかった
                    ChordUtil.dprint("global_get_1_1," + "data not found at predecessor,"
                                     + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                                     + ChordUtil.gen_debug_str_of_node(cur_predecessor.node_info))

        # 返ってきた値が ChordNode.QUERIED_DATA_NOT_FOUND_STR だった場合、target_nodeから
        # 一定数の successor を辿ってそれぞれにも data_id に対応するデータを持っていないか問い合わせるようにする
        if got_value_str == ChordNode.QUERIED_DATA_NOT_FOUND_STR:
            tried_node_num = 0
            # 最初は処理の都合上、最初にgetをかけたノードを設定する
            cur_successor = target_node
            while tried_node_num < ChordNode.GLOBAL_GET_NEAR_NODES_TRY_MAX_NODES:
                try:
                    cur_successor = ChordUtil.get_node_by_address(cast(NodeInfo,cur_successor.node_info.successor_info_list[0]).address_str)
                except NodeIsDownedExceptiopn:
                    ChordUtil.dprint("global_get_2,NODE_IS_DOWNED")
                    break

                got_value_str = cur_successor.get(data_id)
                tried_node_num += 1
                ChordUtil.dprint("global_get_2," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(cur_successor.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_data(data_id) + ","
                                 + got_value_str + "," + str(tried_node_num))
                if got_value_str != ChordNode.QUERIED_DATA_NOT_FOUND_STR:
                    # データが円環上でIDが小さくなっていく方向（反時計時計回りの方向）を前方とした場合に
                    # 後方に位置するsuccessorを辿ることでデータを取得することができた
                    ChordUtil.dprint("global_get_2_1," + "data found at successor,"
                                     + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                                     + ChordUtil.gen_debug_str_of_node(cur_successor.node_info))
                    break
                else:
                    # できなかった
                    ChordUtil.dprint("global_get_2_1," + "data not found at successor,"
                                     + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                                     + ChordUtil.gen_debug_str_of_node(cur_successor.node_info))

        # リトライを試みたであろう時の処理
        if ChordNode.need_getting_retry_data_id != -1:
            if got_value_str != ChordNode.QUERIED_DATA_NOT_FOUND_STR:
                # リトライに成功した
                ChordUtil.dprint("global_get_2_5,retry of global_get is succeeded")
                # リトライは不要なためクリア
                ChordNode.need_getting_retry_data_id = -1
                ChordNode.need_getting_retry_node = None
            else:
                # リトライに失敗した（何もしない）
                ChordUtil.dprint("global_get_2_5,retry of global_get is failed")
                pass

        # 取得に失敗した場合はリトライに必要な情報をクラス変数に設定しておく
        if got_value_str == ChordNode.QUERIED_DATA_NOT_FOUND_STR:
            ChordNode.need_getting_retry_data_id = data_id
            ChordNode.need_getting_retry_node = self

        ChordUtil.dprint("global_get_3," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
              + ChordUtil.gen_debug_str_of_node(target_node.node_info) + ","
              + ChordUtil.gen_debug_str_of_data(data_id) + "," + got_value_str)
        return got_value_str

    # 得られた value の文字列を返す
    def get(self, data_id : int) -> str:
        try:
            sv_entry : StoredValueEntry = self.data_store.get(data_id)
        except:
            err_str = ChordNode.QUERIED_DATA_NOT_FOUND_STR
            ChordUtil.dprint("get_1," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_data(data_id) + "," + err_str)
            return err_str

        # TODO: get要求に応じたデータを参照した際に自身が担当でないノードであった
        #       場合は、担当ノードの生死をチェックし、生きていれば QUERIED_DATA_NOT_FOUND_STR
        #       を返し、ダウンしていた場合は、以下の2つを行った上で、保持していたデータを返す
        #       - 自身のsuccessorList内のノードに担当ノードの変更を通知する（データの紐づけを変えさせる）
        #         notify_master_node_changeメソッドを利用する
        #       - 通常、担当が切り替わった場合、レプリカの保有ノードが規定数より少なくなってしまうため、
        #         自身のsuccessorList内の全ノードがレプリカを持った状態とする
        #         receive_replicaメソッドを利用する
        #       on get

        # print(sv_entry)
        ret_value_str : str = sv_entry.value_data
        # print(ret_value_str)
        # print("", flush=True)

        ChordUtil.dprint("get_2," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_data(data_id) + "," + ret_value_str)
        return ret_value_str

    # TODO: Deleteの実装
    # def global_delete(self, key_str):
    #     print("not implemented yet")
    #
    # def delete(self, key_str):
    #     print("not implemented yet")

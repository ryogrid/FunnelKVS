# coding:utf-8

from typing import Dict, List, Optional, cast

import modules.gval as gval
from .node_info import NodeInfo
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

    # global_put が find_successorでの例外発生で失敗した場合にこのクラス変数に格納して次のput処理の際にリトライさせる
    # なお、本シミュレータの設計上、このフィールドは一つのデータだけ保持できれば良い
    need_put_retry_data_id : int = -1
    need_put_retry_data_value : str = ""
    need_put_retry_node : Optional['ChordNode'] = None

    # join が find_successorでの例外発生で失敗した場合にこのクラス変数に格納して次のjoin処理の際にリトライさせる
    # なお、本シミュレータの設計上、このフィールドは一つのデータだけ保持できれば良い
    need_join_retry_node : Optional['ChordNode'] = None
    need_join_retry_tyukai_node: Optional['ChordNode'] = None

    # join処理もコンストラクタで行ってしまう
    def __init__(self, node_address: str, first_node=False):
        self.node_info = NodeInfo()

        # TODO: レプリケーション対応したら下のコメントアウトされたフィールドに切り替える
        # KeyもValueもどちらも文字列. Keyはハッシュを通されたものなので元データの値とは異なる
        self.stored_data : Dict[str, str] = {}

        # self.stored_data : Dict[str, StoredValueEntry] = {}

        # 主担当ノードのNodeInfoオブジェクトから、そのノードが担当するデータを引くためのインデックス辞書.
        # 大半のkeyはレプリカを自身に保持させているノードとなるが、自ノードである場合も同じ枠組みで
        # 扱う.
        # つまり、レプリカでないデータについてもこのインデックス辞書は扱うことになる
        self.master2data_idx : Dict[NodeInfo, List[StoredValueEntry]]

        # 保持してるデータが紐づいている主担当ノードの情報を保持するためのリスト
        # ただし、主担当ノードが切り替わった場合に参照先を一つ切り替えるだけで関連する
        # 全データの紐づけが変更可能とするため、NodeInfoを指す（参照をフィールドに持つ）
        # NodeInfoPointerクラスを間に挟む形とし、StoredValueEntryでも当該クラスの
        # オブジェクト参照するようにしてある
        self.master_node_list : List[NodeInfoPointer]

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

    # TODO: 自ノードが担当ノードとして保持しているデータを全て返す
    #       pass_tantou_data_for_replication
    def pass_tantou_data_for_replication(self) -> List[DataIdAndValue]:
        raise Exception("not implemented yet")

    # TODO: 自ノードが担当ノードとなっているものを除いて、保持しているデータをマスター
    #       ごとに dict に詰めて返す
    #       pass_all_replica
    def pass_all_replica(self) -> Dict[NodeInfo, List[DataIdAndValue]]:
        raise Exception("not implemented yet")

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
            successor = tyukai_node.find_successor(self.node_info.node_id)
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
        tantou_data_list : List[KeyValue] = successor.delegate_my_tantou_data(self.node_info.node_id, False)
        for key_value in tantou_data_list:
            self.stored_data[str(key_value.data_id)] = key_value.value

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
                self.stabilize_successor()

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
            target_node = self.find_successor(data_id)
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

    # TODO: レプリカデータを呼び出し先ノードに受け取らせる
    #       他のノードが保持しておいて欲しいレプリカを渡す際に呼び出される.
    #       なお、master_node 引数と呼び出し元ノードは一致しない場合がある.
    #       返り値として、処理が完了した時点でmaster_nodeに紐づいているレプリカをいくつ保持して
    #       いるかを返す
    #       receive_replica
    def receive_replica(self, master_node : NodeInfo, pass_datas : List[DataIdAndValue]) -> int:
        raise Exception("not implemented yet")

    def put(self, data_id : int, value_str : str):
        key_id_str = str(data_id)
        self.stored_data[key_id_str] = value_str

        # TODO: データの保持形式の変更への対応
        #       on put

        # TODO: レプリカを successorList内のノードに渡す処理の実装
        #       receive_replicaメソッドの呼び出しが主.
        #       なお、新規ノードのjoin時のレプリカのコピーにおいて、predecessorのさらに前に位置するノードが
        #       担当するデータのレプリカは考慮されないため、successorList内のノードで自身の保持データのレプリカ
        #       全てを保持していないノードが存在する場合があるため、receive_replicaメソッド呼び出し時に返ってくる
        #       レプリカデータの保持数が、認識と合っていない場合は、不足しているデータを渡すといった対処が必要となる
        #       on put

        ChordUtil.dprint("put," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_data(data_id))

    # 得られた value の文字列を返す
    def global_get(self, data_id : int) -> str:
        ChordUtil.dprint("global_get_0," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_data(data_id))

        try:
            target_node = self.find_successor(data_id)
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


    # TODO: レプリカに紐づけられているマスターノードが切り替わったことを通知し、管理情報を
    #       通知内容に応じて更新させる
    #       notify_master_node_change
    def notify_master_node_change(self, old_master : NodeInfo, new_master : NodeInfo):
        raise Exception("not implemented yet")

    # 得られた value の文字列を返す
    def get(self, data_id : int) -> str:
        try:
            ret_value_str = self.stored_data[str(data_id)]
        except:
            ret_value_str = ChordNode.QUERIED_DATA_NOT_FOUND_STR

        # TODO: データの保持形式の変更への対応
        #       on get

        # TODO: get要求に応じたデータを参照した際に自身が担当でないノードであった
        #       場合は、担当ノードの生死をチェックし、生きていれば QUERIED_DATA_NOT_FOUND_STR
        #       を返し、ダウンしていた場合は、以下の2つを行った上で、保持していたデータを返す
        #       - 自身のsuccessorList内のノードに担当ノードの変更を通知する（データの紐づけを変えさせる）
        #         notify_master_node_changeメソッドを利用する
        #       - 通常、担当が切り替わった場合、レプリカの保有ノードが規定数より少なくなってしまうため、
        #         自身のsuccessorList内の全ノードがレプリカを持った状態とする
        #         receive_replicaメソッドを利用する
        #       on get

        ChordUtil.dprint("get," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_data(data_id) + "," + ret_value_str)
        return ret_value_str

    # TODO: Deleteの実装
    # def global_delete(self, key_str):
    #     print("not implemented yet")
    #
    # def delete(self, key_str):
    #     print("not implemented yet")

    # TODO: 保持しているレプリカを data_id の範囲を指定して削除させる.
    #       マスターノードの担当範囲の変更や、新規ノードのjoinにより、レプリカを保持させていた
    #       ノードの保持するデータに変更が生じたり、レプリケーションの対象から外れた場合に用いる.
    #       対象 data_id の範囲は [range_start, range_end) となり、両方を無指定とした場合は
    #       全範囲が対象となる
    #       delete_replica
    def delete_replica(self, master_node : NodeInfo, range_start : int = -1, range_end : int = -1):
        raise Exception("not implemented yet")

    # 自身が保持しているデータのうち委譲するものを返す.
    # 対象となるデータは時計周りに辿った際に 引数 node_id と 自身の node_id
    # の間に data_id が位置するデータである.
    # join呼び出し時、新たに参加してきた新規ノードに、successorとなる自身が、担当から外れる
    # 範囲のデータの委譲を行うために、新規ノードから呼び出される形で用いられる.
    # rest_copy引数によってコピーを渡すだけか、完全に委譲してしまい自身のデータストアからは渡したデータを削除
    # するかどうか選択できる
    def delegate_my_tantou_data(self, node_id : int, rest_copy : bool = True) -> List[KeyValue]:
        ret_datas : List[KeyValue] = []
        for key, value in self.stored_data.items():
            data_id : int = int(key)

            # Chordネットワークを右回りにたどった時に、データの id (data_id) が呼び出し元の node_id から
            # 自身の node_id の間に位置する場合は、そのデータの担当は自身から変わらないため、渡すデータから
            # 除外する
            if ChordUtil.exist_between_two_nodes_right_mawari(node_id, self.node_info.node_id, data_id):
                continue

            # 文字列の参照をそのまま用いてしまうが、文字列はイミュータブルであるため
            # 問題ない
            item = KeyValue(None, value)
            item.data_id = data_id
            ret_datas.append(item)

        # データを委譲する際に元々持っていたノードから削除するよう指定されていた場合
        if rest_copy == False:
            for kv in ret_datas:
                del self.stored_data[str(kv.data_id)]

        # TODO: 委譲したことで自身が担当ノードで無くなったデータについてsuccessorList
        #       内のノードに通知し、削除させる（それらのノードは再度同じレプリカを保持する
        #       ことになるかもしれないが、それは新担当の管轄なので、非効率ともなるがひとまず削除させる）
        #       delete_replicaメソッドを利用する
        #       削除が完了するまで本メソッドは終了しないため、新担当がレプリカを配布する処理と不整合
        #       が起こることはない
        #       on delegate_my_tantou_data

        return ret_datas

    # id が自身の正しい predecessor でないかチェックし、そうであった場合、経路表の情報を更新する
    # 本メソッドはstabilize処理の中で用いられる
    # Attention: TargetNodeDoesNotExistException を raiseする場合がある
    def check_predecessor(self, id : int, node_info : NodeInfo):
        ChordUtil.dprint("check_predecessor_2," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
              + ChordUtil.gen_debug_str_of_node(self.node_info.successor_info_list[0]))

        # この時点で認識している predecessor がノードダウンしていないかチェックする
        is_pred_alived = ChordUtil.is_node_alive(cast(NodeInfo, self.node_info.predecessor_info).address_str)

        if is_pred_alived:
            distance_check = ChordUtil.calc_distance_between_nodes_left_mawari(self.node_info.node_id, id)
            distance_cur = ChordUtil.calc_distance_between_nodes_left_mawari(self.node_info.node_id, cast(NodeInfo,self.node_info.predecessor_info).node_id)

            # 確認を求められたノードの方が現在の predecessor より predecessorらしければ
            # 経路表の情報を更新する
            if distance_check < distance_cur:
                self.node_info.predecessor_info = node_info.get_partial_deepcopy()

                ChordUtil.dprint("check_predecessor_3," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                      + ChordUtil.gen_debug_str_of_node(self.node_info.successor_info_list[0]) + ","
                      + ChordUtil.gen_debug_str_of_node(self.node_info.predecessor_info))
        else: # predecessorがダウンしていた場合は無条件でチェックを求められたノードをpredecessorに設定する
            self.node_info.predecessor_info = node_info.get_partial_deepcopy()

    #  ノードダウンしておらず、チェーンの接続関係が正常 (predecessorの情報が適切でそのノードが生きている)
    #  なノードで、諸々の処理の結果、self の successor[0] となるべきノードであると確認されたノードを返す.
    #　注: この呼び出しにより、self.node_info.successor_info_list[0] は更新される
    #  規約: 呼び出し元は、selfが生きていることを確認した上で本メソッドを呼び出さなければならない
    def stabilize_successor_inner(self) -> NodeInfo:
        # 本メソッド呼び出しでsuccessorとして扱うノードはsuccessorListの先頭から生きているもの
        # をサーチし、発見したノードとする.
        ChordUtil.dprint("stabilize_successor_inner_0," + ChordUtil.gen_debug_str_of_node(self.node_info))

        successor : ChordNode
        successor_tmp : Optional[ChordNode] = None
        for idx in range(len(self.node_info.successor_info_list)):
            try:
                if ChordUtil.is_node_alive(self.node_info.successor_info_list[idx].address_str):
                    successor_tmp = ChordUtil.get_node_by_address(self.node_info.successor_info_list[idx].address_str)
                    break
                else:
                    ChordUtil.dprint("stabilize_successor_inner_1,SUCCESSOR_IS_DOWNED,"
                                     + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                                     + ChordUtil.gen_debug_str_of_node(self.node_info.successor_info_list[idx]))
            except TargetNodeDoesNotExistException:
                # joinの中から呼び出された際に、successorを辿って行った結果、一周してjoin処理中のノードを get_node_by_addressしようと
                # した際に発生してしまうので、ここで対処する
                # join処理中のノードのpredecessor, sucessorはjoin処理の中で適切に設定されているはずなので、後続の処理を行わず successor[0]を返す
                return self.node_info.successor_info_list[0].get_partial_deepcopy()

        if successor_tmp != None:
            successor = cast(ChordNode, successor_tmp)
        else:
            # successorListの全てのノードを当たっても、生きているノードが存在しなかった場合
            # 起きてはいけない状況なので例外を投げてプログラムを終了させる
            raise Exception("Maybe some parameters related to fault-tolerance of Chord network are not appropriate")

        # 生存が確認されたノードを successor[0] として設定する
        self.node_info.successor_info_list[0] = successor.node_info.get_partial_deepcopy()

        ChordUtil.dprint("stabilize_successor_inner_1," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(self.node_info.successor_info_list[0]))

        pred_id_of_successor = cast(NodeInfo, successor.node_info.predecessor_info).node_id

        ChordUtil.dprint("stabilize_successor_inner_2," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(self.node_info.successor_info_list[0]) + ","
                         + str(pred_id_of_successor))

        # successorに認識している predecessor の情報をチェックさせて、適切なものに変更させたり、把握していない
        # 自身のsuccessor[0]になるべきノードの存在が判明した場合は 自身の successor[0]をそちらに張り替える.
        # なお、下のパターン1から3という記述は以下の資料による説明に基づく
        # https://www.slideshare.net/did2/chorddht
        if(pred_id_of_successor == self.node_info.node_id):
            # パターン1
            # 特に訂正は不要
            ChordUtil.dprint("stabilize_successor_inner_3," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                             + ChordUtil.gen_debug_str_of_node(self.node_info.successor_info_list[0]) + ","
                             + str(pred_id_of_successor))
        else:
            # 以下、パターン2およびパターン3に対応する処理

            try:
                # 自身がsuccessorにとっての正しいpredecessorでないか確認を要請し必要であれば
                # 情報を更新してもらう
                # 注: successorが認識していた predecessorがダウンしていた場合、下の呼び出しにより後続でcheck_predecessorを
                #     を呼び出すまでもなく、successorのpredecessorは自身になっている. 従って後続でノードダウン検出した場合の
                #     check_predecessorの呼び出しは不要であるが呼び出しは行うようにしておく
                successor.check_predecessor(self.node_info.node_id, self.node_info)

                distance_unknown = ChordUtil.calc_distance_between_nodes_left_mawari(successor.node_info.node_id, pred_id_of_successor)
                distance_me = ChordUtil.calc_distance_between_nodes_left_mawari(successor.node_info.node_id, self.node_info.node_id)
                if distance_unknown < distance_me:
                    # successorの認識しているpredecessorが自身ではなく、かつ、そのpredecessorが
                    # successorから自身に対して前方向にたどった場合の経路中に存在する場合
                    # 自身の認識するsuccessorの情報を更新する

                    try:
                        new_successor = ChordUtil.get_node_by_address(cast(NodeInfo, successor.node_info.predecessor_info).address_str)
                        self.node_info.successor_info_list.insert(0, new_successor.node_info.get_partial_deepcopy())

                        # TODO: 新たなsuccesorに対して担当データのレプリカを渡し、successorListから溢れたノードには
                        #       レプリカを削除させる
                        #       joinの中で行っている処理を参考に実装すれば良い
                        #       on stabilize_successor_inner

                        # 新たなsuccessorに対して自身がpredecessorでないか確認を要請し必要であれ
                        # ば情報を更新してもらう
                        new_successor.check_predecessor(self.node_info.node_id, self.node_info)

                        ChordUtil.dprint("stabilize_successor_inner_4," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                                         + ChordUtil.gen_debug_str_of_node(self.node_info.successor_info_list[0]) + ","
                                         + ChordUtil.gen_debug_str_of_node(new_successor.node_info))
                    except NodeIsDownedExceptiopn:
                        # 例外発生時は張り替えを中止する
                        #   - successorは変更しない
                        #   - この時点でのsuccessor[0]が認識するpredecessorを自身とする(successr[0]のcheck_predecessorを呼び出す)

                        # successor[0]の変更は行わず、ダウンしていたノードではなく自身をpredecessorとするよう(間接的に)要請する
                        successor.check_predecessor(self.node_info.node_id, self.node_info)
                        ChordUtil.dprint("stabilize_successor_inner_5," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                                         + ChordUtil.gen_debug_str_of_node(self.node_info.successor_info_list[0]))

            except TargetNodeDoesNotExistException:
                # joinの中から呼び出された際に、successorを辿って行った結果、一周してjoin処理中のノードを get_node_by_addressしようと
                # した際にcheck_predecessorで発生する場合があるので、ここで対処する
                # join処理中のノードのpredecessor, sucessorはjoin処理の中で適切に設定されているはずなの特に処理は不要であり
                # 本メソッドは元々の successor[0] を返せばよい
                ChordUtil.dprint("stabilize_successor_inner_6," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(self.node_info.successor_info_list[0]))

        return self.node_info.successor_info_list[0].get_partial_deepcopy()


    # successorListに関するstabilize処理を行う
    # コメントにおいては、successorListの構造を意識した記述の場合、一番近いsuccessorを successor[0] と
    # 記述し、以降に位置するノードは近い順に successor[idx] と記述する
    def stabilize_successor(self):
        # TODO: put時にレプリカを全て、もしくは一部持っていないノードについてはケアされる
        #       ため、大局的には問題ないと思われるが、ノードダウンを検出した場合や、未認識
        #       であったノードを発見した場合に、レプリカの配置状態が前述のケアでカバーできない
        #       ような状態とならないか確認する
        #       on stabilize_successor

        ChordUtil.dprint("stabilize_successor_0," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
              + ChordUtil.gen_debug_str_of_node(self.node_info.successor_info_list[0]))

        # 後続のノード（successorや、successorのsuccessor ....）を辿っていき、
        # downしているノードをよけつつ、各ノードの接続関係を正常に修復していきつつ、
        # self.node_info.successor_info_list に最大で gval.SUCCESSOR_LIST_NORMAL_LEN個
        # のノード情報を詰める.
        # 処理としては successor 情報を1ノード分しか保持しない設計であった際のstabilize_successorを
        # successorList内のノードに順に呼び出して、stabilize処理を行わせると同時に、そのノードのsuccessor[0]
        # を返答させるといったものである.

        # 最終的に self.node_info.successor_info_listに上書きするリスト
        updated_list : List[NodeInfo] = []

        # 最初は自ノードを指定してそのsuccessor[0]を取得するところからスタートする
        cur_node : ChordNode = self

        while len(updated_list) < gval.SUCCESSOR_LIST_NORMAL_LEN:
            cur_node_info : NodeInfo = cur_node.stabilize_successor_inner()
            ChordUtil.dprint("stabilize_successor_1," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                             + ChordUtil.gen_debug_str_of_node(cur_node_info))
            if cur_node_info.node_id == self.node_info.node_id:
                # Chordネットワークに (downしていない状態で) 存在するノード数が gval.SUCCESSOR_LIST_NORMAL_LEN
                # より多くない場合 successorをたどっていった結果、自ノードにたどり着いてしまうため、その場合は規定の
                # ノード数を満たしていないが、successor_info_list の更新処理は終了する
                ChordUtil.dprint("stabilize_successor_2," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(cur_node_info))
                if len(updated_list) == 0:
                    # first node の場合の考慮
                    # second node が 未joinの場合、successsor[0] がリストに存在しない状態となってしまうため
                    # その場合のみ、update_list で self.node_info.successor_info_listを上書きせずにreturnする
                    ChordUtil.dprint("stabilize_successor_2_5," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                                     + ChordUtil.gen_debug_str_of_node(cur_node_info))
                    return

                break

            updated_list.append(cur_node_info)
            # この呼び出しで例外は発生しない
            cur_node = ChordUtil.get_node_by_address(cur_node_info.address_str)

        self.node_info.successor_info_list = updated_list
        ChordUtil.dprint("stabilize_successor_3," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                     + str(self.node_info.successor_info_list))

    # FingerTableに関するstabilize処理を行う
    # 一回の呼び出しで1エントリを更新する
    # FingerTableのエントリはこの呼び出しによって埋まっていく
    def stabilize_finger_table(self, idx):
        ChordUtil.dprint("stabilize_finger_table_1," + ChordUtil.gen_debug_str_of_node(self.node_info))

        # FingerTableの各要素はインデックスを idx とすると 2^IDX 先のIDを担当する、もしくは
        # 担当するノードに最も近いノードが格納される
        update_id = ChordUtil.overflow_check_and_conv(self.node_info.node_id + 2**idx)
        try:
            found_node = self.find_successor(update_id)
        except AppropriateNodeNotFoundException:
            # 適切な担当ノードを得ることができなかった
            # 今回のエントリの更新はあきらめるが、例外の発生原因はおおむね見つけたノードがダウンしていた
            # ことであるので、更新対象のエントリには None を設定しておく
            self.node_info.finger_table[idx] = None
            ChordUtil.dprint("stabilize_finger_table_2_5,NODE_IS_DOWNED," + ChordUtil.gen_debug_str_of_node(self.node_info))
            return

        self.node_info.finger_table[idx] = found_node.node_info.get_partial_deepcopy()

        ChordUtil.dprint("stabilize_finger_table_3," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(found_node.node_info))

    # id（int）で識別されるデータを担当するノードの名前解決を行う
    # Attention: 適切な担当ノードを得ることができなかった場合、FindNodeFailedExceptionがraiseされる
    def find_successor(self, id : int) -> 'ChordNode':
        ChordUtil.dprint("find_successor_1," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
              + ChordUtil.gen_debug_str_of_data(id))

        n_dash = self.find_predecessor(id)
        if n_dash == None:
            ChordUtil.dprint("find_successor_2," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                             + ChordUtil.gen_debug_str_of_data(id))
            raise AppropriateNodeNotFoundException()

        ChordUtil.dprint("find_successor_3," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(n_dash.node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(self.node_info.successor_info_list[0]) + ","
                         + ChordUtil.gen_debug_str_of_data(id))

        try:
            # 取得しようとしたノードがダウンしていた場合 NodeIsDownedException が raise される
            return ChordUtil.get_node_by_address(n_dash.node_info.successor_info_list[0].address_str)
        except NodeIsDownedExceptiopn:
            ChordUtil.dprint("find_successor_4,FOUND_NODE_IS_DOWNED" + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                             + ChordUtil.gen_debug_str_of_data(id))
            raise AppropriateNodeNotFoundException()


    # id(int)　の前で一番近い位置に存在するノードを探索する
    def find_predecessor(self, id: int) -> 'ChordNode':
        ChordUtil.dprint("find_predecessor_1," + ChordUtil.gen_debug_str_of_node(self.node_info))

        n_dash = self
        # n_dash と n_dashのsuccessorの 間に id が位置するような n_dash を見つけたら、ループを終了し n_dash を return する
        while not ChordUtil.exist_between_two_nodes_right_mawari(n_dash.node_info.node_id, n_dash.node_info.successor_info_list[0].node_id, id):
            ChordUtil.dprint("find_predecessor_2," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                             + ChordUtil.gen_debug_str_of_node(n_dash.node_info))
            n_dash_found = n_dash.closest_preceding_finger(id)

            if n_dash_found.node_info.node_id == n_dash.node_info.node_id:
                # 見つかったノードが、n_dash と同じで、変わらなかった場合
                # 同じを経路表を用いて探索することになり、結果は同じになり無限ループと
                # なってしまうため、探索は継続せず、探索結果として n_dash (= n_dash_found) を返す
                ChordUtil.dprint("find_predecessor_3," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(n_dash.node_info))
                return n_dash_found

            # closelst_preceding_finger は id を通り越してしまったノードは返さない
            # という前提の元で以下のチェックを行う
            distance_old = ChordUtil.calc_distance_between_nodes_right_mawari(self.node_info.node_id, n_dash.node_info.node_id)
            distance_found = ChordUtil.calc_distance_between_nodes_right_mawari(self.node_info.node_id, n_dash_found.node_info.node_id)
            distance_data_id = ChordUtil.calc_distance_between_nodes_right_mawari(self.node_info.node_id, id)
            if distance_found < distance_old and not (distance_old >= distance_data_id):
                # 探索を続けていくと n_dash は id に近付いていくはずであり、それは上記の前提を踏まえると
                # 自ノードからはより遠い位置の値になっていくということのはずである
                # 従って、そうなっていなかった場合は、繰り返しを継続しても意味が無く、最悪、無限ループになってしまう
                # 可能性があるため、探索を打ち切り、探索結果は古いn_dashを返す
                # ただし、古い n_dash が 一回目の探索の場合 self であり、同じ node_idの距離は ID_SPACE_RANGE となるようにしている
                # ため、上記の条件が常に成り立ってしまう. 従って、その場合は例外とする（n_dashが更新される場合は、更新されたn_dashのnode_idが
                # 探索対象のデータのid を通り越すことは無い）

                ChordUtil.dprint("find_predecessor_4," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(n_dash.node_info))

                return n_dash

            ChordUtil.dprint("find_predecessor_5_n_dash_updated," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                             + ChordUtil.gen_debug_str_of_node(n_dash.node_info) + "->"
                             + ChordUtil.gen_debug_str_of_node(n_dash_found.node_info))

            # チェックの結果問題ないので n_dashを closest_preceding_fingerで探索して得た
            # ノード情報 n_dash_foundに置き換える
            n_dash = n_dash_found

        return n_dash

    #  自身の持つ経路情報をもとに,  id から前方向に一番近いノードの情報を返す
    def closest_preceding_finger(self, id : int) -> 'ChordNode':
        # 範囲の広いエントリから探索していく
        # finger_tableはインデックスが小さい方から大きい方に、範囲が大きくなっていく
        # ように構成されているため、リバースしてインデックスの大きな方から小さい方へ
        # 順に見ていくようにする
        for node_info in reversed(self.node_info.finger_table):
            # 埋まっていないエントリも存在し得る
            if node_info == None:
                ChordUtil.dprint("closest_preceding_finger_0," + ChordUtil.gen_debug_str_of_node(self.node_info))
                continue

            casted_node_info = cast(NodeInfo, node_info)

            ChordUtil.dprint("closest_preceding_finger_1," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                  + ChordUtil.gen_debug_str_of_node(casted_node_info))

            # テーブル内のエントリが保持しているノードのIDが自身のIDと探索対象のIDの間にあれば
            # それを返す
            # (大きな範囲を見た場合、探索対象のIDが自身のIDとエントリが保持しているノードのIDの中に含まれて
            #  しまっている可能性が高く、エントリが保持しているノードが、探索対象のIDを飛び越してしまっている
            #  可能性が高いということになる。そこで探索範囲を狭めていって、飛び越さない範囲で一番近いノードを
            #  見つけるという処理になっていると思われる）
            # #if self.node_info.node_id < entry.node_id and entry.node_id <= id:
            if ChordUtil.exist_between_two_nodes_right_mawari(self.node_info.node_id, id, casted_node_info.node_id):
                ChordUtil.dprint("closest_preceding_finger_2," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(casted_node_info))
                try:
                    return ChordUtil.get_node_by_address(casted_node_info.address_str)
                except NodeIsDownedExceptiopn:
                    continue

        ChordUtil.dprint("closest_preceding_finger_3")

        # どんなに範囲を狭めても探索対象のIDを超えてしまうノードしか存在しなかった場合
        # 自身の知っている情報の中で対象を飛び越さない範囲で一番近いノードは自身という
        # ことになる
        return self

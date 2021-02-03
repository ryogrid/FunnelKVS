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

    # TODO: 経路表データに対してwriteロックをとっていないといけないと思われる
    #       constructor of ChordNode

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
            self.stabilizer.join(node_address)

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

        success = target_node.put(data_id, value_str)
        if not success:
            ChordNode.need_put_retry_data_id = data_id
            ChordNode.need_put_retry_node = self
            ChordUtil.dprint("global_put_2,RETRY_IS_NEEDED" + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                             + ChordUtil.gen_debug_str_of_data(data_id))
            return False

        ChordUtil.dprint("global_put_3," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(target_node.node_info) + ","
                         + ChordUtil.gen_debug_str_of_data(data_id))

        return True

    def put(self, data_id : int, value_str : str) -> bool:
        ChordUtil.dprint("put_0," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_data(data_id))

        # 担当範囲（predecessorのidと自身のidの間）のデータであるかのチェック処理を加える
        # そこに収まっていなかった場合、一定時間後リトライが行われるようエラーを返す.
        # (predecessorの生死をチェックし、生きていればエラーとし、ダウンしていたら担当である
        #  とも、そうでないとも確定しないため、リクエストを受けるという実装も可能だが、stabilize処理
        #  で predecessor が生きているノードとなるまで下手にデータを持たない方が、データ配置の整合性
        #  を壊すリスクが減りそうな気がするので、そうする)
        if self.node_info.predecessor_info == None:
            return False
        # Chordネットワークを右回りにたどった時に、データの id (data_id) が predecessor の node_id から
        # 自身の node_id の間に位置する場合は、そのデータは自身の担当だが、そうではない場合
        if not ChordUtil.exist_between_two_nodes_right_mawari(cast(NodeInfo,self.node_info.predecessor_info).node_id, self.node_info.node_id, data_id):
            return False

        self.data_store.store_new_data(data_id, value_str)

        # レプリカを successorList内のノードに渡す
        # なお、新規ノードのjoin時のレプリカのコピーにおいて、predecessorのさらに前に位置するノードが
        # 担当するデータのレプリカは考慮されないため、successorList内のノードで自身の保持データのレプリカ
        # 全てを保持していないノードが存在する場合があるため、receive_replicaメソッド呼び出し時に返ってくる
        # レプリカの保持数が、全件となっていない場合は全て保持させる
        # TODO: successor_info_listのreadロックをとっておく必要あり
        #       on put
        for succ_info in self.node_info.successor_info_list:
            try:
                succ_node : ChordNode = ChordUtil.get_node_by_address(succ_info.address_str)
            except NodeIsDownedExceptiopn:
                # stabilize処理 と put処理 を経ていずれ正常な状態に
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

        return True

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
                    # TODO: predecessor_infoへのreadロックを取得しておく必要あり
                    #       on global_get
                    cur_predecessor = ChordUtil.get_node_by_address(cast(NodeInfo,cur_predecessor.node_info.predecessor_info).address_str)
                except NodeIsDownedExceptiopn:
                    # ここでは何も対処はしない
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
                    # TODO: successor_info_listのreadロックをとっておく必要あり
                    #       on global_get
                    cur_successor = ChordUtil.get_node_by_address(cast(NodeInfo,cur_successor.node_info.successor_info_list[0]).address_str)
                except NodeIsDownedExceptiopn:
                    # ここでは何も対処はしない
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


        if sv_entry.master_info.node_info.node_id == self.node_info.node_id:
            # 自ノードが担当ノード（マスター）のデータであった
            ret_value_str = sv_entry.value_data
            ChordUtil.dprint("get_2," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                             + ChordUtil.gen_debug_str_of_node(sv_entry.master_info.node_info) + ","
                             + ChordUtil.gen_debug_str_of_data(data_id) + "," + ret_value_str)
        else:
            # get要求に応じたデータを参照した際に自身が担当でないノードであった場合は、
            # 担当ノードの生死をチェックし、生きていれば QUERIED_DATA_NOT_FOUND_STR
            # を返し、ダウンしていた場合は、自身がダウンしていた担当ノードに成り代わる.
            # 具体的には、以下を行った上で、保持しているデータを返す
            #   - 自身の保持しているデータに紐づいている担当ノードの情報を更新する
            #   - 自身のsuccessorList内のノードに担当ノードの変更を通知する（データの紐づけを変えさせる）
            #     notify_master_node_changeメソッドを利用する
            #  通常、担当が切り替わった場合、レプリカの保有ノード数が規定数より少なくなってしまうが、
            #  少なくとも次に新たなデータの put を受けた際に、不足状態が解消される処理が走るため
            #  ここでは、レプリカの保持ノードを増やすような処理は行わない

            ret_value_str = self.QUERIED_DATA_NOT_FOUND_STR

            if ChordUtil.is_node_alive(sv_entry.master_info.node_info.address_str):
                # データの担当ノードであるマスターが生きていた.
                # 自身はレプリカを保持しているが、取得先が誤っているためエラーとして扱う.
                # (返してしまってもほとんどの場合で問題はないが、マスターに put や delete などの更新リクエストが
                #  かかっていた場合、タイミングによってデータの不整合が起きてしまう)
                ret_value_str = self.QUERIED_DATA_NOT_FOUND_STR
                ChordUtil.dprint("get_3," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(sv_entry.master_info.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_data(data_id) + "," + ret_value_str)
            else:
                # データの担当ノードであるマスターがダウンしていた.
                # 自身の保持しているデータに紐づいている担当ノードの情報を更新する
                self.data_store.notify_master_node_change(sv_entry.master_info.node_info, self.node_info)

                # TODO: successor_info_listへのreadロックをとっておく必要あり
                #       on get
                # 自身のsuccessorList内のノードに担当ノードの変更を通知する
                for node_info in self.node_info.successor_info_list:
                    try:
                        node_obj : ChordNode = ChordUtil.get_node_by_address(node_info.address_str)
                        node_obj.data_store.notify_master_node_change(sv_entry.master_info.node_info, self.node_info)
                        ChordUtil.dprint("get_4," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                                         + ChordUtil.gen_debug_str_of_node(sv_entry.master_info.node_info) + ","
                                         + ChordUtil.gen_debug_str_of_data(data_id))
                    except NodeIsDownedExceptiopn:
                        # ノードがダウンしていた場合は無視して次のノードに進む
                        # ノードダウンに関する対処は stabilize処理の中で後ほど行われるためここでは
                        # 何もしない
                        ChordUtil.dprint("get_5," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                                         + ChordUtil.gen_debug_str_of_node(sv_entry.master_info.node_info) + ","
                                         + ChordUtil.gen_debug_str_of_data(data_id))
                        continue

                ret_value_str = sv_entry.value_data


        ChordUtil.dprint("get_6," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_data(data_id) + "," + ret_value_str)

        return ret_value_str

    # TODO: Deleteの実装
    # def global_delete(self, key_str):
    #     print("not implemented yet")
    #
    # def delete(self, key_str):
    #     print("not implemented yet")

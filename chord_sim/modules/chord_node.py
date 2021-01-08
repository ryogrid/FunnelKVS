# coding:utf-8

from typing import Dict, List, Optional, cast

import modules.gval as gval
from .node_info import NodeInfo
from .chord_util import ChordUtil, KeyValue, NodeIsDownedExectiopn

class ChordNode:
    QUERIED_DATA_NOT_FOUND_STR = "QUERIED_DATA_WAS_NOT_FOUND"

    # global_get内で探索した担当ノードにgetをかけて、データを持っていないと
    # レスポンスがあった際に、持っていないか辿っていくノードの一方向における上限数
    GLOBAL_GET_NEAR_NODES_TRY_MAX_NODES = 5

    # 取得が NOT_FOUNDになった場合はこのクラス変数に格納して次のget処理の際にリトライさせる
    # なお、このシミュレータの実装上、このフィールドは一つのデータだけ保持できれば良い
    need_getting_retry_data_id : int = -1
    need_getting_retry_node : Optional['ChordNode'] = None

    # join処理もコンストラクタで行ってしまう
    def __init__(self, node_address: str, first_node=False):
        self.node_info = NodeInfo()
        # KeyもValueもどちらも文字列. Keyはハッシュを通されたものなので元データの値とは異なる
        self.stored_data : Dict[str, str] = {}

        # ミリ秒精度のUNIXTIMEから自身のアドレスにあたる文字列と、Chorネットワーク上でのIDを決定する
        self.node_info.address_str = ChordUtil.gen_address_str()
        self.node_info.node_id = ChordUtil.hash_str_to_int(self.node_info.address_str)

        gval.already_born_node_num += 1
        self.node_info.born_id = gval.already_born_node_num

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

    # node_addressに対応するノードに問い合わせを行い、教えてもらったノードをsuccessorとして設定する
    def join(self, node_address : str):
        # TODO: あとで、ちゃんとノードに定義されたAPIを介して情報をやりとりするようにする必要あり

        # TODO: join時にsuccessorListを埋めておくようにする. また、レプリケーションデータの受け取りも行う

        # TODO: 実装上例外は発生しない. また実システムでもダウンしているノードの情報が与えられることは
        #       想定しない
        tyukai_node = ChordUtil.get_node_by_address(node_address)
        # 仲介ノードに自身のsuccessorになるべきノードを探してもらう
        # TODO: 例外発生時にリトライする
        successor = tyukai_node.search_node(self.node_info.node_id)
        self.node_info.successor_info_list.append(successor.node_info.get_partial_deepcopy())

        # successorから自身が担当することになるID範囲のデータの委譲を受け、格納する
        tantou_data_list : List[KeyValue] = successor.get_copies_of_my_tantou_data(self.node_info.node_id, False)
        for key_value in tantou_data_list:
            self.stored_data[str(key_value.data_id)] = key_value.value

        # finger_tableのインデックス0は必ずsuccessorになるはずなので、設定しておく
        # self.node_info.finger_table[0] = self.node_info.successor_info_list[0].get_partial_deepcopy()
        self.node_info.finger_table[0] = ChordUtil.get_deepcopy_of_successor_list(self.node_info.successor_info_list)

        if tyukai_node.node_info.node_id == tyukai_node.node_info.successor_info_list[0].node_id:
            # secondノードの場合の考慮 (仲介ノードは必ずfirst node)

            # 2ノードでsuccessorでもpredecessorでも、チェーン構造で正しい環が構成されるよう強制的に全て設定してしまう
            self.node_info.predecessor_info = tyukai_node.node_info.get_partial_deepcopy()
            tyukai_node.node_info.predecessor_info = self.node_info.get_partial_deepcopy()
            tyukai_node.node_info.successor_info_list[0] = self.node_info.get_partial_deepcopy()
            # fingerテーブルの0番エントリも強制的に設定する
            tyukai_node.node_info.finger_table[0] = [self.node_info.get_partial_deepcopy()]
        else:
            # 強制的に自身を既存のチェーンに挿入する
            # successorは predecessorの 情報を必ず持っていることを前提とする
            self.node_info.predecessor_info = cast(NodeInfo, successor.node_info.predecessor_info).get_partial_deepcopy()
            successor.node_info.predecessor_info = self.node_info.get_partial_deepcopy()
            # TODO: 例外発生時は取得を試みたノードはダウンしているが、無視してpredecessorに設定する.
            #       不正な状態に一時的になるが、predecessorをsuccessor_info_listに持つノードが
            #       stabilize_successorを実行した時点で解消されるはず
            predecessor = ChordUtil.get_node_by_address(cast(NodeInfo, self.node_info.predecessor_info).address_str)
            predecessor.node_info.successor_info_list[0] = self.node_info.get_partial_deepcopy()

        # 自ノードの情報、仲介ノードの情報、successorとして設定したノードの情報
        ChordUtil.dprint("join," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(tyukai_node.node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(self.node_info.successor_info_list[0]))

    def global_put(self, data_id : int, value_str : str):
        # TODO: 例外発生時にリトライする
        target_node = self.search_node(data_id)
        target_node.put(data_id, value_str)
        ChordUtil.dprint("global_put_2," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(target_node.node_info) + ","
                         + ChordUtil.gen_debug_str_of_data(data_id))

    def put(self, data_id : int, value_str : str):
        key_id_str = str(data_id)
        self.stored_data[key_id_str] = value_str
        ChordUtil.dprint("put," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_data(data_id))

    # 得られた value の文字列を返す
    def global_get(self, data_id : int) -> str:
        ChordUtil.dprint("global_get_0," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_data(data_id))

        # TODO: 例外発生時にリトライする
        target_node = self.search_node(data_id)
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
                # TODO: 例外発生時はbreakする
                cur_predecessor = ChordUtil.get_node_by_address(cast(NodeInfo,cur_predecessor.node_info.predecessor_info).address_str)
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
                # TODO: 例外発生時はbreakする
                cur_successor = ChordUtil.get_node_by_address(cast(NodeInfo,cur_successor.node_info.successor_info_list[0]).address_str)
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
                ChordUtil.dprint("global_get_2_5,retry success")
                # リトライは不要なためクリア
                ChordNode.need_getting_retry_data_id = -1
                ChordNode.need_getting_retry_node = None
            else:
                # リトライに失敗した（何もしない）
                ChordUtil.dprint("global_get_2_5,retry failed")
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
        ret_value_str = None
        try:
            ret_value_str = self.stored_data[str(data_id)]
        except:
            ret_value_str = ChordNode.QUERIED_DATA_NOT_FOUND_STR

        ChordUtil.dprint("get," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_data(data_id) + "," + ret_value_str)
        return ret_value_str

    # some_id をChordネットワーク上で担当するノードを返す
    # Attention: ノード探索を行ったが見つかったノードがダウンしていたか何かでアクセス不能
    #            であった場合は NodeIsDownedException を raise する
    def search_node(self, some_id : int) -> 'ChordNode':
        ChordUtil.dprint("search_node_0," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_data(some_id))

        found_node = self.find_successor(some_id)
        if found_node == None:
            # TODO: ノード探索が失敗した場合は、一定時間を空けてリトライするようにする
            ChordUtil.dprint("search_node_1," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                             + ChordUtil.gen_debug_str_of_data(some_id))
            raise NodeIsDownedExectiopn()

        found_node = cast(ChordNode, found_node)
        ChordUtil.dprint("search_node_2," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(found_node.node_info) + ","
                         + ChordUtil.gen_debug_str_of_data(some_id))

        return found_node

    # TODO: Deleteの実装
    # def global_delete(self, key_str):
    #     print("not implemented yet")
    #
    # def delete(self, key_str):
    #     print("not implemented yet")

    # 自身が保持しているデータを一部取り除いて返す.
    # 取り除くデータは時計周りに辿った際に 引数 node_id と 自身の node_id
    # の間に data_id が位置するデータである.
    # join呼び出し時、新たに参加してきた新規ノードに、successorとなる自身が、担当から外れる
    # 範囲のデータの委譲を行うために、新規ノードから呼び出される形で用いられる.
    # rest_copy引数によってコピーを渡すだけか、完全に委譲してしまい自身のデータストアからは渡したデータを削除
    # するかどうか選択できる
    def get_copies_of_my_tantou_data(self, node_id : int, rest_copy : bool = True) -> List[KeyValue]:
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

        # データを委譲する際に元々持っていたノードからは削除するよう指定されていた場合
        if rest_copy == False:
            for kv in ret_datas:
                del self.stored_data[str(kv.data_id)]

        return ret_datas

    # id が自身の正しい predecessor でないかチェックし、そうであった場合、経路表の情報を更新する
    # 本メソッドはstabilize処理の中で用いられる
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
            if ChordUtil.is_node_alive(self.node_info.successor_info_list[idx].address_str):
                successor_tmp = ChordUtil.get_node_by_address(self.node_info.successor_info_list[idx].address_str)
                break
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
                    self.node_info.successor_info_list[0] = new_successor.node_info.get_partial_deepcopy()

                    # 新たなsuccessorに対して自身がpredecessorでないか確認を要請し必要であれ
                    # ば情報を更新してもらう
                    new_successor.check_predecessor(self.node_info.node_id, self.node_info)

                    ChordUtil.dprint("stabilize_successor_inner_4," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                                     + ChordUtil.gen_debug_str_of_node(self.node_info.successor_info_list[0]) + ","
                                     + ChordUtil.gen_debug_str_of_node(new_successor.node_info))
                except NodeIsDownedExectiopn:
                    # 例外発生時は張り替えを中止する
                    #   - successorは変更しない
                    #   - この時点でのsuccessor[0]が認識するpredecessorを自身とする(successr[0]のcheck_predecessorを呼び出す)

                    # successor[0]の変更は行わず、ダウンしていたノードではなく自身をpredecessorとするよう(間接的に)要請する
                    successor.check_predecessor(self.node_info.node_id, self.node_info)
                    ChordUtil.dprint("stabilize_successor_inner_5," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                                     + ChordUtil.gen_debug_str_of_node(self.node_info.successor_info_list[0]) + ","
                                     + str(cast(NodeInfo, self.node_info.successor_info_list[0].predecessor_info).node_id))

        return self.node_info.successor_info_list[0].get_partial_deepcopy()


    # successorListに関するstabilize処理を行う
    # コメントにおいては、successorListの構造を意識した記述の場合、一番近いsuccessorを successor[0] と
    # 記述し、以降に位置するノードは近い順に successor[idx] と記述する
    def stabilize_successor(self):
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

        while len(self.node_info.successor_info_list) == gval.SUCCESSOR_LIST_NORMAL_LEN:
            cur_node_info : NodeInfo = cur_node.stabilize_successor_inner()
            ChordUtil.dprint("stabilize_successor_1," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                             + ChordUtil.gen_debug_str_of_node(cur_node_info))
            if cur_node_info.node_id == self.node_info.node_id:
                # Chordネットワークに (downしていない状態で) 存在するノード数が gval.SUCCESSOR_LIST_NORMAL_LEN
                # より多くない場合 successorをたどっていった結果、自ノードにたどり着いてしまうため、その場合は規定の
                # ノード数を満たしていないが、successor_info_list の更新処理は終了する
                ChordUtil.dprint("stabilize_successor_2," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(cur_node_info))
                break
            updated_list.append(cur_node_info)
            # この呼び出しで例外は発生しない
            cur_node = ChordUtil.get_node_by_address(cur_node_info)

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
        found_node = self.find_successor(update_id)
        # found_node = self.find_predecessor(update_id)
        if found_node == None:
            # 今回のエントリの更新はあきらめる
            ChordUtil.dprint("stabilize_finger_table_2," + ChordUtil.gen_debug_str_of_node(self.node_info))
            return

        # TODO: finger_tableのエントリを引数に、found_nodeの生死を確認し、found_nodeもしくは適切なノードから
        #       successorListを取得するメソッドを定義し、それを利用する形に置き換える必要あり
        # TODO: 今の設計だと、finger_tableの各エントリに複数ノードの情報を保持している意味がないのでは？
        #       そもそも、インデックス0以外を readで参照するコードが存在しないし。
        #       リトライさせたところで、途中で経由するノードのfinger_tableの情報にすぐにノードダウンの事実
        #       が反映されるかも怪しいし、finger_tableの各エントリを拡張した点を活用して、リトライを可能な限り
        #       避けるような設計にするのが良いのでは？
        self.node_info.finger_table[idx] = ChordUtil.get_deepcopy_of_successor_list([found_node.node_info])

        ChordUtil.dprint("stabilize_finger_table_3," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(found_node.node_info))

    # id（int）で識別されるデータを担当するノードの名前解決を行う
    def find_successor(self, id : int) -> Optional['ChordNode']:
        ChordUtil.dprint("find_successor_1," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
              + ChordUtil.gen_debug_str_of_data(id))

        n_dash = self.find_predecessor(id)
        if n_dash == None:
            ChordUtil.dprint("find_successor_2," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                             + ChordUtil.gen_debug_str_of_data(id))
            return None

        ChordUtil.dprint("find_successor_3," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(n_dash.node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(self.node_info.successor_info_list[0]) + ","
                         + ChordUtil.gen_debug_str_of_data(id))

        # TODO: 例外発生時はNoneを返す
        return ChordUtil.get_node_by_address(n_dash.node_info.successor_info_list[0].address_str)

    # id(int)　の前で一番近い位置に存在するノードを探索する
    def find_predecessor(self, id: int) -> 'ChordNode':
        ChordUtil.dprint("find_predecessor_1," + ChordUtil.gen_debug_str_of_node(self.node_info))

        n_dash = self
        # n_dash と n_dashのsuccessorの 間に id が位置するような n_dash を見つけたら、ループを終了し n_dash を return する
        while not ChordUtil.exist_between_two_nodes_right_mawari(cast(NodeInfo,n_dash.node_info).node_id, cast(NodeInfo, n_dash.node_info.successor_info_list[0]).node_id, id):
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
        for slist in reversed(self.node_info.finger_table):
            # 埋まっていないエントリも存在し得る
            if slist == None:
                ChordUtil.dprint("closest_preceding_finger_0," + ChordUtil.gen_debug_str_of_node(self.node_info))
                continue

            casted_slist = cast(List[NodeInfo], slist)

            ChordUtil.dprint("closest_preceding_finger_1," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                  + ChordUtil.gen_debug_str_of_node(casted_slist[0]))

            # テーブル内のエントリが保持しているノードのIDが自身のIDと探索対象のIDの間にあれば
            # それを返す
            # (大きな範囲を見た場合、探索対象のIDが自身のIDとエントリが保持しているノードのIDの中に含まれて
            #  しまっている可能性が高く、エントリが保持しているノードが、探索対象のIDを飛び越してしまっている
            #  可能性が高いということになる。そこで探索範囲を狭めていって、飛び越さない範囲で一番近いノードを
            #  見つけるという処理になっていると思われる）
            # #if self.node_info.node_id < entry.node_id and entry.node_id <= id:
            if ChordUtil.exist_between_two_nodes_right_mawari(self.node_info.node_id, id, casted_slist[0].node_id):
                ChordUtil.dprint("closest_preceding_finger_2," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(casted_slist[0]))
                # TODO: 例外発生時はcontinueしてしまってよい
                return ChordUtil.get_node_by_address(casted_slist[0].address_str)

        ChordUtil.dprint("closest_preceding_finger_3")

        # どんなに範囲を狭めても探索対象のIDを超えてしまうノードしか存在しなかった場合
        # 自身の知っている情報の中で対象を飛び越さない範囲で一番近いノードは自身という
        # ことになる
        return self

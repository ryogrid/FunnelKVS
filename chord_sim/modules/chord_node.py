# coding:utf-8

from typing import Dict, List, Any, Optional, cast

import modules.gval as gval
from .node_info import NodeInfo
from .chord_util import ChordUtil, KeyValue

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

            # successorは自身として終了する
            self.node_info.successor_info_list.append(self.node_info)
            # 最初の1ノードなので、joinメソッド内で行われるsuccessor からの
            # データの委譲は必要ない

            # joinの処理の中でsuccessorをfinger_tableのインデックス0に設定する
            # が、first nodeは stabilize_successorでsuccessorを張り替える
            # 際にその処理を行う
            return
        else:
            self.join(node_address)

    # node_addressに対応するノードに問い合わせを行い、教えてもらったノードをsuccessorとして設定する
    def join(self, node_address : str):
        # TODO: あとで、ちゃんとノードに定義されたAPIを介して情報をやりとりするようにする必要あり

        tyukai_node = ChordUtil.get_node_by_address(node_address)
        # 仲介ノードに自身のsuccessorになるべきノードを探してもらう
        successor = tyukai_node.search_node(self.node_info.node_id)
        self.node_info.successor_info_list.append(successor.node_info.get_partial_deepcopy())

        # successorから自身が担当することになるID範囲のデータの委譲を受け、格納する
        tantou_data_list : List[KeyValue] = successor.get_copies_of_my_tantou_data(self.node_info.node_id, False)
        for key_value in tantou_data_list:
            self.stored_data[str(key_value.data_id)] = key_value.value

        # finger_tableのインデックス0は必ずsuccessorになるはずなので、設定しておく
        self.node_info.finger_table[0] = self.node_info.successor_info_list[0].get_partial_deepcopy()

        # successorから見たpredecessorおよび、自身から見たpredecessorの情報は
        # このタイミングで更新可能なはずなのでここで一度stabilize_successorを呼び出してしまう
        self.stabilize_successor()

        # 上記のstabilize_successorの呼び出しにより、successorが元々 predecessor の情報を保持
        # していた場合は、自身にそのノードが predecessor として設定されているはず
        # そして、その場合、自身の predecessor には自身を successorとして認識してもらわないと困る
        # のでそこの確認処理を行わせる
        if self.node_info.predecessor_info != None:
            predecessor_node = ChordUtil.get_node_by_address(cast(NodeInfo, self.node_info.predecessor_info).address_str)
            predecessor_node.stabilize_successor()

        # 自ノードの情報、仲介ノードの情報、successorとして設定したノードの情報
        ChordUtil.dprint("join," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(tyukai_node.node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(self.node_info.successor_info_list[0]))

    def global_put(self, data_id : int, value_str : str):
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
    def global_get(self, data_id : int):
        ChordUtil.dprint("global_get_0," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_data(data_id))

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
                    break

        # 返ってきた値が ChordNode.QUERIED_DATA_NOT_FOUND_STR だった場合、target_nodeから
        # 一定数の successor を辿ってそれぞれにも data_id に対応するデータを持っていないか問い合わせるようにする
        if got_value_str == ChordNode.QUERIED_DATA_NOT_FOUND_STR:
            tried_node_num = 0
            # 最初は処理の都合上、最初にgetをかけたノードを設定する
            cur_successor = target_node
            while tried_node_num < ChordNode.GLOBAL_GET_NEAR_NODES_TRY_MAX_NODES:
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
                    break

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
    def search_node(self, some_id : int) -> 'ChordNode':
        ChordUtil.dprint("search_node_0," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_data(some_id))

        found_node = self.find_successor(some_id)
        if found_node == None:
            # TODO: ノード探索が失敗した場合は、一定時間を空けてリトライするようにする
            ChordUtil.dprint("search_node_1," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                             + ChordUtil.gen_debug_str_of_data(some_id))
            raise Exception("appropriate node was not found.")

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
    # 範囲のデータの委譲（ここではコピー）を行うために、新規ノードから呼び出される形で用いられる.
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
        if self.node_info.predecessor_info == None:
            # 未設定状態なので確認するまでもなく、predecessorらしいと判断し
            # 経路情報に設定し、処理を終了する
            self.node_info.predecessor_info = node_info.get_partial_deepcopy()
            ChordUtil.dprint("check_predecessor_1," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                  + ChordUtil.gen_debug_str_of_node(self.node_info.successor_info_list[0]) + ","
                  + ChordUtil.gen_debug_str_of_node(self.node_info.predecessor_info))

            return

        ChordUtil.dprint("check_predecessor_2," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
              + ChordUtil.gen_debug_str_of_node(self.node_info.successor_info_list[0]))

        distance_check = ChordUtil.calc_distance_between_nodes_left_mawari(self.node_info.node_id, id)
        distance_cur = ChordUtil.calc_distance_between_nodes_left_mawari(self.node_info.node_id, cast(NodeInfo,self.node_info.predecessor_info).node_id)

        # 確認を求められたノードの方が現在の predecessor より predecessorらしければ
        # 経路表の情報を更新する
        if distance_check < distance_cur:
            self.node_info.predecessor_info = node_info.get_partial_deepcopy()

            ChordUtil.dprint("check_predecessor_3," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                  + ChordUtil.gen_debug_str_of_node(self.node_info.successor_info_list[0]) + ","
                  + ChordUtil.gen_debug_str_of_node(self.node_info.predecessor_info))

    # successorおよびpredicessorに関するstabilize処理を行う
    # predecessorはこの呼び出しで初めて設定される
    def stabilize_successor(self):
        ChordUtil.dprint("stabilize_successor_1," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
              + ChordUtil.gen_debug_str_of_node(self.node_info.successor_info_list[0]))

        # firstノードに対する考慮（ノード作成時に自身をsuccesorに設定しているために自身だけ
        # でsuccessorチェーンのループを作ったままになってしまうことを回避する）
        if self.node_info.predecessor_info != None and (self.node_info.node_id == self.node_info.successor_info_list[0].node_id):
            ChordUtil.dprint("stabilize_successor_1_5," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                             + ChordUtil.gen_debug_str_of_node(self.node_info.successor_info_list[0]))
            # secondノードがjoin済みであれば、当該ノードのstabilize_successorによって
            # secondノードがpredecessorとして設定されているはずなので、succesorをそちら
            # に張り替える
            self.node_info.successor_info_list[0] = self.node_info.predecessor_info.get_partial_deepcopy()
            # finger_tableのインデックス0は必ずsuccessorになるはずなので、設定しておく
            self.node_info.finger_table[0] = self.node_info.successor_info_list[0].get_partial_deepcopy()

        # 自身のsuccessorに、当該ノードが認識しているpredecessorを訪ねる
        # 自身が保持している successor_infoのミュータブルなフィールドは最新の情報でない
        # 場合があるため、successorのChordNodeオブジェクトを引いて、そこから最新のnode_info
        # の参照を得る
        successor = ChordUtil.get_node_by_address(self.node_info.successor_info_list[0].address_str)
        successor_info = successor.node_info
        # successor_info = self.node_info.successor_info
        if successor_info.predecessor_info == None:
            # successor が predecessor を未設定であった場合は自身を predecessor として保持させて
            # 処理を終了する
            successor_info.predecessor_info = self.node_info.get_partial_deepcopy()

            ChordUtil.dprint("stabilize_successor_2," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                             + ChordUtil.gen_debug_str_of_node(self.node_info.successor_info_list[0]))
            return

        ChordUtil.dprint("stabilize_successor_3," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(self.node_info.successor_info_list[0]))

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

                self.node_info.successor_info_list[0] = successor_obj.node_info.predecessor_info.get_partial_deepcopy()

                # 新たなsuccessorに対して自身がpredecessorでないか確認を要請し必要であれ
                # ば情報を更新してもらう
                new_successor_obj = ChordUtil.get_node_by_address(self.node_info.successor_info_list[0].address_str)
                new_successor_obj.check_predecessor(self.node_info.node_id, self.node_info)

                ChordUtil.dprint("stabilize_successor_4," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(self.node_info.successor_info_list[0]) + ","
                                 + ChordUtil.gen_debug_str_of_node(new_successor_obj.node_info))

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
            ChordUtil.dprint("stabilize_finger_table_2," + ChordUtil.gen_debug_str_of_node(self.node_info))
            return

        self.node_info.finger_table[idx] = found_node.node_info.get_partial_deepcopy()

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

        return ChordUtil.get_node_by_address(n_dash.node_info.successor_info_list[0].address_str)

    # id(int)　の前で一番近い位置に存在するノードを探索する
    def find_predecessor(self, id: int) -> 'ChordNode':
        ChordUtil.dprint("find_predecessor_1," + ChordUtil.gen_debug_str_of_node(self.node_info))

        n_dash = self
        # n_dash と n_dashのsuccessorの 間に id が位置するような n_dash を見つけたら、ループを終了し n_dash を return する
        #while not (n_dash.node_info.predecessor_info.node_id < id and id <= n_dash.node_info.successor_info.node_id):
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
        for entry in reversed(self.node_info.finger_table):
            # 埋まっていないエントリも存在し得る
            if entry == None:
                ChordUtil.dprint("closest_preceding_finger_0," + ChordUtil.gen_debug_str_of_node(self.node_info))
                continue

            ChordUtil.dprint("closest_preceding_finger_1," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                  + ChordUtil.gen_debug_str_of_node(entry))

            # テーブル内のエントリが保持しているノードのIDが自身のIDと探索対象のIDの間にあれば
            # それを返す
            # (大きな範囲を見た場合、探索対象のIDが自身のIDとエントリが保持しているノードのIDの中に含まれて
            #  しまっている可能性が高く、エントリが保持しているノードが、探索対象のIDを飛び越してしまっている
            #  可能性が高いということになる。そこで探索範囲を狭めていって、飛び越さない範囲で一番近いノードを
            #  見つけるという処理になっていると思われる）
            # #if self.node_info.node_id < entry.node_id and entry.node_id <= id:
            if ChordUtil.exist_between_two_nodes_right_mawari(self.node_info.node_id, id, entry.node_id):
                ChordUtil.dprint("closest_preceding_finger_2," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(entry))
                return ChordUtil.get_node_by_address(entry.address_str)

        ChordUtil.dprint("closest_preceding_finger_3")

        # どんなに範囲を狭めても探索対象のIDを超えてしまうノードしか存在しなかった場合
        # 自身の知っている情報の中で対象を飛び越さない範囲で一番近いノードは自身という
        # ことになる
        return self

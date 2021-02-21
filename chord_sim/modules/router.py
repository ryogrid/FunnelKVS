# coding:utf-8

from typing import Dict, List, Optional, cast, TYPE_CHECKING

import modules.gval as gval
from .chord_util import ChordUtil, KeyValue, NodeIsDownedExceptiopn, AppropriateNodeNotFoundException, \
    InternalControlFlowException, StoredValueEntry, NodeInfoPointer, DataIdAndValue

if TYPE_CHECKING:
    from .node_info import NodeInfo
    from .chord_node import ChordNode

class Router:

    def __init__(self, existing_node : 'ChordNode'):
        self.existing_node : 'ChordNode' = existing_node

    # id（int）で識別されるデータを担当するノードの名前解決を行う
    # Attention: 適切な担当ノードを得ることができなかった場合、FindNodeFailedExceptionがraiseされる
    def find_successor(self, id : int) -> 'ChordNode':
        if self.existing_node.node_info.lock_of_succ_infos.acquire(timeout=gval.LOCK_ACQUIRE_TIMEOUT) == False:
            # 失敗させる
            raise AppropriateNodeNotFoundException()
        try:
            ChordUtil.dprint("find_successor_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                  + ChordUtil.gen_debug_str_of_data(id))

            n_dash = self.find_predecessor(id)
            if n_dash == None:
                ChordUtil.dprint("find_successor_2," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_data(id))
                raise AppropriateNodeNotFoundException()


            ChordUtil.dprint("find_successor_3," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + ChordUtil.gen_debug_str_of_node(n_dash.node_info) + ","
                             + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]) + ","
                             + ChordUtil.gen_debug_str_of_data(id))

            try:
                # 取得しようとしたノードがダウンしていた場合 NodeIsDownedException が raise される
                return ChordUtil.get_node_by_address(n_dash.node_info.successor_info_list[0].address_str)
            except NodeIsDownedExceptiopn:
                # ここでは何も対処しない
                ChordUtil.dprint("find_successor_4,FOUND_NODE_IS_DOWNED," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_data(id))
                raise AppropriateNodeNotFoundException()
        finally:
            self.existing_node.node_info.lock_of_succ_infos.release()

    # id(int)　の前で一番近い位置に存在するノードを探索する
    def find_predecessor(self, id: int) -> 'ChordNode':
        ChordUtil.dprint("find_predecessor_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info))

        n_dash : 'ChordNode' = self.existing_node

        if self.existing_node.node_info.lock_of_succ_infos.acquire(timeout=gval.LOCK_ACQUIRE_TIMEOUT) == False:
            # 最初の n_dash を返してしまい、find_predecessorは失敗したと判断させる
            return n_dash
        try:
            # n_dash と n_dashのsuccessorの 間に id が位置するような n_dash を見つけたら、ループを終了し n_dash を return する
            while not ChordUtil.exist_between_two_nodes_right_mawari(n_dash.node_info.node_id, n_dash.node_info.successor_info_list[0].node_id, id):
                ChordUtil.dprint("find_predecessor_2," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(n_dash.node_info))
                n_dash_found = n_dash.router.closest_preceding_finger(id)

                if n_dash_found.node_info.node_id == n_dash.node_info.node_id:
                    # 見つかったノードが、n_dash と同じで、変わらなかった場合
                    # 同じを経路表を用いて探索することになり、結果は同じになり無限ループと
                    # なってしまうため、探索は継続せず、探索結果として n_dash (= n_dash_found) を返す
                    ChordUtil.dprint("find_predecessor_3," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                     + ChordUtil.gen_debug_str_of_node(n_dash.node_info))
                    return n_dash_found

                # closelst_preceding_finger は id を通り越してしまったノードは返さない
                # という前提の元で以下のチェックを行う
                distance_old = ChordUtil.calc_distance_between_nodes_right_mawari(self.existing_node.node_info.node_id, n_dash.node_info.node_id)
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

                    ChordUtil.dprint("find_predecessor_4," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                     + ChordUtil.gen_debug_str_of_node(n_dash.node_info))

                    return n_dash

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
                try:
                    return ChordUtil.get_node_by_address(casted_node_info.address_str)
                except NodeIsDownedExceptiopn:
                    # ここでは何も対処しない
                    continue

        ChordUtil.dprint("closest_preceding_finger_3")

        # どんなに範囲を狭めても探索対象のIDを超えてしまうノードしか存在しなかった場合
        # 自身の知っている情報の中で対象を飛び越さない範囲で一番近いノードは自身という
        # ことになる
        return self.existing_node

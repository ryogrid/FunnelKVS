# coding:utf-8

from typing import Dict, List, Optional, cast, TYPE_CHECKING

import modules.gval as gval
from .chord_util import ChordUtil, KeyValue, NodeIsDownedExceptiopn, AppropriateNodeNotFoundException, \
    TargetNodeDoesNotExistException, StoredValueEntry, NodeInfoPointer, DataIdAndValue

if TYPE_CHECKING:
    from .node_info import NodeInfo
    from .chord_node import ChordNode

class Stabilizer:

    def __init__(self, existing_node : 'ChordNode'):
        self.existing_node : 'ChordNode' = existing_node

    # id が自身の正しい predecessor でないかチェックし、そうであった場合、経路表の情報を更新する
    # 本メソッドはstabilize処理の中で用いられる
    # Attention: TargetNodeDoesNotExistException を raiseする場合がある
    def check_predecessor(self, id : int, node_info : 'NodeInfo'):
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

    #  ノードダウンしておらず、チェーンの接続関係が正常 (predecessorの情報が適切でそのノードが生きている)
    #  なノードで、諸々の処理の結果、self の successor[0] となるべきノードであると確認されたノードを返す.
    #　注: この呼び出しにより、self.existing_node.node_info.successor_info_list[0] は更新される
    #  規約: 呼び出し元は、selfが生きていることを確認した上で本メソッドを呼び出さなければならない
    def stabilize_successor_inner(self) -> 'NodeInfo':
        # 本メソッド呼び出しでsuccessorとして扱うノードはsuccessorListの先頭から生きているもの
        # をサーチし、発見したノードとする.
        ChordUtil.dprint("stabilize_successor_inner_0," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info))

        successor : 'ChordNode'
        successor_tmp : Optional['ChordNode'] = None
        for idx in range(len(self.existing_node.node_info.successor_info_list)):
            try:
                if ChordUtil.is_node_alive(self.existing_node.node_info.successor_info_list[idx].address_str):
                    successor_tmp = ChordUtil.get_node_by_address(self.existing_node.node_info.successor_info_list[idx].address_str)
                    break
                else:
                    ChordUtil.dprint("stabilize_successor_inner_1,SUCCESSOR_IS_DOWNED,"
                                     + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                     + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[idx]))
            except TargetNodeDoesNotExistException:
                # joinの中から呼び出された際に、successorを辿って行った結果、一周してjoin処理中のノードを get_node_by_addressしようと
                # した際に発生してしまうので、ここで対処する
                # join処理中のノードのpredecessor, sucessorはjoin処理の中で適切に設定されているはずなので、後続の処理を行わず successor[0]を返す
                return self.existing_node.node_info.successor_info_list[0].get_partial_deepcopy()

        if successor_tmp != None:
            successor = cast('ChordNode', successor_tmp)
        else:
            # successorListの全てのノードを当たっても、生きているノードが存在しなかった場合
            # 起きてはいけない状況なので例外を投げてプログラムを終了させる
            raise Exception("Maybe some parameters related to fault-tolerance of Chord network are not appropriate")

        # 生存が確認されたノードを successor[0] として設定する
        self.existing_node.node_info.successor_info_list[0] = successor.node_info.get_partial_deepcopy()

        ChordUtil.dprint("stabilize_successor_inner_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]))

        pred_id_of_successor = cast('NodeInfo', successor.node_info.predecessor_info).node_id

        ChordUtil.dprint("stabilize_successor_inner_2," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]) + ","
                         + str(pred_id_of_successor))

        # successorに認識している predecessor の情報をチェックさせて、適切なものに変更させたり、把握していない
        # 自身のsuccessor[0]になるべきノードの存在が判明した場合は 自身の successor[0]をそちらに張り替える.
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

                        # TODO: 新たなsuccesorに対して担当データのレプリカを渡し、successorListから溢れたノードには
                        #       レプリカを削除させる
                        #       joinの中で行っている処理を参考に実装すれば良い
                        #       on stabilize_successor_inner

                        # 新たなsuccessorに対して自身がpredecessorでないか確認を要請し必要であれ
                        # ば情報を更新してもらう
                        new_successor.stabilizer.check_predecessor(self.existing_node.node_info.node_id, self.existing_node.node_info)

                        ChordUtil.dprint("stabilize_successor_inner_4," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                         + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]) + ","
                                         + ChordUtil.gen_debug_str_of_node(new_successor.node_info))
                    except NodeIsDownedExceptiopn:
                        # 例外発生時は張り替えを中止する
                        #   - successorは変更しない
                        #   - この時点でのsuccessor[0]が認識するpredecessorを自身とする(successr[0]のcheck_predecessorを呼び出す)

                        # successor[0]の変更は行わず、ダウンしていたノードではなく自身をpredecessorとするよう(間接的に)要請する
                        successor.stabilizer.check_predecessor(self.existing_node.node_info.node_id, self.existing_node.node_info)
                        ChordUtil.dprint("stabilize_successor_inner_5," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                         + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]))

            except TargetNodeDoesNotExistException:
                # joinの中から呼び出された際に、successorを辿って行った結果、一周してjoin処理中のノードを get_node_by_addressしようと
                # した際にcheck_predecessorで発生する場合があるので、ここで対処する
                # join処理中のノードのpredecessor, sucessorはjoin処理の中で適切に設定されているはずなの特に処理は不要であり
                # 本メソッドは元々の successor[0] を返せばよい
                ChordUtil.dprint("stabilize_successor_inner_6," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.successor_info_list[0]))

        return self.existing_node.node_info.successor_info_list[0].get_partial_deepcopy()


    # successorListに関するstabilize処理を行う
    # コメントにおいては、successorListの構造を意識した記述の場合、一番近いsuccessorを successor[0] と
    # 記述し、以降に位置するノードは近い順に successor[idx] と記述する
    def stabilize_successor(self):
        # TODO: put時にレプリカを全て、もしくは一部持っていないノードについてはケアされる
        #       ため、大局的には問題ないと思われるが、ノードダウンを検出した場合や、未認識
        #       であったノードを発見した場合に、レプリカの配置状態が前述のケアでカバーできない
        #       ような状態とならないか確認する
        #       on stabilize_successor

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
            cur_node_info : 'NodeInfo' = cur_node.stabilizer.stabilize_successor_inner()
            ChordUtil.dprint("stabilize_successor_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + ChordUtil.gen_debug_str_of_node(cur_node_info))
            if cur_node_info.node_id == self.existing_node.node_info.node_id:
                # Chordネットワークに (downしていない状態で) 存在するノード数が gval.SUCCESSOR_LIST_NORMAL_LEN
                # より多くない場合 successorをたどっていった結果、自ノードにたどり着いてしまうため、その場合は規定の
                # ノード数を満たしていないが、successor_info_list の更新処理は終了する
                ChordUtil.dprint("stabilize_successor_2," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(cur_node_info))
                if len(updated_list) == 0:
                    # first node の場合の考慮
                    # second node が 未joinの場合、successsor[0] がリストに存在しない状態となってしまうため
                    # その場合のみ、update_list で self.existing_node.node_info.successor_info_listを上書きせずにreturnする
                    ChordUtil.dprint("stabilize_successor_2_5," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                     + ChordUtil.gen_debug_str_of_node(cur_node_info))
                    return

                break

            updated_list.append(cur_node_info)
            # この呼び出しで例外は発生しない
            cur_node = ChordUtil.get_node_by_address(cur_node_info.address_str)

        self.existing_node.node_info.successor_info_list = updated_list
        ChordUtil.dprint("stabilize_successor_3," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                     + str(self.existing_node.node_info.successor_info_list))

    # FingerTableに関するstabilize処理を行う
    # 一回の呼び出しで1エントリを更新する
    # FingerTableのエントリはこの呼び出しによって埋まっていく
    def stabilize_finger_table(self, idx):
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
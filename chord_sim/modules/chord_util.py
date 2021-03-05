# coding:utf-8

import sys
import time
import random
import datetime
import dataclasses
import traceback
from typing import List, Any, Optional, cast, TYPE_CHECKING

from . import gval

if TYPE_CHECKING:
    from .chord_node import ChordNode
    from .node_info import NodeInfo

class ChordUtil:
    # 任意の文字列をハッシュ値（定められたbit数で表現される整数値）に変換しint型で返す
    # アルゴリズムはSHA1, 160bitで表現される正の整数となる
    # メモ: 10進数の整数は組み込みの hex関数で 16進数表現での文字列に変換可能
    @classmethod
    def hash_str_to_int(cls, input_str : str) -> int:
        # hash_hex_str = hashlib.sha1(input_str.encode()).hexdigest()
        # hash_id_num = int(hash_hex_str, 16)

        # TODO: ID_SPACE_BITS ビットで表現できる符号なし整数をID空間とする.
        #       通常、ID_SPACE_BITS は sha1 で 160 となるが、この検証コードでは
        #       ハッシュ関数を用いなくても問題の起きない実装となっているため、より小さい
        #       ビット数で表現可能な IDスペース 内に収まる値を乱数で求めて返す
        hash_id_num = random.randint(0, gval.ID_SPACE_RANGE - 1)
        return hash_id_num

    # 与えたリストの要素のうち、ランダムに選択した1要素を返す
    @classmethod
    def get_random_elem(cls, list_like : List[Any]) -> Any:
        length = len(list_like)
        idx = random.randint(0, length - 1)
        return list_like[idx]

    # UNIXTIME（ミリ秒精度）にいくつか値を加算した値からアドレス文字列を生成する
    @classmethod
    def gen_address_str(cls) -> str:
        return str(time.time() + 10)

    # 計算したID値がID空間の最大値を超えていた場合は、空間内に収まる値に変換する
    @classmethod
    def overflow_check_and_conv(cls, id : int) -> int:
        ret_id = id
        if id > gval.ID_MAX:
            # 1を足すのは MAX より 1大きい値が 0 となるようにするため
            ret_id = id - (gval.ID_MAX + 1)
        return ret_id

    # idがID空間の最大値に対して何パーセントの位置かを適当な精度の浮動小数の文字列
    # にして返す
    @classmethod
    def conv_id_to_ratio_str(cls, id : int) -> str:
        ratio = (id / gval.ID_MAX) * 100.0
        return '%2.4f' % ratio

    # ID空間が環状になっていることを踏まえて base_id から前方をたどった場合の
    # ノード間の距離を求める
    # ここで前方とは、IDの値が小さくなる方向である
    @classmethod
    def calc_distance_between_nodes_left_mawari(cls, base_id : int, target_id : int) -> int:
        # successorが自分自身である場合に用いられる場合を考慮し、base_id と target_id が一致する場合は
        # 距離0と考えることもできるが、一周分を距離として返す
        if base_id == target_id:
            return gval.ID_SPACE_RANGE - 1

        # 0をまたいだ場合に考えやすくするためにtarget_idを0にずらし、base_idを
        # 同じ数だけずらす
        slided_target_id = 0
        slided_base_id = base_id - target_id
        if(slided_base_id < 0):
            # マイナスの値をとった場合は値0を通り越しているので
            # それにあった値に置き換える
            slided_base_id = gval.ID_MAX + slided_base_id

        # あとは差をとって、符号を逆転させる（前方は値が小さくなる方向を意味するため）
        distance = -1 * (slided_target_id - slided_base_id)

        # 求めた値が負の値の場合は入力された値において base_id < target_id
        # であった場合であり、前方をたどった場合の距離は ID_MAX から得られた値
        # の絶対値を引いたものであり、ここでは負の値となっているのでそのまま加算
        # すればよい
        if distance < 0:
            distance = gval.ID_MAX + distance

        return distance

    # ID空間が環状になっていることを踏まえて base_id から後方をたどった場合の
    # ノード間の距離を求める
    # ここで後方とは、IDの値が大きくなる方向である
    @classmethod
    def calc_distance_between_nodes_right_mawari(cls, base_id : int, target_id : int) -> int:
        # successorが自分自身である場合に用いられる場合を考慮し、base_id と target_id が一致する場合は
        # 距離0と考えることもできるが、一周分を距離として返す
        if base_id == target_id:
            return gval.ID_SPACE_RANGE - 1

        # 0をまたいだ場合に考えやすくするためにtarget_idを0にずらし、base_idを
        # 同じ数だけずらす
        slided_base_id = 0
        slided_target_id = target_id - base_id
        if(slided_target_id < 0):
            # マイナスの値をとった場合は値0を通り越しているので
            # それにあった値に置き換える
            slided_target_id = gval.ID_MAX + slided_target_id

        # あとは単純に差をとる
        distance = slided_target_id - slided_base_id

        # 求めた値が負の値の場合は入力された値において target_id < base_id
        # であった場合であり、前方をたどった場合の距離は ID_MAX から得られた値
        # の絶対値を引いたものであり、ここでは負の値となっているのでそのまま加算
        # すればよい
        if distance < 0:
            distance = gval.ID_MAX + distance

        return distance

    # from_id から IDが大きくなる方向にたどった場合に、 end_id との間に
    # target_idが存在するか否かを bool値で返す
    @classmethod
    def exist_between_two_nodes_right_mawari(cls, from_id : int, end_id : int, target_id : int) -> bool:
        distance_end = ChordUtil.calc_distance_between_nodes_right_mawari(from_id, end_id)
        distance_target = ChordUtil.calc_distance_between_nodes_right_mawari(from_id, target_id)

        if distance_target < distance_end:
            return True
        else:
            return False

    @classmethod
    def dprint(cls, print_str : str, flush=False):
        print(str(datetime.datetime.now()) + "," + print_str, flush=flush)

    @classmethod
    def print_no_lf(cls, print_str : str):
        print(print_str, end="")

    @classmethod
    def gen_debug_str_of_node(cls, node_info : Optional['NodeInfo']) -> str:
        casted_info : 'NodeInfo' = cast('NodeInfo', node_info)
        return str(casted_info.born_id) + "," + hex(casted_info.node_id) + "," \
               + ChordUtil.conv_id_to_ratio_str(casted_info.node_id)

    @classmethod
    def gen_debug_str_of_data(cls, data_id : int) -> str:
        return hex(data_id) + "," + ChordUtil.conv_id_to_ratio_str(data_id)

    # Attention: 取得しようとしたノードが all_node_dict に存在しないことは、そのノードが 離脱（ダウンしている状態も含）
    #            したことを意味するため、当該状態に対応する NodeIsDownedException 例外を raise する
    @classmethod
    def get_node_by_address(cls, address : str) -> 'ChordNode':
        try:
            ret_val = gval.all_node_dict[address]
        except KeyError:
            # join処理の途中で構築中のノード情報を取得しようとしてしまった場合に発生する
            # traceback.print_stack(file=sys.stdout)
            # print("KeyError occured", flush=True)
            raise InternalControlFlowException("accessed to join operation progressing node.")
        # except KeyError:
        #     traceback.print_stack(file=sys.stdout)
        #     print("KeyError occured", flush=True)
        #     sys.exit(1)

        if ret_val.is_alive == False:
            ChordUtil.dprint("get_node_by_address_1,NODE_IS_DOWNED," + ChordUtil.gen_debug_str_of_node(ret_val.node_info))
            raise NodeIsDownedExceptiopn()

        return ret_val

    # Attention: InternalControlFlowException を raiseする場合がある
    @classmethod
    def is_node_alive(cls, address : str) -> bool:
        try:
            node_obj = ChordUtil.get_node_by_address(address)
        except NodeIsDownedExceptiopn:
            return False

        return True

    # デバッグ用のメソッド
    # グローバル変数にあるデータIDに対応するデータがどのノードに存在するかを記録する
    # 本メソッドは新たにデータをstoreした際に呼び出す
    @classmethod
    def add_data_placement_info(cls, data_id : int, node_info : 'NodeInfo'):
        try:
            node_list : List['NodeInfo'] = gval.all_data_placement_dict[str(data_id)]
        except KeyError:
            node_list = []
            gval.all_data_placement_dict[str(data_id)] = node_list

        node_list.append(node_info)

    # デバッグ用のメソッド
    # グローバル変数にあるデータIDに対応するデータがどのノードに存在するかを記録する
    # 本メソッドはデータの削除が行われた際に呼び出す
    @classmethod
    def remove_data_placement_info(cls, data_id : int, node_info : 'NodeInfo'):
        try:
            node_list : List['NodeInfo'] = gval.all_data_placement_dict[str(data_id)]
        except KeyError:
            # 本来は起きてはならないエラーだが対処のし様もないのでワーニングを出力しておく
            ChordUtil.dprint("remove_data_1," + ChordUtil.gen_debug_str_of_node(node_info) + ","
                             + ChordUtil.gen_debug_str_of_data(data_id)
                             + ",WARNING__DATA_AND_BELONGS_NODE_RERATION_MAY_BE_BROKEN")
            return

        node_list.remove(node_info)

    # デバッグ用のメソッド
    # グローバル変数にあるデータIDに対応するデータがどのノードに存在するかを出力する
    # 本メソッドはデータの削除が行われた際に呼び出す
    @classmethod
    def print_data_placement_info(cls, data_id : int, after_notfound_limit = False):
        try:
            node_list : List['NodeInfo'] = gval.all_data_placement_dict[str(data_id)]
        except KeyError:
            # データを持っているノードがいないか、記録のバグ
            ChordUtil.dprint("print_data_placement_info_1,"
                             + ChordUtil.gen_debug_str_of_data(data_id)
                             + ",DATA_HAVING_NODE_DOES_NOT_EXIST_OR_INFORMATION_BUG")
            return

        if after_notfound_limit:
            additional_str = "NOT_FOUND_LIMIT_REACHED,"
        else:
            additional_str = ""

        # ロックをとっていないので面倒な処理が頭に入っている
        # なお、処理中に node_list の要素が増えた場合や出力済みのデータが削除された場合は
        # 表示に不整合が生じるが大きな問題ではない認識
        list_len = len(node_list)
        for idx in range(0, list_len):
            if idx < len(node_list):
                ChordUtil.dprint("print_data_placement_info_INFO," + additional_str
                                 + ChordUtil.gen_debug_str_of_data(data_id) + ","
                                 + ChordUtil.gen_debug_str_of_node(node_list[idx]))

    @classmethod
    def dprint_data_storage_operations(cls, callee_node : 'NodeInfo', master_node : 'NodeInfo', operation_type : str, data_id : int):
        ChordUtil.dprint("dprint_data_storage_operations," + ChordUtil.gen_debug_str_of_node(callee_node) + ","
                         + ChordUtil.gen_debug_str_of_node(master_node) + ","
                         + operation_type + "," + ChordUtil.gen_debug_str_of_data(data_id))

    @classmethod
    def dprint_routing_info(cls, callee_node : 'ChordNode', calee_method : str):
        ChordUtil.dprint("dprint_routing_info__PRED," + ChordUtil.gen_debug_str_of_node(callee_node.node_info) + ","
                         + calee_method + "," + "PREDECESSOR_INFO," + str(callee_node.node_info.predecessor_info))
        ChordUtil.dprint("dprint_routing_info__SUCC," +ChordUtil.gen_debug_str_of_node(callee_node.node_info) + "," + calee_method + ","
                         + "SUCCESSOR_INFO_LIST," + str(len(callee_node.node_info.successor_info_list)) + ","
                         + " | ".join([str(ninfo)  for ninfo in callee_node.node_info.successor_info_list]))

# 大量のオブジェクトが紐づくNodeInfoを一気に切り替えられるようにするため、間接的にNodeInfoを
# 保持するクラスとして用いる （Listなどを間に挟むことでも同じことは可能だが、可読性が低いので避ける）
class NodeInfoPointer:

    def __init__(self, node_info : 'NodeInfo'):
        self.node_info : NodeInfo = node_info

# all_data_listグローバル変数に格納される形式としてのみ用いる
class KeyValue:
    def __init__(self, key : Optional[str], value : str):
        self.key : Optional[str] = key
        self.value_data : str = value
        self.data_id : Optional[int] = None
        # keyのハッシュ値
        if key == None:
            self.data_id = None
        else:
            self.data_id = ChordUtil.hash_str_to_int(cast(str, key))

@dataclasses.dataclass
class DataIdAndValue:
    data_id : int
    value_data : str

@dataclasses.dataclass
class StoredValueEntry:
    master_info : NodeInfoPointer
    data_id : int
    value_data : str

    def __eq__(self, other):
        if not isinstance(other, StoredValueEntry):
            return False
        return self.data_id == other.data_id

class NodeIsDownedExceptiopn(Exception):

    def __init__(self):
        super(NodeIsDownedExceptiopn, self).__init__("Accessed node seems to be downed.")

class AppropriateNodeNotFoundException(Exception):

    def __init__(self):
        super(AppropriateNodeNotFoundException, self).__init__("Appropriate node is not found.")

# 通常、join時に all_node_dictにノードオブジェクトが登録される前に
# ノードのアドレスによる取得を試みた場合など、設計上起きてしまうことがある例外について総じて利用する
class InternalControlFlowException(Exception):

    def __init__(self, msg_str):
        super(InternalControlFlowException, self).__init__(msg_str)
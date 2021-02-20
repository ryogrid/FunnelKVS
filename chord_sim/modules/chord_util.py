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

    # # あるノードを経路表に入れる際に適切なエントリに入れるために
    # # 対応するインデックスを求めて返す
    # # 設定するインデックスは node_id より値が大きくてもっとも近いエントリの
    # # インデックスとなる
    # # 値が大きいエントリに入れるのは、担当ノードとしては、そのノードのsuccessorを
    # # 用いるためである
    # @classmethod
    # def calc_idx_of_ftable_from_node_id(cls, from_node_id : int, target_node_id : int) -> int:
    #     distance : int = ChordUtil.calc_distance_between_nodes_right_mawari(from_node_id, target_node_id)
    #     if distance == 0:
    #         # 同じノードを比較しており、2^0である 1 よりも距離が小さいので、finger_tableには入れない
    #         # ようにさせる
    #         return -1
    #
    #     log2_value = math.log2(distance)
    #     ceiled_value = math.floor(log2_value)
    #
    #     return ceiled_value - 1 # 0オリジンのため

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

# class StoredValueEntry:
#
#     def __init__(self, master_info : NodeInfoPointer, data_id : int, value_data : str):
#         self.master_info : NodeInfoPointer = master_info
#         self.data_id : int = data_id
#         self.value_data : str = value_data

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
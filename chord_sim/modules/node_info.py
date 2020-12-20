# coding:utf-8

import copy
from typing import List, Optional

from . import gval

class NodeInfo:

    def __init__(self):
        self.node_id: int = None
        self.address_str: str = None

        # デバッグ用のID（実システムには存在しない）
        # 何ノード目として生成されたかの値
        self.born_id: int = None

        # 以下の2つはNodeInfoオブジェクトを保持.
        # ある時点で取得したものが保持されており、変化する場合のあるフィールド
        # の内容は最新の内容となっているとは限らないため注意が必要.
        # そのような情報が必要な場合はChordNodeオブジェクトから参照し、
        # 必要であれば、その際に下のフィールドにdeepcopyを設定しなおさ
        # なければならない.

        # 状況に応じて伸縮するが、インデックス0には必ず 非None な要素が入っている
        # ように制御する
        self.successor_info_list: List[NodeInfo] = []
        # join後はNoneになることのないように制御される
        self.predecessor_info: Optional[NodeInfo] = None

        # NodeInfoオブジェクトを要素として持つリスト
        # インデックスの小さい方から狭い範囲が格納される形で保持する
        # sha1で生成されるハッシュ値は160bit符号無し整数であるため要素数は160となる

        # TODO: 現在は ID_SPACE_BITS が検証時の実行時間の短縮のため30となっている
        self.finger_table: List[Optional[List[NodeInfo]]] = [None] * gval.ID_SPACE_BITS

    # 単純にdeepcopyするとチェーン構造になっているものが全てコピーされてしまう
    # ため、そこの考慮を行い、また、finger_tableはコピーしない形での deepcopy
    # を返す.
    # 上述の考慮により、コピーした NodeInfoオブジェクト の successor_infoと
    # predecessor_infoは deepcopy の対象ではあるが、それらの中の同名のフィールド
    # にはNoneが設定される. これにより、あるノードがコピーされた NodeInfo を保持
    # した場合、predecessor や successorは辿ることができるが、その先は辿ることが
    # 直接的にはできないことになる（predecessor や successorの ChordNodeオブジェクト
    # を引いてやれば可能）
    # 用途としては、あるノードの node_info を他のノードが取得し保持する際に利用される
    # ことを想定して実装されている.
    def get_partial_deepcopy(self) -> 'NodeInfo':
        ret_node_info: NodeInfo = NodeInfo()

        ret_node_info.node_id = copy.copy(self.node_id)
        ret_node_info.address_str = copy.copy(self.address_str)
        ret_node_info.born_id = copy.copy(self.born_id)
        ret_node_info.successor_info_list = []
        ret_node_info.predecessor_info = None

        return ret_node_info

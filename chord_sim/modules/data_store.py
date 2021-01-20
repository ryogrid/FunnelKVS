# coding:utf-8

from typing import Dict, List, Optional, cast

from .node_info import NodeInfo
from .chord_node import ChordNode
from .chord_util import ChordUtil, KeyValue, NodeIsDownedExceptiopn, AppropriateNodeNotFoundException, \
    TargetNodeDoesNotExistException, StoredValueEntry, NodeInfoPointer, DataIdAndValue

class DataStore:

    def __init__(self, existing_node : ChordNode):
        self.existing_node : ChordNode = existing_node

        # Keyはハッシュを通されたものなので元データの値とは異なる
        self.stored_data : Dict[str, StoredValueEntry] = {}

        # 主担当ノードのNodeInfoオブジェクトから、そのノードが担当するデータを引くためのインデックス辞書.
        # 大半のkeyはレプリカを自身に保持させているノードとなるが、自ノードである場合も同じ枠組みで
        # 扱う.
        # つまり、レプリカでないデータについてもこのインデックス辞書は扱うことになる
        self.master2data_idx : Dict[NodeInfo, List[StoredValueEntry]] = {}

        # 保持してるデータが紐づいている主担当ノードの情報を保持するためのdict
        # ただし、主担当ノードが切り替わった場合に参照先を一つ切り替えるだけで関連する
        # 全データの紐づけが変更可能とするため、NodeInfoを指す（参照をフィールドに持つ）
        # NodeInfoPointerクラスを間に挟む形とし、StoredValueEntryでも当該クラスの
        # オブジェクト参照するようにしてある
        # キーはマスターノードのID文字列
        self.master_node_dict : Dict[str, NodeInfoPointer] = {}

    def store_new_data(self, data_id : int, value_str : str):
        key_id_str = str(data_id)
        #self.stored_data[key_id_str] = value_str
        try:
            ninfo_p = self.master_node_dict[key_id_str]
        except KeyError:
            ninfo_p = NodeInfoPointer(self.existing_node.node_info)
            self.master_node_dict[key_id_str] = ninfo_p

        sv_entry = StoredValueEntry(master_info=ninfo_p, data_id=data_id, value=value_str)
        self.stored_data[key_id_str] = sv_entry
        try:
            data_list : List[StoredValueEntry] = self.master2data_idx[self.existing_node.node_info]
        except KeyError:
            data_list = []
            self.master2data_idx[self.existing_node.node_info] = data_list

        data_list.append(sv_entry)

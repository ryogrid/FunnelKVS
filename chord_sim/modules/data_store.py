# coding:utf-8

from typing import Dict, List, Optional, cast, TYPE_CHECKING

from .chord_util import ChordUtil, KeyValue, NodeIsDownedExceptiopn, AppropriateNodeNotFoundException, \
    TargetNodeDoesNotExistException, StoredValueEntry, NodeInfoPointer, DataIdAndValue

if TYPE_CHECKING:
    from .chord_node import ChordNode
    from .node_info import NodeInfo

class DataStore:

    def __init__(self, existing_node : 'ChordNode'):
        self.existing_node : 'ChordNode' = existing_node

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

    # master_node引数を指定しなかった場合は、self.existing_node.node_info をデータのマスターの情報として格納する
    def store_new_data(self, data_id : int, value_str : str, master_info : Optional['NodeInfo'] = None):
        if master_info == None:
            master_node_info = self.existing_node.node_info
        else:
            master_node_info = cast(NodeInfo, master_info).get_partial_deepcopy()

        key_id_str = str(data_id)
        #self.stored_data[key_id_str] = value_str
        try:
            ninfo_p = self.master_node_dict[key_id_str]
        except KeyError:
            ninfo_p = NodeInfoPointer(master_node_info)
            self.master_node_dict[key_id_str] = ninfo_p

        sv_entry = StoredValueEntry(master_info=ninfo_p, data_id=data_id, value_data=value_str)
        self.stored_data[key_id_str] = sv_entry
        try:
            data_list : List[StoredValueEntry] = self.master2data_idx[master_node_info]
        except KeyError:
            data_list = []
            self.master2data_idx[master_node_info] = data_list

        data_list.append(sv_entry)

    # TODO: 保持しているレプリカを data_id の範囲を指定して削除させる.
    #       マスターノードの担当範囲の変更や、新規ノードのjoinにより、レプリカを保持させていた
    #       ノードの保持するデータに変更が生じたり、レプリケーションの対象から外れた場合に用いる.
    #       対象 data_id の範囲は [range_start, range_end) となり、両方を無指定とした場合は
    #       全範囲が対象となる
    #       delete_replica
    def delete_replica(self, master_node : 'NodeInfo', range_start : int = -1, range_end : int = -1):
        raise Exception("not implemented yet")

    # TODO: 自ノードが担当ノードとして保持しているデータを全て返す
    #       pass_tantou_data_for_replication
    def pass_tantou_data_for_replication(self) -> List[DataIdAndValue]:
        raise Exception("not implemented yet")

    # TODO: 自ノードが担当ノードとなっているものを除いて、保持しているデータをマスター
    #       ごとに dict に詰めて返す
    #       pass_all_replica
    def pass_all_replica(self) -> Dict['NodeInfo', List[DataIdAndValue]]:
        raise Exception("not implemented yet")

    # TODO: レプリカデータを呼び出し先ノードに受け取らせる
    #       他のノードが保持しておいて欲しいレプリカを渡す際に呼び出される.
    #       なお、master_node 引数と呼び出し元ノードは一致しない場合がある.
    #       返り値として、処理が完了した時点でmaster_nodeに紐づいているレプリカをいくつ保持して
    #       いるかを返す
    #       receive_replica
    def receive_replica(self, master_node : 'NodeInfo', pass_datas : List[DataIdAndValue]) -> int:
        raise Exception("not implemented yet")

    # TODO: レプリカに紐づけられているマスターノードが切り替わったことを通知し、管理情報を
    #       通知内容に応じて更新させる
    #       notify_master_node_change
    def notify_master_node_change(self, old_master : 'NodeInfo', new_master : 'NodeInfo'):
        raise Exception("not implemented yet")

    # 自身が保持しているデータのうち委譲するものを返す.
    # 対象となるデータは時計周りに辿った際に 引数 node_id と 自身の node_id
    # の間に data_id が位置するデータである.
    # join呼び出し時、新たに参加してきた新規ノードに、successorとなる自身が、担当から外れる
    # 範囲のデータの委譲を行うために、新規ノードから呼び出される形で用いられる.
    # rest_copy引数によってコピーを渡すだけか、完全に委譲してしまい自身のデータストアからは渡したデータを削除
    # するかどうか選択できる
    def delegate_my_tantou_data(self, node_id : int, rest_copy : bool = True) -> List[KeyValue]:
        #TODO: 新しいデータ保持の枠組みに対応させる
        #      on delegate_my_tantou_data

        ret_datas : List[KeyValue] = []
        for key, value in self.stored_data.items():
            data_id : int = int(key)

            # Chordネットワークを右回りにたどった時に、データの id (data_id) が呼び出し元の node_id から
            # 自身の node_id の間に位置する場合は、そのデータの担当は自身から変わらないため、渡すデータから
            # 除外する
            if ChordUtil.exist_between_two_nodes_right_mawari(node_id, self.existing_node.node_info.node_id, data_id):
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

    # 存在しないKeyが与えられた場合 KeyErrorがraiseされる
    def get(self, data_id : int) -> StoredValueEntry:
        return self.stored_data[str(data_id)]

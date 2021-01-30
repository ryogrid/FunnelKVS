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

        # 主担当ノードのnode_id文字列から、そのノードが担当するデータを引くためのインデックス辞書.
        # 大半のkeyはレプリカを自身に保持させているノードとなるが、自ノードである場合も同じ枠組みで
        # 扱う.
        # つまり、レプリカでないデータについてもこのインデックス辞書は扱うことになる
        self.master2data_idx : Dict[str, List[StoredValueEntry]] = {}

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
            master_node_info = cast('NodeInfo', master_info).get_partial_deepcopy()

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
            data_list : List[StoredValueEntry] = self.master2data_idx[str(master_node_info.node_id)]
        except KeyError:
            data_list = []
            self.master2data_idx[str(master_node_info.node_id)] = data_list

        data_list.append(sv_entry)

    # 保持しているレプリカを data_id の範囲を指定して削除させる.
    # マスターノードの担当範囲の変更や、新規ノードのjoinにより、レプリカを保持させていた
    # ノードの保持するデータに変更が生じたり、レプリケーションの対象から外れた場合に用いる.
    # 対象 data_id の範囲は (range_start, range_end) となり、両方を無指定とした場合は
    # 全範囲が対象となる
    # data_idの範囲はstartからendに向かう向きがChord空間上で右回りとなるよう指定する
    def delete_replica(self, master_node : 'NodeInfo', range_start : int = -1, range_end : int = -1):
        ChordUtil.dprint("delete_replica_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(master_node) + ","
                         + str(range_start) + "," + str(range_end))
        try:
            related_entries: List[StoredValueEntry] = self.master2data_idx[str(master_node.node_id)]
        except KeyError:
            ChordUtil.dprint("delete_replica_2," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + ChordUtil.gen_debug_str_of_node(master_node) + ","
                             + str(range_start) + "," + str(range_end))
            return

        # 範囲指定されていた場合は該当範囲に含まれるデータのみを削除する
        if range_start != -1 and range_end != -1:
            delete_entries : List[StoredValueEntry] = []
            for sv_entry in related_entries:
                if ChordUtil.exist_between_two_nodes_right_mawari(range_start, range_end, sv_entry.data_id):
                    delete_entries.append(sv_entry)
            ChordUtil.dprint("delete_replica_3," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + ChordUtil.gen_debug_str_of_node(master_node) + ","
                             + str(range_start) + "," + str(range_end) + "," + str(delete_entries))
            for sv_entry in delete_entries:
                related_entries.remove(sv_entry)

        # 全範囲の削除が指定されているか、範囲指定での削除の結果、指定されたマスターノードに紐づくデータが
        # 0件となった場合、当該ノードに関連する管理情報は不要であるため削除する
        try:
            if len(related_entries) == 0 or (range_start == -1 and range_end == -1):
                del self.master2data_idx[str(master_node.node_id)]
                del self.master_node_dict[str(master_node.node_id)]
                ChordUtil.dprint("delete_replica_4," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(master_node) + ","
                                 + str(range_start) + "," + str(range_end))
        except KeyError:
            ChordUtil.dprint("delete_replica_5,KeyError" + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + ChordUtil.gen_debug_str_of_node(master_node) + ","
                             + str(range_start) + "," + str(range_end))
            pass

    # 自ノードが担当ノードとして保持しているデータを全て返す
    def pass_tantou_data_for_replication(self) -> List[DataIdAndValue]:
        ChordUtil.dprint(
            "pass_tantou_data_for_replication_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info))
        try:
            tantou_data_list : List[StoredValueEntry] = self.master2data_idx[str(self.existing_node.node_info.node_id)]
            ChordUtil.dprint(
                "pass_tantou_data_for_replication_2," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                + str(len(tantou_data_list)))
            return [DataIdAndValue(data_id = data.data_id, value_data=data.value_data ) for data in tantou_data_list]
        except KeyError:
            # まだ一つもデータをputされたり、他ノードから委譲される、担当データを持っていたノードに代替として成り代わるといったことがなかった
            ChordUtil.dprint(
                "pass_tantou_data_for_replication_3," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                + "NO_TANDOU_DATA_YET")
            return []

    # 自ノードが担当ノードとなっているものを除いて、保持しているデータ（レプリカ）を マスターノード
    # ごとに dict に詰めて返す
    def pass_all_replica(self) -> Dict['NodeInfo', List[DataIdAndValue]]:
        ret_dict : Dict['NodeInfo', List[DataIdAndValue]] = {}
        for node_id_str, ninfo_p in self.master_node_dict.items():
            master_info : 'NodeInfo' = ninfo_p.node_info.get_partial_deepcopy()
            if node_id_str != str(self.existing_node.node_info.node_id):
                data_list = self.master2data_idx[node_id_str]
                ret_dict[master_info] = [DataIdAndValue(data_id = data.data_id, value_data=data.value_data ) for data in data_list]

        return ret_dict

    # レプリカデータを受け取る
    # 他のノードが、保持しておいて欲しいレプリカを渡す際に呼び出される.
    # なお、master_node 引数と呼び出し元ノードは一致しない場合がある.
    # replace_allオプション引数をTrueとした場合は、指定したノードのデータを丸っと入れ替える
    # 返り値として、処理が完了した時点でmaster_nodeに紐づいているレプリカをいくつ保持して
    # いるかを返す
    def receive_replica(self, master_node : 'NodeInfo', pass_datas : List[DataIdAndValue], replace_all = False) -> int:
        ChordUtil.dprint("receive_replica_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(master_node) + ","
                         + str(len(pass_datas)) + "," + str(replace_all))
        if replace_all:
            self.delete_replica(master_node)

        copied_master_node = master_node.get_partial_deepcopy()
        for id_value in pass_datas:
            self.store_new_data(id_value.data_id, id_value.value_data, master_info=copied_master_node)


        replica_cnt = self.get_replica_cnt_by_master_node(master_node.node_id)
        ChordUtil.dprint("receive_replica_2," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(master_node) + ","
                         + str(len(pass_datas)) + "," + str(replace_all) + "," + str(replica_cnt))
        return replica_cnt

    # 複数マスタのレプリカをまとめて受け取り格納する
    def store_replica_of_several_masters(self, data_dict: Dict['NodeInfo', List[DataIdAndValue]]):
        ChordUtil.dprint("store_replica_of_several_masters_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                         + str(len(data_dict)))

        for master_node, data_list in data_dict.items():
            copied_master_node = master_node.get_partial_deepcopy()
            ChordUtil.dprint("store_replica_of_several_masters_2,"
                             + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + ChordUtil.gen_debug_str_of_node(copied_master_node) + ","
                             + str(len(data_list)))
            for id_value in data_list:
                self.store_new_data(id_value.data_id, id_value.value_data, master_info=copied_master_node)

    # レプリカに紐づけられているマスターノードが切り替わったことを通知し、管理情報を
    # 通知内容に応じて更新させる
    def notify_master_node_change(self, old_master : 'NodeInfo', new_master : 'NodeInfo'):
        ChordUtil.dprint("notify_master_node_change_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(old_master) + ","
                         + ChordUtil.gen_debug_str_of_node(new_master))
        try:
            # 各データが保持しているマスタの情報への参照を更新する
            ninfo_p : NodeInfoPointer = self.master_node_dict[str(old_master.node_id)]
            ninfo_p.node_info = new_master.get_partial_deepcopy()

            ChordUtil.dprint(
                "notify_master_node_change_2," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                + ChordUtil.gen_debug_str_of_node(old_master) + ","
                + ChordUtil.gen_debug_str_of_node(new_master) + ","
                + ChordUtil.gen_debug_str_of_node(ninfo_p.node_info))

            # マスターノードのIDから、紐づいているデータのリストを得るための dict においても切り替えを行う
            data_list : List[StoredValueEntry] = self.master2data_idx[str(old_master.node_id)]
            del self.master2data_idx[str(old_master.node_id)]
            self.master2data_idx[str(new_master.node_id)] = data_list

            ChordUtil.dprint(
                "notify_master_node_change_3," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                + ChordUtil.gen_debug_str_of_node(old_master) + ","
                + ChordUtil.gen_debug_str_of_node(new_master) + ","
                + ChordUtil.gen_debug_str_of_node(ninfo_p.node_info) + "," + str(len(data_list)))
        except KeyError:
            # 指定されたマスターノードのデータは保持していなかったことを意味するので何もせずに終了する
            ChordUtil.dprint(
                "notify_master_node_change_4," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                + ChordUtil.gen_debug_str_of_node(old_master) + ","
                + ChordUtil.gen_debug_str_of_node(new_master))

    # 自身が保持しているデータのうち委譲するものを返す.
    # 対象となるデータは時計周りに辿った際に 引数 node_id と 自身の node_id
    # の間に data_id が位置するデータである.
    # join呼び出し時、新たに参加してきた新規ノードに、successorとなる自身が、担当から外れる
    # 範囲のデータの委譲を行うために、新規ノードから呼び出される形で用いられる.
    # rest_copy引数によってコピーを渡すだけか、完全に委譲してしまい自身のデータストアからは渡したデータを削除
    # するかどうか選択できる
    def delegate_my_tantou_data(self, node_id : int, rest_copy : bool = True) -> List[KeyValue]:
        ChordUtil.dprint("delegate_my_tantou_data_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                         + ChordUtil.gen_debug_str_of_data(node_id))
        ret_datas : List[KeyValue] = []
        for key, value in self.stored_data.items():
            data_id : int = int(key)
            # Chordネットワークを右回りにたどった時に、データの id (data_id) が呼び出し元の node_id から
            # 自身の node_id の間に位置する場合は、そのデータの担当は自身から変わらないため、渡すデータから
            # 除外する
            if ChordUtil.exist_between_two_nodes_right_mawari(node_id, self.existing_node.node_info.node_id, data_id):
                ChordUtil.dprint(
                    "delegate_my_tantou_data_2," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                    + ChordUtil.gen_debug_str_of_data(node_id) + "," + ChordUtil.gen_debug_str_of_data(data_id))
                continue

            # 文字列の参照をそのまま用いてしまうが、文字列はイミュータブルであるため
            # 問題ない
            item = KeyValue(None, value.value_data)
            item.data_id = data_id
            ret_datas.append(item)

        # データを委譲する際に元々持っていたノードから削除するよう指定されていた場合
        if rest_copy == False:
            for kv in ret_datas:
                del self.stored_data[str(kv.data_id)]

        # 委譲したことで自身が担当ノードで無くなったデータについてsuccessorList
        # 内のノードに通知し、削除させる（それらのノードは再度同じレプリカを保持する
        # ことになるかもしれないが、それは新担当の管轄なので、非効率ともなるがひとまず削除させる）
        # 削除が完了するまで本メソッドは終了しないため、新担当がレプリカを配布する処理と以下の処理が
        # バッティングすることはない
        for node_info in self.existing_node.node_info.successor_info_list:
            try:
                node : ChordNode = ChordUtil.get_node_by_address(node_info.address_str)
                # マスターノードが自ノードとして設定されているデータのうち自ノードが担当でなくなるデータを削除させる.
                # 少なくとも、自ノードが担当となる範囲以外は自身の担当でなくなるため、担当範囲以外全てを指定して要請する.
                # 始点・終点の指定としては、左周りで考えた時に自ノードから、委譲先ノードまでの範囲が、担当が自ノードから
                # 変化していないID範囲であることを踏まえると、Chordネットワークを右回りでたどった時に、自ノードから委譲
                # 先のノードに至るID範囲は自身が担当でない全てのID範囲と考えることができる
                node.data_store.delete_replica(self.existing_node.node_info, self.existing_node.node_info.node_id, node_id)
                ChordUtil.dprint(
                    "delegate_my_tantou_data_3," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                    + ChordUtil.gen_debug_str_of_data(node_id) + "," + ChordUtil.gen_debug_str_of_node(node.node_info))
            except NodeIsDownedExceptiopn:
                # stablize処理 がよろしくやってくれるのでここでは何もしない
                ChordUtil.dprint(
                    "delegate_my_tantou_data_4,NODE_IS_DOWNED" + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                    + ChordUtil.gen_debug_str_of_data(node_id) + "," + ChordUtil.gen_debug_str_of_node(node.node_info))
                continue

        return ret_datas

    # 存在しないKeyが与えられた場合 KeyErrorがraiseされる
    def get(self, data_id : int) -> StoredValueEntry:
        return self.stored_data[str(data_id)]

    def get_replica_cnt_by_master_node(self, node_id : int) -> int:
        try:
            replica_list = self.master2data_idx[str(node_id)]
            return len(replica_list)
        except KeyError:
            return 0

    # 自ノードの担当データはレプリカではないが、レプリカと同様に扱う
    def get_all_replica_by_master_node(self, node_id : int) -> List[DataIdAndValue]:
        try:
            replica_list : List[StoredValueEntry] = self.master2data_idx[str(node_id)]
            return [DataIdAndValue(data_id = data.data_id, value_data=data.value_data ) for data in replica_list]
        except KeyError:
            return []
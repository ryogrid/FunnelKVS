# coding:utf-8

from typing import Dict, List, Optional, cast, TYPE_CHECKING

from .chord_util import ChordUtil, KeyValue, NodeIsDownedExceptiopn, AppropriateNodeNotFoundException, \
    InternalControlFlowException, StoredValueEntry, DataIdAndValue

if TYPE_CHECKING:
    from .chord_node import ChordNode
    from .node_info import NodeInfo

class DataStore:

    DATA_STORE_OP_DIRECT_STORE = "DIRECT_STORE"
    DATA_STORE_OP_DIRECT_REMOVE = "DIRECT_REMOVE"

    def __init__(self, existing_node : 'ChordNode'):
        self.existing_node : 'ChordNode' = existing_node

        # Keyはハッシュを通されたものなので元データの値とは異なる
        self.stored_data : Dict[str, StoredValueEntry] = {}

    # DataStoreクラスオブジェクトのデータ管理の枠組みに従った、各関連フィールドの一貫性を維持したまま
    # データ追加・更新処理を行うアクセサメソッド
    # master_node引数を指定しなかった場合は、self.existing_node.node_info をデータのマスターの情報として格納する
    def store_new_data(self, data_id : int, value_str : str):
        ChordUtil.dprint("store_new_data_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                         + ChordUtil.gen_debug_str_of_data(data_id))

        with self.existing_node.node_info.lock_of_datastore:
            sv_entry = StoredValueEntry(data_id=data_id, value_data=value_str)

            # デバッグプリント
            ChordUtil.dprint_data_storage_operations(self.existing_node.node_info,
                                                     DataStore.DATA_STORE_OP_DIRECT_STORE,
                                                     data_id
                                                     )

            self.stored_data[str(data_id)] = sv_entry
            # デバッグのためにグローバル変数の形で管理されているデータのロケーション情報を更新する
            ChordUtil.add_data_placement_info(data_id, self.existing_node.node_info)

    # DataStoreクラスオブジェクトのデータ管理の枠組みに従った、各関連フィールドの一貫性を維持したまま
    # データ削除処理を行うアクセサメソッド
    def remove_data(self, data_id: int):
        with self.existing_node.node_info.lock_of_datastore:
            try:
                del self.stored_data[str(data_id)]
            except KeyError:
                # 本来は起きてはならないエラーだが対処のし様もないのでワーニングだけ出力する
                ChordUtil.dprint("remove_data_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_data(data_id)
                                 + ",WARNING__REMOVE_TARGET_DATA_NOT_EXIST")
                return

            # デバッグのためにグローバル変数の形で管理されているデータのロケーション情報を更新する
            ChordUtil.remove_data_placement_info(data_id, self.existing_node.node_info)
            # デバッグプリント
            ChordUtil.dprint_data_storage_operations(self.existing_node.node_info,
                                                     DataStore.DATA_STORE_OP_DIRECT_REMOVE,
                                                     data_id
                                                     )

    # 自ノードが担当ノードとなる保持データを全て返す
    def pass_tantou_data_for_replication(self) -> List[DataIdAndValue]:
        with self.existing_node.node_info.lock_of_datastore:
            ChordUtil.dprint(
                "pass_tantou_data_for_replication_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info))

            ret_data_list : List[DataIdAndValue] = []
            for key, value in self.stored_data.items():
                if ChordUtil.exist_between_two_nodes_right_mawari(self.existing_node.node_info.predecessor_info.node_id, self.existing_node.node_info.node_id, int(key)):
                    ret_data_list.append(DataIdAndValue(data_id=int(key), value_data=value.value_data))

            ChordUtil.dprint("pass_tantou_data_for_replication_2," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.predecessor_info) + ","
                             + str(len(ret_data_list)))

        return ret_data_list

    # レプリカデータを受け取る
    # 他のノードが、保持しておいて欲しいレプリカを渡す際に呼び出される.
    # なお、master_node 引数と呼び出し元ノードは一致しない場合がある.
    # replace_allオプション引数をTrueとした場合は、指定したノードのデータを丸っと入れ替える
    # 返り値として、処理が完了した時点でmaster_nodeに紐づいているレプリカをいくつ保持して
    # いるかを返す
    def receive_replica(self, pass_datas : List[DataIdAndValue]):
        with self.existing_node.node_info.lock_of_datastore:
            ChordUtil.dprint("receive_replica_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + str(len(pass_datas)))

            for id_value in pass_datas:
                self.store_new_data(id_value.data_id, id_value.value_data)

            ChordUtil.dprint("receive_replica_2," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + str(len(pass_datas)))

    # 複数マスタのレプリカをまとめて受け取り格納する
    def store_replica_of_multi_masters(self, data_list: List[DataIdAndValue]):
        ChordUtil.dprint(
            "store_replica_of_multi_masters_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
            + str(len(data_list)))

        self.receive_replica(data_list)

        ChordUtil.dprint(
            "store_replica_of_multi_masters_2," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
            + str(len(data_list)))

    # 自身が保持しているデータのうち委譲するものを返す.
    # 対象となるデータは時計周りに辿った際に 引数 node_id と 自身の node_id
    # の間に data_id が位置するデータである.
    # join呼び出し時、新たに参加してきた新規ノードに、successorとなる自身が、担当から外れる
    # 範囲のデータの委譲を行うために、新規ノードから呼び出される形で用いられる.
    # rest_copy引数によってコピーを渡すだけか、完全に委譲してしまい自身のデータストアからは渡したデータを削除
    # するかどうか選択できる
    def delegate_my_tantou_data(self, node_id : int, rest_copy : bool = True) -> List[KeyValue]:
        # TODO: stabilize処理の中で唯一 datastoreのロックをとっている箇所
        #       対処しないとまずいかもしれない
        with self.existing_node.node_info.lock_of_datastore:
            ChordUtil.dprint("delegate_my_tantou_data_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             + ChordUtil.gen_debug_str_of_data(node_id))
            ret_datas : List[KeyValue] = []
            try:
                tantou_data: List[StoredValueEntry] = self.master2data_idx[str(self.existing_node.node_info.node_id)]
            except KeyError:
                ChordUtil.dprint(
                    "delegate_my_tantou_data_2," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                    + ChordUtil.gen_debug_str_of_data(node_id) + ",NO_TANTOU_DATA_YET")
                # まだ一つもデータを保持していなかったということなので空リストを返す
                return []

            for entry in tantou_data:
                # Chordネットワークを右回りにたどった時に、データの id (data_id) が呼び出し元の node_id から
                # 自身の node_id の間に位置する場合は、そのデータの担当は自身から変わらないため、渡すデータから
                # 除外する
                if ChordUtil.exist_between_two_nodes_right_mawari(node_id, self.existing_node.node_info.node_id, entry.data_id):
                    ChordUtil.dprint(
                        "delegate_my_tantou_data_2," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                        + ChordUtil.gen_debug_str_of_data(node_id) + "," + ChordUtil.gen_debug_str_of_data(entry.data_id))
                    continue

                # 文字列の参照をそのまま用いてしまうが、文字列はイミュータブルであるため
                # 問題ない
                item = KeyValue(None, entry.value_data)
                item.data_id = entry.data_id
                ret_datas.append(item)

            # データを委譲する際に元々持っていたノードから削除するよう指定されていた場合
            if rest_copy == False:
                for kv in ret_datas:
                    self.remove_data(cast('int', kv.data_id), self.existing_node.node_info)

                # 委譲したことで自身が担当ノードで無くなったデータについてsuccessorList
                # 内のノードに通知し、削除させる（それらのノードは再度同じレプリカを保持する
                # ことになるかもしれないが、それは新担当の管轄なので、非効率ともなるがひとまず削除させる）
                # 削除が完了するまで本メソッドは終了しないため、新担当がレプリカを配布する処理と以下の処理が
                # バッティングすることはない
                # TODO: 現在の実装では同じスレッドが処理を行い、こちらのメソッドが終わった後にレプリカを配布するため
                #       バッティングは起きないが、stored_data内のデータを削除する処理ではマスターノードは意識されない
                #       ため実システム化や複数スレッド化した再は考慮が必要かもしれない
                #       in delegate_my_node_data
                with self.existing_node.node_info.lock_of_succ_infos, self.existing_node.node_info.lock_of_datastore:
                    for node_info in self.existing_node.node_info.successor_info_list:
                        try:
                            node : ChordNode = ChordUtil.get_node_by_address(node_info.address_str)
                            # マスターノードが自ノードとして設定されているデータのうち自ノードが担当でなくなるデータを削除させる.
                            # 少なくとも、自ノードが担当となる範囲以外は自身の担当でなくなるため、担当範囲以外全てを指定して要請する.
                            # 始点・終点の指定としては、左周りで考えた時に自ノードから、委譲先ノードまでの範囲が、担当が自ノードから
                            # 変化していないID範囲であることを踏まえると、Chordネットワークを右回りでたどった時に、自ノードから委譲
                            # 先のノードに至るID範囲は自身が担当でない全てのID範囲と考えることができる
                            node.data_store.delete_replica(self.existing_node.node_info, range_start=self.existing_node.node_info.node_id, range_end=node_id)
                            ChordUtil.dprint(
                                "delegate_my_tantou_data_3," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                + ChordUtil.gen_debug_str_of_data(node_id) + "," + ChordUtil.gen_debug_str_of_node(node.node_info))
                        except NodeIsDownedExceptiopn:
                            # stablize処理 がよろしくやってくれるのでここでは何もしない
                            ChordUtil.dprint(
                                "delegate_my_tantou_data_4,NODE_IS_DOWNED" + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                + ChordUtil.gen_debug_str_of_data(node_id) + "," + ChordUtil.gen_debug_str_of_node(node_info))
                            continue

        return ret_datas

    # 存在しないKeyが与えられた場合 KeyErrorがraiseされる
    def get(self, data_id : int) -> StoredValueEntry:
        with self.existing_node.node_info.lock_of_datastore:
            return self.stored_data[str(data_id)]

    # 自ノードのidが指定された場合、返るデータはマスターデータだが同様に扱うものとする
    def pass_all_replica(self) -> List[DataIdAndValue]:
        ChordUtil.dprint("pass_all_replica_1," + ChordUtil.gen_debug_str_of_node(
            self.existing_node.node_info))

        with self.existing_node.node_info.lock_of_datastore:
            ret_data_list: List[DataIdAndValue] = []
            for key, value in self.stored_data.items():
                ret_data_list.append(DataIdAndValue(data_id=int(key), value_data=value.value_data))

            ChordUtil.dprint("pass_all_replica_2," + ChordUtil.gen_debug_str_of_node(
                self.existing_node.node_info) + ","
                + str(len(ret_data_list)))

        return ret_data_list
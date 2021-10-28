/*
class DataStore:
    def __init__(self, existing_node : 'ChordNode'):
        self.existing_node : 'ChordNode' = existing_node

        # Keyはハッシュを通されたものなので元データの値とは異なる
        self.stored_data : Dict[str, DataIdAndValue] = {}

    # DataStoreクラスオブジェクトのデータ管理の枠組みに従った、各関連フィールドの一貫性を維持したまま
    # データ追加・更新処理を行うアクセサメソッド
    # master_node引数を指定しなかった場合は、self.existing_node.node_info をデータのマスターの情報として格納する
    def store_new_data(self, data_id : int, value_str : str):
        # ログの量が多くなりすぎるのでコメントアウトしておく
        # ChordUtil.dprint("store_new_data_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
        #                  + ChordUtil.gen_debug_str_of_data(data_id))

        with self.existing_node.node_info.lock_of_datastore:
            di_entry = DataIdAndValue(data_id=data_id, value_data=value_str)

            # デバッグプリント
            ChordUtil.dprint_data_storage_operations(self.existing_node.node_info,
                                                     DataStore.DATA_STORE_OP_DIRECT_STORE,
                                                     data_id
                                                     )

            self.stored_data[str(data_id)] = di_entry
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
    def get_all_tantou_data(self, node_id : Optional[int] = None) -> List[DataIdAndValue]:
        with self.existing_node.node_info.lock_of_datastore:
            ChordUtil.dprint(
                "pass_tantou_data_for_replication_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info))

            if self.existing_node.node_info.predecessor_info == None and node_id == None:
                ChordUtil.dprint(
                    "pass_tantou_data_for_replication_2," + ChordUtil.gen_debug_str_of_node(
                        self.existing_node.node_info))
                return []

            if node_id != None:
                pred_id = cast(int, node_id)
            else:
                pred_id = cast('NodeInfo', self.existing_node.node_info.predecessor_info).node_id

            ret_data_list : List[DataIdAndValue] = []
            for key, value in self.stored_data.items():
                if ChordUtil.exist_between_two_nodes_right_mawari(pred_id, self.existing_node.node_info.node_id, int(key)):
                    ret_data_list.append(DataIdAndValue(data_id=int(key), value_data=value.value_data))

            ChordUtil.dprint("pass_tantou_data_for_replication_3," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                             # + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info.predecessor_info) + ","
                             + str(len(ret_data_list)))

        return ret_data_list



    # 存在しないKeyが与えられた場合 KeyErrorがraiseされる
    def get(self, data_id : int) -> PResult[Optional[DataIdAndValue]]:
        with self.existing_node.node_info.lock_of_datastore:
            try:
                return PResult.Ok(self.stored_data[str(data_id)])
            except KeyError:
                return PResult.Err(None, ErrorCode.KeyError_CODE)


    # 全ての保持しているデータを返す
    def get_all_data(self) -> List[DataIdAndValue]:
        ChordUtil.dprint("get_all_data_1," + ChordUtil.gen_debug_str_of_node(
            self.existing_node.node_info))

        with self.existing_node.node_info.lock_of_datastore:
            ret_data_list: List[DataIdAndValue] = []
            for key, value in self.stored_data.items():
                ret_data_list.append(DataIdAndValue(data_id=int(key), value_data=value.value_data))

            ChordUtil.dprint("get_all_data_2," + ChordUtil.gen_debug_str_of_node(
                self.existing_node.node_info) + ","
                + str(len(ret_data_list)))

        return ret_data_list
*/
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::cell::RefCell;

use crate::gval;
use crate::chord_node;
use crate::node_info;
use crate::chord_util;
use crate::stabilizer;
use crate::router;
use crate::endpoints;

pub const DELETED_ENTRY_MARKING_STR : &str = "THIS_KEY_IS_DELETED";
pub const DATA_STORE_OP_DIRECT_STORE : &str = "DIRECT_STORE";
pub const DATA_STORE_OP_DIRECT_REMOVE : &str = "DIRECT_REMOVE";

type ArMu<T> = Arc<Mutex<T>>;

#[derive(Debug, Clone)]
pub struct DataStore {
    // Keyはハッシュを通されたものなので元データの値とは異なる
    stored_data : HashMap<String, chord_util::DataIdAndValue>,
}

impl DataStore {
    pub fn new() -> DataStore {
        let sd = HashMap::new();
        DataStore {stored_data : sd}
    }
}

// TODO: (rustr) 引数にArMu型でラップされたDataStoreオブジェクトをとる形で
//               stored_dataを引数を操作するよう、なんちゃってカプセル化する
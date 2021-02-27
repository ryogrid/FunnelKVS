# coding:utf-8

from typing import Dict, List, Optional, cast, TYPE_CHECKING

from .chord_util import ChordUtil, KeyValue, NodeIsDownedExceptiopn, AppropriateNodeNotFoundException, \
    InternalControlFlowException, StoredValueEntry, NodeInfoPointer, DataIdAndValue

if TYPE_CHECKING:
    from .chord_node import ChordNode

class TaskQueue:
    JOIN_PARTIAL = "join_partial"

    def __init__(self, existing_node : 'ChordNode'):
        self.tqueue : List[str] = []
        self.existing_node = existing_node

    def append_task(self, task_code : str):
        self.tqueue.append(task_code)

    # キュー内の最初のタスクを実行する
    # 処理が失敗した場合は先頭に戻す
    def exec_first(self):
        if len(self.tqueue) > 0:
            ChordUtil.dprint("exec_first_0," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + "," + str(self.tqueue))
            task_code : str = self.tqueue.pop()
            if task_code == TaskQueue.JOIN_PARTIAL:
                try:
                    self.existing_node.stabilizer.partial_join_op()
                except InternalControlFlowException:
                    # 実行に失敗したため再実行すべく先頭に戻す
                    self.tqueue.insert(0, task_code)
                    ChordUtil.dprint("exec_first_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                     + "INTERNAL_CONTROL_FLOW_EXCEPTION_OCCURED")
                except KeyError:
                    # まだ put されていないことを意味するので、無視して正常終了する
                    ChordUtil.dprint("exec_first_2," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                                     + "SUCCESS_WITH_NO_DELEGATED_DATA")

/*
# coding:utf-8

from .chord_util import ChordUtil, InternalControlFlowException, NodeIsDownedExceptiopn

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
                ret = self.existing_node.stabilizer.partial_join_op()
                if (ret.is_ok):
                    pass
                else:  # ret.err_code == ErrorCode.InternalControlFlowException_CODE
                    # 実行に失敗したため再実行すべく先頭に戻す
                    self.tqueue.insert(0, task_code)
                    ChordUtil.dprint(
                        "exec_first_1," + ChordUtil.gen_debug_str_of_node(self.existing_node.node_info) + ","
                        + "INTERNAL_CONTROL_FLOW_EXCEPTION_OCCURED")
*/
use std::sync::Arc;
use std::cell::RefCell;
use parking_lot::{ReentrantMutex, const_reentrant_mutex};

use crate::gval;
use crate::chord_node;
use crate::node_info;
use crate::chord_util;
use crate::endpoints;
use crate::data_store;
use crate::router;
use crate::stabilizer;

pub const JOIN_PARTIAL : &str = "join_partial";

#[derive(Debug, Clone)]
pub struct TaskQueue {
    pub existing_node : Arc<ReentrantMutex<RefCell<chord_node::ChordNode>>>,
    pub tqueue : Arc<ReentrantMutex<RefCell<Vec<String>>>>
}

impl TaskQueue {
    pub fn new(parent : Arc<ReentrantMutex<RefCell<chord_node::ChordNode>>>) -> TaskQueue {
        let tq = Arc::new(const_reentrant_mutex(RefCell::new(Vec::new())));
        TaskQueue {existing_node : parent, tqueue: tq}
    }
}
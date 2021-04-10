# coding:utf-8

from typing import Dict, List, Optional, cast, TYPE_CHECKING

from .chord_util import ChordUtil, InternalControlFlowException, NodeIsDownedExceptiopn

if TYPE_CHECKING:
    from .chord_node import ChordNode

class Endpoints:

    def __init__(self, existing_node : 'ChordNode'):
        self.existing_node = existing_node

    def rrpc__global_put(self):
        raise Exception("not implemented yet")

    def grpc__put(self):
        raise Exception("not implemented yet")

    def grpc__global_get_recover_prev(self):
        raise Exception("not implemented yet")

    def rrpc__global_get(self):
        raise Exception("not implemented yet")

    def grpc__get(self):
        raise Exception("not implemented yet")

    def grpc__global_delete(self):
        raise Exception("not implemented yet")

    def grpc__pass_node_info(self):
        raise Exception("not implemented yet")

    def grpc__get_all_tantou_data(self):
        raise Exception("not implemented yet")

    def grpc__receive_replica(self):
        raise Exception("not implemented yet")

    def grpc__delegate_my_tantou_data(self):
        raise Exception("not implemented yet")

    def grpc__get_all_data(self):
        raise Exception("not implemented yet")

    def grpc__find_successor(self):
        raise Exception("not implemented yet")

    def grpc__closest_preceding_finger(self):
        raise Exception("not implemented yet")

    def grpc__pass_successor_list(self):
        raise Exception("not implemented yet")

    def grpc__pass_predecessor_info(self):
        raise Exception("not implemented yet")

    def grpc__set_routing_infos_force(self):
        raise Exception("not implemented yet")

    def grpc__check_predecessor(self):
        raise Exception("not implemented yet")

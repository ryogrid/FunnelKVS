# coding:utf-8

import threading
import time

# アドレス文字列をキーとしてとり、対応するノードのChordNodeオブジェクトを返すハッシュ
# IPアドレスが分かれば、対応するノードと通信できることと対応している
all_node_dict = {}

# DHT上で保持されている全てのデータが保持されているリスト
# 2要素のリスト [key、value] を要素として持つ
# 全てのノードはputの際はDHTにデータをputするのとは別にこのリストにデータを追加し、
# getする際はDHTに対してgetを発行するための適当なキーをこのリストからランダム
# に選んだ要素のkeyを用いる. また value も一時的に保持しておき、取得できた内容と
# 一致しているか確認する
all_data_list = []

class NodeInfo:
    id_str = None
    address_str = None
    # 半閉区間 (start, end] で startの値は含まない
    assigned_range_start = None
    assigned_range_end = None

    def __init__(self, **params):
        # メンバ変数に代入していく
        for key, val in params.items():
            if hasattr(self, key):
                self.__dict__[key] = val

class ChordNode:
    node_info = None
    #KeyもValueもどちらも文字列. Keyはハッシュを通されたものなので元データの値とは異なる
    stored_data = {}
    predicessor = None
    finger_table = []
    successors = []

    def __init__(self, node_address):
        print("hello Chord!")
        self.initialize_routing_entries()

    def put(self, key_str, value_str):
        print("not implemented yet")

    def get(self, key_str):
        print("not implemented yet")

    def delete(self, key_str):
        print("not implemented yet")

    def initialize_routing_entries(self):
        print("not implemented yet")

def add_new_node():
    print("not implemented yet")

def do_stabilize_on_random_node():
    print("not implemented yet")

def do_put_on_random_node():
    print("not implemented yet")

def do_get_on_random_node():
    print("not implemented yet")

def node_join_th():
    counter = 0
    while counter < 500:
        add_new_node()
        time.sleep(1) # sleep 1sec

def stabilize_th():
    while True:
        do_stabilize_on_random_node()

def data_put_th():
    while True:
        do_put_on_random_node()

def data_get_th():
    while True:
        do_get_on_random_node()

def main():
    node_join_th_handle = threading.Thread(target=node_join_th, daemon=True, args=())
    node_join_th_handle.start()

    stabilize_th_handle = threading.Thread(target=stabilize_th, daemon=True, args=())
    stabilize_th_handle.start()

    data_put_th_handle = threading.Thread(target=data_put_th, daemon=True, args=())
    data_put_th_handle.start()

    data_get_th_handle = threading.Thread(target=data_get_th, daemon=True, args=())
    data_get_th_handle.start()

if __name__ == '__main__':
    main()
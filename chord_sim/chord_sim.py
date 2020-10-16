# coding:utf-8

# TODO: それっぽく動作していることが分かるような最低限のデバッグログを出力するようにする

import threading
import time

# アドレス文字列をキーとしてとり、対応するノードのChordNodeオブジェクトを返すハッシュ
# IPアドレスが分かれば、対応するノードと通信できることと対応している
all_node_dict = {}

# DHT上で保持されている全てのデータが保持されているリスト
# KeyValueオブジェクトを要素として持つ
# 全てのノードはputの際はDHTにデータをputするのとは別にこのリストにデータを追加し、
# getする際はDHTに対してgetを発行するためのデータをこのリストからランダム
# に選び、そのkeyを用いて探索を行う. また value も一時的に保持しておき、取得できた内容と
# 一致しているか確認する
all_data_list = []

class ChordUtil:
    # 任意の文字列をハッシュ値（定められたbit数で表現される整数値）に変換しint型で返す
    @classmethod
    def hash_str(cls, input_str):
        # TODO: ハッシュ関数のサンプルコードを探しておき、実装する
        print("not implemented yet")

class KeyValue:
    key = None
    value = None
    # keyのハッシュ値
    id = None

    def __init__(self, key, value):
        self.key = key
        self.value = value
        self.id = ChordUtil.hash_str(key)

class NodeInfo:
    # NodeInfoオブジェクトに対応するChordNodeのオブジェクト
    # 本来は address_str フィールドの文字列からオブジェクトを引くという
    # ことをせずにアクセスできるのは実システムとの対応が崩れるのでズルなのだが
    # ひとまず保持しておくことにする
    node_obj = None

    id = None
    address_str = None

    # 半開区間 (start, end] で startの値は含まない
    assigned_range_start = None
    assigned_range_end = None

    # NodeInfoオブジェクトを保持
    successor_info = None
    predicessor_info = None

    def __init__(self, **params):
        # メンバ変数に代入していく
        for key, val in params.items():
            if hasattr(self, key):
                self.__dict__[key] = val

class ChordNode:
    node_info = None
    # KeyもValueもどちらも文字列. Keyはハッシュを通されたものなので元データの値とは異なる
    stored_data = {}

    # NodeInfoオブジェクトを要素として持つリスト
    finger_table = []

    # join時の処理もコンストラクタで行う
    def __init__(self, node_address):
        print("hello Chord!")
        # TODO: join時は自ノードの情報を他ノードに通知する必要はなかったかもしれない
        #       ただし、その場合でも、joinすることによって担当範囲を引き継ぐことになる場合を考えると、
        #       最初の仲介ノードから保持しているデータを受け取る必要はありそう（レプリケーションを
        #       考慮する場合も同様）
        self.join(node_address)

    #TODO: put
    def put(self, key_str, value_str):
        print("not implemented yet")

    #TODO: get
    def get(self, key_str):
        print("not implemented yet")

    #TODO: delete
    def delete(self, key_str):
        print("not implemented yet")

    # TODO: join
    #       node_addressに対応するノードをsuccessorとして設定し, そのノードと
    #       必要に応じてやり取りを行う
    def join(self, node_address):
        print("not implemented yet")

    # TODO: stabilize
    #       stabilize処理を行う
    #       FingerTableやpredecessorはここで初めて設定される
    def stabilize(self):
        print("not implemented yet")

    # TODO: find_successor
    #       idで識別されるデータを担当するノードの名前解決を行う
    #       実システムでのやりとりの形になるようにブレークダウンする必要あり
    #       なお、node_infoクラスにChordNodeオブジェクト自体も格納しておけばこのような形でも検証できなくはない
    def find_successor(self, id):
        n_dash = self.find_predecessor(id)
        return n_dash.first_successor

    # TODO: find_predecessor
    #       id　の前で一番近い位置に存在するノードを探索する
    #       実システムでのやりとりの形になるようにブレークダウンする必要あり
    #       なお、node_infoクラスにChordNodeオブジェクト自体も格納しておけばこのような形でも検証できなくはない
    def find_predecessor(self, id):
        n_dash = self
        while not (n_dash.node_info.predecessor_info.id < id and id <= n_dash.node_info.successor_info.id):
            n_dash = n_dash.closest_preceding_finger(id)
        return n_dash

    # TODO: closest_preceding_finger
    #       自身の持つ経路情報をもとに,  id から前方向に一番近いノードの情報を返す
    def closest_preceding_finger(self, id):
        # TODO: 範囲の狭いエントリから探索していく形になるよう確認すること
        for entry in self.finger_table:
            if self.node_info.id < entry.id and entry.id <= id:
                return entry
        #自身が一番近いpredecessorである
        return self.node_info

# node_addrに対応するノードをpredecessorとして持つ形でネットワークに新規ノードを参加させる
def add_new_node(node_addr):
    new_node = ChordNode(node_addr)
    all_node_dict[new_node.node_info.address_str] = new_node

# ランダムに選択したノードに stabilize のアクションをとらせる
# やりとりを行う側（つまりChordNodeクラス）にそのためのメソッドを定義する必要がありそう
def do_stabilize_on_random_node():
    # TODO: ランダムに選択するよう変更する
    node = list(all_node_dict.values())[0]
    node.stabilize()
    print("not implemented yet")

# 適当なデータを生成し、IDを求めて、そのIDなデータを担当するChordネットワーク上のノードの
# アドレスをよろしく解決し、見つかったノードにputの操作を依頼する
def do_put_on_random_node():
    # TODO: 実行毎に内容が異なるよう修正する
    kv_data = KeyValue("hogehoge", "fugafuga")
    # TODO: ランダムに選択するよう変更する
    node = list(all_node_dict.values())[0]
    node.put(kv_data.key, kv_data.value)
    all_data_list.append(kv_data)
    print("not implemented yet")


# グローバル変数であるall_data_listからランダムにデータを選択し、そのデータのIDから
# Chordネットワーク上の担当ノードのアドレスをよろしく解決し、見つかったノードにgetの操作を依頼する
def do_get_on_random_node():
    # TODO: ランダムに選択するよう修正する
    target_data = all_data_list[0]
    target_data_key = target_data.id

    # TODO: ランダムに選択するよう変更する
    node = list(all_node_dict.values())[0]
    node.get(target_data_key)
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
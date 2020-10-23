# coding:utf-8

# TODO: それっぽく動作していることが分かるような最低限のデバッグプリントを出力するようにする
# TODO: ノードの　born_id をデバッグプリントに含めるようにする

import threading
import time
import random
import hashlib

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

# 検証を分かりやすくするために何ノード目として生成されたか
# のデバッグ用IDを持たせるためのカウンタ
already_born_node_num = 0

class ChordUtil:
    # 任意の文字列をハッシュ値（定められたbit数で表現される整数値）に変換しint型で返す
    # アルゴリズムはSHA1, 160bitで表現される正の整数となる
    # メモ: 10進数の整数は組み込みの hex関数で 16進数表現での文字列に変換可能
    @classmethod
    def hash_str(cls, input_str):
        hash_hex_str = hashlib.sha1(input_str.encode()).hexdigest()
        hash_id_num = int(hash_hex_str, 16)
        return hash_id_num

    # 与えたリストの要素のうち、ランダムに選択した1要素を返す
    @classmethod
    def get_random_elem(cls, list_like):
        length = len(list_like)
        idx = random.randint(0, length - 1)
        return list_like[idx]

    # UNIXTIME（ミリ秒精度）にいくつか値を加算した値からアドレス文字列を生成する
    @classmethod
    def gen_address_str(cls):
        return str(time.time() + 10)

# all_data_listグローバル変数に格納される形式としてのみ用いる
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

    # デバッグ用のID（実システムには存在しない）
    # 何ノード目として生成されたかの値
    born_id = None

    # 半開区間 (start, end] で startの値は含まない
    assigned_range_start = None
    assigned_range_end = None

    # NodeInfoオブジェクトを保持
    successor_info = None
    predecessor_info = None

    def __init__(self, **params):
        # メンバ変数に代入していく
        for key, val in params.items():
            if hasattr(self, key):
                self.__dict__[key] = val

class ChordNode:
    node_info = NodeInfo()
    # KeyもValueもどちらも文字列. Keyはハッシュを通されたものなので元データの値とは異なる
    stored_data = {}

    # NodeInfoオブジェクトを要素として持つリスト
    # インデックスの小さい方から狭い範囲が格納される形で保持する
    finger_table = []

    # join時の処理もコンストラクタで行う
    def __init__(self, node_address, first_node = False):
        global already_born_node_num

        # ミリ秒精度のUNIXTIMEから自身のアドレスにあたる文字列と、Chorネットワーク上でのIDを決定する
        self.node_info.address_str = ChordUtil.gen_address_str()
        self.node_info.id = ChordUtil.hash_str(self.node_info.address_str)

        already_born_node_num += 1
        self.node_info.born_id = already_born_node_num

        if(first_node):
            # 最初の1ノードの場合
            # 自身を仲介ノード（successorに設定される）としてネットワークに参加する
            # TODO: 初期ノードの初期化がこれで問題ないか確認する
            self.join(self.node_info.address_str)
        else:
            self.join(node_address)

    def global_put(self, key_str, value_str):
        # resolve ID to address of a node which is assigned ID range the ID is included to
        # 注: 現状、ここでは対象のChordNordオブジェクトを直接取得してしまっており、正確にはアドレスの解決ではない
        target_node = self.find_successor()
        target_node.put(key_str, value_str)
        print("global_put," + str(ChordUtil.hash_str(key_str)) + "," + key_str + "," + value_str)

    def put(self, key_str, value_str):
        key_id_str = str(ChordUtil.hash_str(key_str))
        self.stored_data[key_id_str] = value_str
        print("put," + str(self.node_info.id) + "," + key_id_str + "," + key_str + "," + value_str)

    # 得られた value の文字列を返す
    def global_get(self, key_str):
        # resolve ID to address of a node which is assigned ID range the ID is included to
        # 注: 現状、ここでは対象のChordNordオブジェクトを直接取得してしまっており、正確にはアドレスの解決ではない
        target_node = self.find_successor()
        key_id_str = str(ChordUtil.hash_str(key_str))
        got_value_str = target_node.get(key_id_str)
        print("global_get," + key_id_str + "," + key_str + "," + got_value_str)
        return got_value_str

    # 得られた value の文字列を返す
    def get(self, id_str):
        # TODO: id_strに対応するデータを持っていなかった場合、例外が発生するので
        #       その場合にNot Found 的なエラー文字列を返してやるようにする
        ret_value_str = self.stored_data[id_str]
        print("get," + str(self.node_info.id) + "," + id_str + "," + ret_value_str)

        return ret_value_str

    # TODO: global_delete (ひとまずglobal_getとglobal_putだけ実装するので後で良い）
    def global_delete(self, key_str):
        print("not implemented yet")
        
    # TODO: delete (ひとまずgetとputだけ実装するので後で良い）
    def delete(self, key_str):
        print("not implemented yet")


    # node_addressに対応するノードをsuccessorとして設定し, そのノードと
    # 必要に応じてやり取りを行う
    def join(self, node_address):
        # TODO: あとで、ちゃんとノードに定義されたAPIを介して情報を受け取るようにする必要あり
        successor_info = all_node_dict[node_address]
        self.node_info.successor_info = successor_info

        # 自ノードのID（16進表現)、仲介ノード（初期ノード、successorとして設定される）のID(16進表現)
        print("join," + hex(self.node_info.id) + "," + hex(successor_info.id))

    # TODO: stabilize
    #       stabilize処理を行う
    #       FingerTableやpredecessorはここで初めて設定される
    def stabilize(self):
        # TODO: FingerTableを順を追って構築していく処理を実装する
        #       また、おそらく、ここでjoin時には分からなかった自身の担当範囲
        #       が決定し、自身がjoinするまでその範囲を担当していたノードから
        #       保持しているデータの委譲（コピーでも良いはず）を受ける必要が
        #       あるはず。
        #       ただし、全ノードが揃って、stabilizeも十分に行われた後にしか
        #       putを行わないという条件であれば、保持データの委譲は不要にできる
        #       が、現実的にはそのようなシチュエーションは想定できないので、初版
        #       での本シミュレータの動作確認時にひとまず動かすというタイミングで
        #       のみ許される条件であろう

        print("not implemented yet")

    # id（int）で識別されるデータを担当するノードの名前解決を行う
    # TODO: あとで実システムでのやりとりの形になるようにブレークダウンする必要あり
    def find_successor(self, id):
        n_dash = self.find_predecessor(id)
        return n_dash.first_successor

    # id(int)　の前で一番近い位置に存在するノードを探索する
    # TODO: あとで実システムでのやりとりの形になるようにブレークダウンする必要あり
    def find_predecessor(self, id):
        n_dash = self
        while not (n_dash.node_info.predecessor_info.id < id and id <= n_dash.node_info.successor_info.id):
            n_dash = n_dash.closest_preceding_finger(id)
        return n_dash

    #  自身の持つ経路情報をもとに,  id から前方向に一番近いノードの情報を返す
    def closest_preceding_finger(self, id):
        # 範囲の狭いエントリから探索していく
        for entry in self.finger_table:
            if self.node_info.id < entry.id and entry.id <= id:
                return entry

        #自身が一番近いpredecessorである
        return self.node_info

# ネットワークに存在するノードから1ノードをランダムに取得する
# ChordNodeオブジェクトを返す
def get_a_random_node():
    node_list = list(all_node_dict.values())
    node = ChordUtil.get_random_elem(node_list)
    return node

# ランダムに仲介ノードを選択し、そのノードに仲介してもらう形でネットワークに参加させる
def add_new_node():
    tyukai_node = get_a_random_node()
    new_node = ChordNode(tyukai_node.node_info.address_str)
    all_node_dict[new_node.node_info.address_str] = new_node

# ランダムに選択したノードに stabilize のアクションをとらせる
# やりとりを行う側（つまりChordNodeクラス）にそのためのメソッドを定義する必要がありそう
def do_stabilize_on_random_node():
    node = get_a_random_node()
    node.stabilize()

# 適当なデータを生成し、IDを求めて、そのIDなデータを担当するChordネットワーク上のノードの
# アドレスをよろしく解決し、見つかったノードにputの操作を依頼する
def do_put_on_random_node():
    unixtime_str = str(time.time())
    # ミリ秒精度で取得したUNIXTIMEを文字列化してkeyとvalueに用いる
    kv_data = KeyValue(unixtime_str, unixtime_str)
    node_list = list(all_node_dict.values())
    node = get_a_random_node()
    node.global_put(kv_data.key, kv_data.value)
    all_data_list.append(kv_data)
    print("not implemented yet")


# グローバル変数であるall_data_listからランダムにデータを選択し、そのデータのIDから
# Chordネットワーク上の担当ノードのアドレスをよろしく解決し、見つかったノードにgetの操作を依頼する
def do_get_on_random_node():
    # まだ put が行われていなかったら何もせずに終了する
    if len(all_data_list) == 0:
        return

    target_data = ChordUtil.get_random_elem(all_data_list)
    target_data_key = target_data.id

    node = get_a_random_node()
    node.global_get(target_data_key)

def node_join_th():
    counter = 0
    while counter < 500:
        add_new_node()
        time.sleep(1) # sleep 1sec

def stabilize_th():
    while True:
        do_stabilize_on_random_node()
        # TODO: 百ミリ秒程度は sleepなりでインターバルを入れないと
        #       プロセスのロードが大変なことになると思われる

def data_put_th():
    while True:
        do_put_on_random_node()
        time.sleep(1) # sleep 1sec

def data_get_th():
    while True:
        do_get_on_random_node()
        time.sleep(1) # sleep 1sec

def main():
    first_node = ChordNode("THIS_VALUE_IS_NOT_USED", first_node=True)
    all_node_dict[first_node.node_info.address_str] = first_node

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
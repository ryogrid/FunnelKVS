# coding:utf-8

import threading
import time
import random
import hashlib
import datetime
import math
from typing import Dict, List, Any

ID_SPACE_BITS = 30 # 160 <- sha1での本来の値
ID_SPACE_RANGE = 2**ID_SPACE_BITS # 0を含めての数である点に注意

STABILIZE_SUCCESSOR_BATCH_TIMES = 20
STABILIZE_FTABLE_BATCH_TIMES = 1

# # 160bit符号なし整数の最大値
# # Chordネットワーク上のID空間の上限
# ID_MAX = 2**ID_SPACE_BITS - 1

# 30bit符号なし整数の最大値
# Chordネットワーク上のID空間の上限
# TODO: 検証時の実行時間短縮のためにハッシュ関数で求めた値の代わりに乱数
#       を用いているため bit数 を少なくしている
ID_MAX = ID_SPACE_RANGE - 1

NODE_NUM = 100
PUT_DATA_NUM = 100

# アドレス文字列をキーとしてとり、対応するノードのChordNodeオブジェクトを返すハッシュ
# IPアドレスが分かれば、対応するノードと通信できることと対応している
all_node_dict : Dict[str, 'ChordNode'] = {}

# DHT上で保持されている全てのデータが保持されているリスト
# KeyValueオブジェクトを要素として持つ
# 全てのノードはputの際はDHTにデータをputするのとは別にこのリストにデータを追加し、
# getする際はDHTに対してgetを発行するためのデータをこのリストからランダム
# に選び、そのkeyを用いて探索を行う. また value も保持しておき、取得できた内容と
# 照らし合わせられるようにする
all_data_list : List['KeyValue'] = []

# 検証を分かりやすくするために何ノード目として生成されたか
# のデバッグ用IDを持たせるためのカウンタ
already_born_node_num = 0

lock_of_all_data = threading.Lock()

class ChordUtil:
    # 任意の文字列をハッシュ値（定められたbit数で表現される整数値）に変換しint型で返す
    # アルゴリズムはSHA1, 160bitで表現される正の整数となる
    # メモ: 10進数の整数は組み込みの hex関数で 16進数表現での文字列に変換可能
    @classmethod
    def hash_str_to_int(cls, input_str : str) -> int:
        # hash_hex_str = hashlib.sha1(input_str.encode()).hexdigest()
        # hash_id_num = int(hash_hex_str, 16)

        # TODO: ID_SPACE_BITS ビットで表現できる符号なし整数をID空間とする.
        #       通常、ID_SPACE_BITS は sha1 で 160 となるが、この検証コードでは
        #       ハッシュ関数を用いなくても問題の起きない実装となっているため、より小さい
        #       ビット数で表現可能な IDスペース 内に収まる値を乱数で求めて返す
        hash_id_num = random.randint(0, ID_SPACE_RANGE - 1)
        return hash_id_num

    # 与えたリストの要素のうち、ランダムに選択した1要素を返す
    @classmethod
    def get_random_elem(cls, list_like : List[Any]) -> Any:
        length = len(list_like)
        idx = random.randint(0, length - 1)
        return list_like[idx]

    # UNIXTIME（ミリ秒精度）にいくつか値を加算した値からアドレス文字列を生成する
    @classmethod
    def gen_address_str(cls) -> str:
        return str(time.time() + 10)

    # 計算したID値がID空間の最大値を超えていた場合は、空間内に収まる値に変換する
    @classmethod
    def overflow_check_and_conv(cls, id : int) -> int:
        ret_id = id
        if id > ID_MAX:
            # 1を足すのは MAX より 1大きい値が 0 となるようにするため
            ret_id = id - (ID_MAX + 1)
        return ret_id

    # idがID空間の最大値に対して何パーセントの位置かを適当な精度の浮動小数の文字列
    # にして返す
    @classmethod
    def conv_id_to_ratio_str(cls, id : int) -> str:
        ratio = (id / ID_MAX) * 100.0
        return '%2.4f' % ratio

    # ID空間が環状になっていることを踏まえて base_id から前方をたどった場合の
    # ノード間の距離を求める
    # ここで前方とは、IDの値が小さくなる方向である
    @classmethod
    def calc_distance_between_nodes_left_mawari(cls, base_id : int, target_id : int) -> int:
        # successorが自分自身である場合に用いられる場合を考慮し、base_id と target_id が一致する場合は
        # 距離0と考えることもできるが、一周分を距離として返す
        if base_id == target_id:
            return ID_SPACE_RANGE - 1

        # 0をまたいだ場合に考えやすくするためにtarget_idを0にずらし、base_idを
        # 同じ数だけずらす
        slided_target_id = 0
        slided_base_id = base_id - target_id
        if(slided_base_id < 0):
            # マイナスの値をとった場合は値0を通り越しているので
            # それにあった値に置き換える
            slided_base_id = ID_MAX + slided_base_id

        # あとは差をとって、符号を逆転させる（前方は値が小さくなる方向を意味するため）
        distance = -1 * (slided_target_id - slided_base_id)

        # 求めた値が負の値の場合は入力された値において base_id < target_id
        # であった場合であり、前方をたどった場合の距離は ID_MAX から得られた値
        # の絶対値を引いたものであり、ここでは負の値となっているのでそのまま加算
        # すればよい
        if distance < 0:
            distance = ID_MAX + distance

        return distance

    # ID空間が環状になっていることを踏まえて base_id から後方をたどった場合の
    # ノード間の距離を求める
    # ここで後方とは、IDの値が大きくなる方向である
    @classmethod
    def calc_distance_between_nodes_right_mawari(cls, base_id : int, target_id : int) -> int:
        # successorが自分自身である場合に用いられる場合を考慮し、base_id と target_id が一致する場合は
        # 距離0と考えることもできるが、一周分を距離として返す
        if base_id == target_id:
            return ID_SPACE_RANGE - 1

        # 0をまたいだ場合に考えやすくするためにtarget_idを0にずらし、base_idを
        # 同じ数だけずらす
        slided_base_id = 0
        slided_target_id = target_id - base_id
        if(slided_target_id < 0):
            # マイナスの値をとった場合は値0を通り越しているので
            # それにあった値に置き換える
            slided_target_id = ID_MAX + slided_target_id

        # あとは単純に差をとる
        distance = slided_target_id - slided_base_id

        # 求めた値が負の値の場合は入力された値において target_id < base_id
        # であった場合であり、前方をたどった場合の距離は ID_MAX から得られた値
        # の絶対値を引いたものであり、ここでは負の値となっているのでそのまま加算
        # すればよい
        if distance < 0:
            distance = ID_MAX + distance

        return distance

    # from_id から IDが大きくなる方向にたどった場合に、 end_id との間に
    # target_idが存在するか否かを bool値で返す
    @classmethod
    def exist_between_two_nodes_right_mawari(cls, from_id : int, end_id : int, target_id : int) -> bool:
        distance_end = ChordUtil.calc_distance_between_nodes_right_mawari(from_id, end_id)
        distance_target = ChordUtil.calc_distance_between_nodes_right_mawari(from_id, target_id)

        if distance_target < distance_end:
            return True
        else:
            return False

    # successor, predecessorを経路表に入れる際に適切なエントリに入れるために
    # 対応するインデックスを求めて返す
    # 設定するインデックスは node_id より値が大きくてもっとも近いエントリの
    # インデックスとなる
    # 値が大きいエントリに入れるのは、担当ノードとしては、そのノードのsuccessorを
    # 用いるためである
    @classmethod
    def calc_idx_of_ftable_from_node_id(cls, from_node_id : int, target_node_id : int) -> int:
        distance : int = ChordUtil.calc_distance_between_nodes_right_mawari(from_node_id, target_node_id)
        if distance == 0:
            # 同じノードを比較しており、2^0である 1 よりも距離が小さいので、finger_tableには入れない
            # ようにさせる
            return -1

        log2_value = math.log2(distance)
        ceiled_value = math.floor(log2_value)

        return ceiled_value - 1 # 0オリジンのため

    @classmethod
    def dprint(cls, print_str : str):
        print(str(datetime.datetime.now()) + "," + print_str)

    @classmethod
    def print_no_lf(cls, print_str : str):
        print(print_str, end="")

    @classmethod
    def gen_debug_str_of_node(cls, node_info : 'NodeInfo') -> str:
        return str(node_info.born_id) + "," + hex(node_info.node_id) + "," \
               + ChordUtil.conv_id_to_ratio_str(node_info.node_id)

    @classmethod
    def gen_debug_str_of_data(cls, data_id : int) -> str:
        return hex(data_id) + "," + ChordUtil.conv_id_to_ratio_str(data_id)


# all_data_listグローバル変数に格納される形式としてのみ用いる
class KeyValue:
    def __init__(self, key, value):
        self.key : str = key
        self.value : str = value
        # keyのハッシュ値
        self.data_id : int = ChordUtil.hash_str_to_int(key)

class NodeInfo:

    def __init__(self):
        self.node_id : int = None
        self.address_str : str = None

        # デバッグ用のID（実システムには存在しない）
        # 何ノード目として生成されたかの値
        self.born_id : int = None

        # NodeInfoオブジェクトを保持
        self.successor_info : 'NodeInfo' = None
        self.predecessor_info : 'NodeInfo' = None

        # NodeInfoオブジェクトを要素として持つリスト
        # インデックスの小さい方から狭い範囲が格納される形で保持する
        # sha1で生成されるハッシュ値は160bit符号無し整数であるため要素数は160となる

        # TODO: 現在は ID_SPACE_BITS が検証時の実行時間の短縮のため30となっている
        self.finger_table : List['NodeInfo'] = [None] * ID_SPACE_BITS

class ChordNode:

    # join時の処理もコンストラクタで行う
    #def __init__(self, node_address : str, first_node = False, second_node = False, third_node = False):
    def __init__(self, node_address: str, first_node=False):
        global already_born_node_num
        # global g_first_node_info

        self.node_info = NodeInfo()
        # KeyもValueもどちらも文字列. Keyはハッシュを通されたものなので元データの値とは異なる
        self.stored_data : Dict[str, str] = {}

        # ミリ秒精度のUNIXTIMEから自身のアドレスにあたる文字列と、Chorネットワーク上でのIDを決定する
        self.node_info.address_str = ChordUtil.gen_address_str()
        self.node_info.node_id = ChordUtil.hash_str_to_int(self.node_info.address_str)

        already_born_node_num += 1
        self.node_info.born_id = already_born_node_num

        if first_node:
            # 最初の1ノードの場合

            # successorは自身として終了する
            self.node_info.successor_info = self.node_info
            return
        else:
            self.join(node_address)

    # node_addressに対応するノードに問い合わせを行い、教えてもらったノードをsuccessorとして設定する
    def join(self, node_address : str):
        # TODO: あとで、ちゃんとノードに定義されたAPIを介して情報をやりとりするようにする必要あり

        tyukai_node = all_node_dict[node_address]
        # 仲介ノードに自身のsuccessorになるべきノードを探してもらう
        successor = tyukai_node.global_query_node(self.node_info.node_id)
        # TODO: successorが None でないかチェックし、Noneであった場合は一定時間待ってから
        #       global_query_nodeの呼び出しをリトライするようにする
        self.node_info.successor_info = successor.node_info

        # 自ノードの情報、仲介ノードの情報、successorとして設定したノードの情報
        ChordUtil.dprint("join," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(tyukai_node.node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(self.node_info.successor_info))

    def global_put(self, data_id : int, value_str : str):
        target_node = self.find_successor(data_id)
        if target_node == None:
            # TODO: ノード探索が失敗した場合は、一定時間を空けてリトライするようにする
            ChordUtil.dprint("global_put_1," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                             + ChordUtil.gen_debug_str_of_data(data_id))
            raise Exception("appropriate node is not found.")
            # return

        target_node.put(data_id, value_str)
        ChordUtil.dprint("global_put_2," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(target_node.node_info) + ","
                         + ChordUtil.gen_debug_str_of_data(data_id))

    def put(self, data_id : int, value_str : str):
        key_id_str = str(data_id)
        self.stored_data[key_id_str] = value_str
        ChordUtil.dprint("put," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_data(data_id))

    # 得られた value の文字列を返す
    def global_get(self, data_id : int):
        ChordUtil.dprint("global_get_0," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_data(data_id))

        target_node = self.find_successor(data_id)
        if target_node == None:
            # TODO: ノード探索が失敗した場合は、一定時間を空けてリトライするようにする
            ChordUtil.dprint("global_get_1," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                  + ChordUtil.gen_debug_str_of_data(data_id))
            raise Exception("appropriate node is not found.")
            # return

        got_value_str = target_node.get(data_id)
        # TODO: 返ってきた値が "QUERIED_KEY_WAS_NOT_FOUND" だった場合、target_nodeから
        #       一定数のsuccessorを辿ってそれぞれにも data_id に対応するデータを持っていないか問い合わせるようにする
        ChordUtil.dprint("global_get_2," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
              + ChordUtil.gen_debug_str_of_node(target_node.node_info) + ","
              + ChordUtil.gen_debug_str_of_data(data_id) + "," + got_value_str)
        return got_value_str

    # 得られた value の文字列を返す
    def get(self, data_id : int):
        ret_value_str = None
        try:
            ret_value_str = self.stored_data[str(data_id)]
        except:
            ret_value_str = "QUERIED_KEY_WAS_NOT_FOUND"

        ChordUtil.dprint("get," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_data(data_id) + "," + ret_value_str)
        return ret_value_str

    # node_id をデータのIDとして見た際にそれを担当するノードを返す
    # joinする際の適切な位置を求めるために利用される
    #（data_idがjoinしてくる新規ノードのものの場合は、当該ノードの successor になるノードが返ることになる）
    def global_query_node(self, node_id : int) -> 'ChordNode':
        ChordUtil.dprint("global_query_node_0," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_data(node_id))

        found_node = self.find_successor(node_id)
        if found_node == None:
            # TODO: ノード探索が失敗した場合は、一定時間を空けてリトライするようにする
            ChordUtil.dprint("global_query_node_1," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                             + ChordUtil.gen_debug_str_of_data(node_id))
            raise Exception("appropriate node is not found.")

        ChordUtil.dprint("global_query_node_2," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(found_node.node_info) + ","
                         + ChordUtil.gen_debug_str_of_data(node_id))

        return found_node

    # TODO: Deleteの実装
    # def global_delete(self, key_str):
    #     print("not implemented yet")
    #
    # def delete(self, key_str):
    #     print("not implemented yet")

    # id が自身の正しい predecessor でないかチェックし、そうであった場合、経路表の情報を更新する
    # 本メソッドはstabilize処理の中で用いられる
    def check_predecessor(self, id : int, node_info : 'NodeInfo'):
        if self.node_info.predecessor_info == None:
            # 未設定状態なので確認するまでもなく、predecessorらしいと判断し
            # 経路情報に設定し、処理を終了する
            self.node_info.predecessor_info = node_info
            ChordUtil.dprint("check_predecessor_1," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                  + ChordUtil.gen_debug_str_of_node(self.node_info.successor_info) + ","
                  + ChordUtil.gen_debug_str_of_node(self.node_info.predecessor_info))

            return

        ChordUtil.dprint("check_predecessor_2," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
              + ChordUtil.gen_debug_str_of_node(self.node_info.successor_info))

        distance_check = ChordUtil.calc_distance_between_nodes_left_mawari(self.node_info.node_id, id)
        distance_cur = ChordUtil.calc_distance_between_nodes_left_mawari(self.node_info.node_id, self.node_info.predecessor_info.node_id)

        # 確認を求められたノードの方が現在の predecessor より predecessorらしければ
        # 経路表の情報を更新する
        if distance_check < distance_cur:
            self.node_info.predecessor_info = node_info

            ChordUtil.dprint("check_predecessor_3," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                  + ChordUtil.gen_debug_str_of_node(self.node_info.successor_info) + ","
                  + ChordUtil.gen_debug_str_of_node(self.node_info.predecessor_info))

    # successorおよびpredicessorに関するstabilize処理を行う
    # predecessorはこの呼び出しで初めて設定される
    def stabilize_successor(self):
        ChordUtil.dprint("stabilize_successor_1," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
              + ChordUtil.gen_debug_str_of_node(self.node_info.successor_info))

        # 自身のsuccessorに、当該ノードが認識しているpredecessorを訪ねる
        successor_info = self.node_info.successor_info
        if successor_info.predecessor_info == None:
            # successor が predecessor を未設定であった場合は自身を predecessor として保持させて
            # 処理を終了する
            successor_info.predecessor_info = self.node_info

            ChordUtil.dprint("stabilize_successor_2," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                             + ChordUtil.gen_debug_str_of_node(self.node_info.successor_info))
            return

        ChordUtil.dprint("stabilize_successor_3," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(self.node_info.successor_info))

        pred_id_of_successor = successor_info.predecessor_info.node_id

        ChordUtil.dprint("stabilize_successor_3_5," + hex(pred_id_of_successor))

        # 下のパターン1から3という記述は以下の資料による説明に基づく
        # https://www.slideshare.net/did2/chorddht
        if(pred_id_of_successor == self.node_info.node_id):
            # パターン1
            # 特に訂正は不要なので処理を終了する
            return
        else:
            # 以下、パターン2およびパターン3に対応する処理

            # 自身がsuccessorにとっての正しいpredecessorでないか確認を要請し必要であれば
            # 情報を更新してもらう
            # 事前チェックによって避けられるかもしれないが、常に実行する
            successor_obj = all_node_dict[successor_info.address_str]
            successor_obj.check_predecessor(self.node_info.node_id, self.node_info)

            distance_unknown = ChordUtil.calc_distance_between_nodes_left_mawari(successor_obj.node_info.node_id, pred_id_of_successor)
            distance_me = ChordUtil.calc_distance_between_nodes_left_mawari(successor_obj.node_info.node_id, self.node_info.node_id)
            if distance_unknown < distance_me:
                # successorの認識しているpredecessorが自身ではなく、かつ、そのpredecessorが
                # successorから自身に対して前方向にたどった場合の経路中に存在する場合
                # 自身の認識するsuccessorの情報を更新する

                self.node_info.successor_info = successor_obj.node_info.predecessor_info

                # 新たなsuccessorに対して自身がpredecessorでないか確認を要請し必要であれ
                # ば情報を更新してもらう
                new_successor_obj = all_node_dict[self.node_info.successor_info.address_str]
                new_successor_obj.check_predecessor(self.node_info.node_id, self.node_info)

                ChordUtil.dprint("stabilize_successor_4," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(self.node_info.successor_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(successor_obj.node_info))

    # FingerTableに関するstabilize処理を行う
    # 一回の呼び出しで1エントリを更新する
    # FingerTableのエントリはこの呼び出しによって埋まっていく
    def stabilize_finger_table(self, idx):
        ChordUtil.dprint("stabilize_finger_table_1," + ChordUtil.gen_debug_str_of_node(self.node_info))

        # FingerTableの各要素はインデックスを idx とすると 2^IDX 先までを担当する、もしくは
        # 担当するノードに最も近いノードが格納される
        update_id = ChordUtil.overflow_check_and_conv(self.node_info.node_id + 2**idx)
        found_node = self.find_successor(update_id)
        if found_node == None:
            ChordUtil.dprint("stabilize_finger_table_2," + ChordUtil.gen_debug_str_of_node(self.node_info))
            return

        self.node_info.finger_table[idx] = found_node.node_info

        ChordUtil.dprint("stabilize_finger_table_3," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(found_node.node_info))

    # id（int）で識別されるデータを担当するノードの名前解決を行う
    def find_successor(self, id : int):
        ChordUtil.dprint("find_successor_1," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
              + ChordUtil.gen_debug_str_of_data(id))

        n_dash = self.find_predecessor(id)
        if n_dash == None:
            ChordUtil.dprint("find_successor_2," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                             + ChordUtil.gen_debug_str_of_data(id))
            return None

        ChordUtil.dprint("find_successor_3," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(n_dash.node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(self.node_info.successor_info) + ","
                         + ChordUtil.gen_debug_str_of_data(id))

        return all_node_dict[n_dash.node_info.successor_info.address_str]

    # id(int)　の前で一番近い位置に存在するノードを探索する
    def find_predecessor(self, id: int):
        ChordUtil.dprint("find_predecessor_1," + ChordUtil.gen_debug_str_of_node(self.node_info))

        n_dash = self
        # n_dash と n_dashのsuccessorの 間に id が位置するような n_dash を見つけたら、ループを終了し n_dash を return する
        #while not (n_dash.node_info.predecessor_info.node_id < id and id <= n_dash.node_info.successor_info.node_id):
        while not ChordUtil.exist_between_two_nodes_right_mawari(n_dash.node_info.node_id, n_dash.node_info.successor_info.node_id, id):
            ChordUtil.dprint("find_predecessor_2," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                             + ChordUtil.gen_debug_str_of_node(n_dash.node_info))
            n_dash_found = n_dash.closest_preceding_finger(id)

            if n_dash_found.node_info.node_id == n_dash.node_info.node_id:
                # 見つかったノードが、n_dash と同じで、変わらなかった場合
                # 同じを経路表を用いて探索することになり、結果は同じになり無限ループと
                # なってしまうため、探索は継続せず、探索結果として n_dash (= n_dash_found) を返す
                ChordUtil.dprint("find_predecessor_3," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(n_dash.node_info))
                return n_dash_found

            # closelst_preceding_finger は id を通り越してしまったノードは返さない
            # という前提の元で以下のチェックを行う
            distance_old = ChordUtil.calc_distance_between_nodes_right_mawari(self.node_info.node_id, n_dash.node_info.node_id)
            distance_found = ChordUtil.calc_distance_between_nodes_right_mawari(self.node_info.node_id, n_dash_found.node_info.node_id)
            distance_data_id = ChordUtil.calc_distance_between_nodes_right_mawari(self.node_info.node_id, id)
            if distance_found < distance_old and not (distance_old >= distance_data_id):
                # 探索を続けていくと n_dash は id に近付いていくはずであり、それは上記の前提を踏まえると
                # 自ノードからはより遠い位置の値になっていくということのはずである
                # 従って、そうなっていなかった場合は、繰り返しを継続しても意味が無く、最悪、無限ループになってしまう
                # 可能性があるため、探索を打ち切り、探索結果は古いn_dashを返す
                # ただし、古い n_dash が 一回目の探索の場合 self であり、同じ node_idの距離は ID_SPACE_RANGE となるようにしている
                # ため、上記の条件が常に成り立ってしまう. 従って、その場合は例外とする（n_dashが更新される場合は、更新されたn_dashのnode_idが
                # 探索対象のデータのid を通り越すことは無い）

                ChordUtil.dprint("find_predecessor_4," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(n_dash.node_info))

                return n_dash

            ChordUtil.dprint("find_predecessor_5_n_dash_updated," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                             + ChordUtil.gen_debug_str_of_node(n_dash.node_info) + "->"
                             + ChordUtil.gen_debug_str_of_node(n_dash_found.node_info))

            # チェックの結果問題ないので n_dashを closest_preceding_fingerで探索して得た
            # ノード情報 n_dash_foundに置き換える
            n_dash = n_dash_found

        return n_dash

    #  自身の持つ経路情報をもとに,  id から前方向に一番近いノードの情報を返す
    def closest_preceding_finger(self, id : int) -> 'ChordNode':
        # 範囲の広いエントリから探索していく
        # finger_tableはインデックスが小さい方から大きい方に、範囲が大きくなっていく
        # ように構成されているため、リバースしてインデックスの大きな方から小さい方へ
        # 順に見ていくようにする
        for entry in reversed(self.node_info.finger_table):
            # 埋まっていないエントリも存在し得る
            if entry == None:
                ChordUtil.dprint("closest_preceding_finger_0," + ChordUtil.gen_debug_str_of_node(self.node_info))
                continue

            ChordUtil.dprint("closest_preceding_finger_1," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                  + ChordUtil.gen_debug_str_of_node(entry))

            # テーブル内のエントリが保持しているノードのIDが自身のIDと探索対象のIDの間にあれば
            # それを返す
            # (大きな範囲を見た場合、探索対象のIDが自身のIDとエントリが保持しているノードのIDの中に含まれて
            #  しまっている可能性が高く、エントリが保持しているノードが、探索対象のIDを飛び越してしまっている
            #  可能性が高いということになる。そこで探索範囲を狭めていって、飛び越さない範囲で一番近いノードを
            #  見つけるという処理になっていると思われる）
            # #if self.node_info.node_id < entry.node_id and entry.node_id <= id:
            if ChordUtil.exist_between_two_nodes_right_mawari(self.node_info.node_id, id, entry.node_id):
                ChordUtil.dprint("closest_preceding_finger_2," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(entry))
                return all_node_dict[entry.address_str]

        ChordUtil.dprint("closest_preceding_finger_3")

        # どんなに範囲を狭めても探索対象のIDを超えてしまうノードしか存在しなかった場合
        # 自身の知っている情報の中で対象を飛び越さない範囲で一番近いノードは自身という
        # ことになる
        return self

# ネットワークに存在するノードから1ノードをランダムに取得する
# ChordNodeオブジェクトを返す
def get_a_random_node() -> 'ChordNode':
    key_list : List[str] = list(all_node_dict.keys())
    selected_key : str = ChordUtil.get_random_elem(key_list)
    return all_node_dict[selected_key]

# stabilize_successorの呼び出しが一通り終わったら確認するのに利用する
# ランダムに選択したノードからsuccessor方向にsuccessorの繋がりでノードを辿って
# 行って各ノードの情報を出力する
# また、predecessorの方向にpredecesorの繋がりでもたどって出力する
def check_nodes_connectivity():
    ChordUtil.dprint("check_nodes_connectivity_1")
    print("flush", flush=True)
    counter : int = 0
    # まずはsuccessor方向に辿る
    cur_node_info : NodeInfo = get_a_random_node().node_info
    ChordUtil.print_no_lf("check_nodes_connectivity__succ")
    print(",", flush=True, end="")
    while counter < 20:
        ChordUtil.print_no_lf(str(cur_node_info.born_id) + "," + ChordUtil.conv_id_to_ratio_str(cur_node_info.node_id) + " -> ")
        cur_node_info = cur_node_info.successor_info
        if cur_node_info == None:
            break
        counter += 1
    print("")

    # 続いてpredecessor方向に辿る
    counter = 0
    cur_node_info = get_a_random_node().node_info
    ChordUtil.print_no_lf("check_nodes_connectivity__pred")
    print(",", flush=True, end="")
    while counter < 20:
        ChordUtil.print_no_lf(str(cur_node_info.born_id) + "," + ChordUtil.conv_id_to_ratio_str(cur_node_info.node_id) + " -> ")
        cur_node_info = cur_node_info.predecessor_info
        if cur_node_info == None:
            break
        counter += 1
    print("")

# ランダムに仲介ノードを選択し、そのノードに仲介してもらう形でネットワークに参加させる
def add_new_node():
    global lock_of_all_data
    global all_node_dict

    # ロックの取得
    lock_of_all_data.acquire()

    tyukai_node = get_a_random_node()
    new_node = ChordNode(tyukai_node.node_info.address_str)
    all_node_dict[new_node.node_info.address_str] = new_node

    # ロックの解放
    lock_of_all_data.release()

# all_node_id辞書のvaluesリスト内から重複なく選択したノードに stabilize のアクションをとらせていく
def do_stabilize_once_at_all_node():
    global lock_of_all_data

    done_stabilize_successor_cnt = 0
    done_stabilize_ftable_cnt = 0

    node_list = list(all_node_dict.values())

    # 各リストはpopメソッドで要素を取り出して利用されていく
    # 同じノードは複数回利用されるため、その分コピーしておく（参照がコピーされるだけ）
    shuffled_node_list_successor = random.sample(node_list, len(node_list))
    shuffled_node_list_successor = shuffled_node_list_successor * STABILIZE_SUCCESSOR_BATCH_TIMES
    shuffled_node_list_ftable = random.sample(node_list, len(node_list))
    shuffled_node_list_ftable = shuffled_node_list_ftable * STABILIZE_FTABLE_BATCH_TIMES

    cur_node_num = len(node_list)
    selected_operation = "" # "successor" or "ftable"

    while True:
        # ロックの取得
        lock_of_all_data.acquire()

        try:
            # まず行う処理を決定する
            if done_stabilize_successor_cnt >= STABILIZE_SUCCESSOR_BATCH_TIMES * cur_node_num \
                and done_stabilize_ftable_cnt >= STABILIZE_FTABLE_BATCH_TIMES * cur_node_num:
                # 関数呼び出し時点で存在した全ノードについて、2種双方が規定回数の stabilze処理を完了したため
                # 関数を終了する

                # ノードの接続状況をデバッグ出力
                check_nodes_connectivity()
                return
            elif done_stabilize_successor_cnt >= STABILIZE_SUCCESSOR_BATCH_TIMES * cur_node_num:
                # 一方は完了しているので他方を実行する
                selected_operation = "ftable"
            elif done_stabilize_ftable_cnt >= STABILIZE_FTABLE_BATCH_TIMES * cur_node_num:
                # 一方は完了しているので他方を実行する
                selected_operation = "successor"
            else:
                # どちらも完了していない
                zero_or_one = random.randint(0,1)

                if zero_or_one == 0:
                    selected_operation = "successor"
                else: # 1
                    selected_operation = "ftable"


            # 選択された処理を実行する
            if selected_operation == "successor":
                node = shuffled_node_list_successor.pop()
                node.stabilize_successor()
                ChordUtil.dprint("do_stabilize_on_random_node__successor," + ChordUtil.gen_debug_str_of_node(node.node_info) + ","
                                   + str(done_stabilize_successor_cnt))
                done_stabilize_successor_cnt += 1
            else: # "ftable"
                node = shuffled_node_list_ftable.pop()
                # 対象ノードについてテーブルの下から順に全て更新する
                for idx in range(0, ID_SPACE_BITS):
                    ChordUtil.dprint(
                        "do_stabilize_on_random_node__ftable," + ChordUtil.gen_debug_str_of_node(node.node_info) + ","
                        + str(idx))
                    node.stabilize_finger_table(idx)
                done_stabilize_ftable_cnt += 1
        finally:
            # ロックの解放
            lock_of_all_data.release()



# 適当なデータを生成し、IDを求めて、そのIDなデータを担当するChordネットワーク上のノードの
# アドレスをよろしく解決し、見つかったノードにputの操作を依頼する
def do_put_on_random_node():
    global lock_of_all_data

    # ロックの取得
    lock_of_all_data.acquire()

    unixtime_str = str(time.time())
    # ミリ秒精度で取得したUNIXTIMEを文字列化してkeyとvalueに用いる
    kv_data = KeyValue(unixtime_str, unixtime_str)
    node = get_a_random_node()
    node.global_put(kv_data.data_id, kv_data.value)
    all_data_list.append(kv_data)

    # ロックの解放
    lock_of_all_data.release()

# グローバル変数であるall_data_listからランダムにデータを選択し、そのデータのIDから
# Chordネットワーク上の担当ノードのアドレスをよろしく解決し、見つかったノードにgetの操作を依頼する
def do_get_on_random_node():
    global lock_of_all_data

    # ロックの取得
    lock_of_all_data.acquire()

    # まだ put が行われていなかったら何もせずに終了する
    if len(all_data_list) == 0:
        lock_of_all_data.release()
        return

    target_data = ChordUtil.get_random_elem(all_data_list)
    target_data_id = target_data.data_id

    node = get_a_random_node()
    node.global_get(target_data_id)

    # ロックの解放
    lock_of_all_data.release()

def node_join_th():
    while already_born_node_num < NODE_NUM:
        add_new_node()
        time.sleep(1)  # sleep 1sec

def stabilize_th():
    while True:
        # 内部で適宜ロックを解放することで他のスレッドの処理も行えるようにしつつ
        # 呼び出し時点でのノードリストを対象に stabilize 処理を行う
        do_stabilize_once_at_all_node()

def data_put_th():
    while True:
        do_put_on_random_node()
        time.sleep(1)  # sleep 1sec

def data_get_th():
    while True:
        # 内部でデータのputが一度も行われていなければreturnしてくるので
        # putを行うスレッドと同時に動作を初めても問題ないようにはなっている
        do_get_on_random_node()
        # エンドレスで行うのでデバッグプリントのサイズが大きくなり過ぎないよう
        # sleepを挟む
        time.sleep(1) # sleep 1sec

def main():
    global all_node_dict

    # 再現性のため乱数シードを固定
    # ただし、複数スレッドが存在し、個々の処理の終了するタイミングや、どのタイミングで
    # スイッチするかは実行毎に異なる可能性があるため、あまり意味はないかもしれない
    random.seed(1337)

    # 最初の1ノードはここで登録する
    first_node = ChordNode("THIS_VALUE_IS_NOT_USED", first_node=True)
    all_node_dict[first_node.node_info.address_str] = first_node
    # # 1ノードしかいなくても stabilize処理は走らせる
    # do_stabilize_once_at_all_node()

    node_join_th_handle = threading.Thread(target=node_join_th, daemon=True)
    node_join_th_handle.start()

    stabilize_th_handle = threading.Thread(target=stabilize_th, daemon=True)
    stabilize_th_handle.start()

    # node_join_and_stabilize_th_handle = threading.Thread(target=node_join_and_stabilize_th, daemon=True)
    # node_join_and_stabilize_th_handle.start()

    data_put_th_handle = threading.Thread(target=data_put_th, daemon=True)
    data_put_th_handle.start()

    data_get_th_handle = threading.Thread(target=data_get_th, daemon=True)
    data_get_th_handle.start()

    while True:
        time.sleep(1)

if __name__ == '__main__':
    main()

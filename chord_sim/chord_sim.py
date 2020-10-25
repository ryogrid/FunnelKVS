# coding:utf-8

import threading
import time
import random
import hashlib
import datetime
from typing import Dict, List, Any

# 160bit符号なし整数の最大値
# Chordネットワーク上のID空間の上限
ID_MAX = 2**160 - 1

# アドレス文字列をキーとしてとり、対応するノードのChordNodeオブジェクトを返すハッシュ
# IPアドレスが分かれば、対応するノードと通信できることと対応している
all_node_dict : Dict[str, 'ChordNode'] = {}

# DHT上で保持されている全てのデータが保持されているリスト
# KeyValueオブジェクトを要素として持つ
# 全てのノードはputの際はDHTにデータをputするのとは別にこのリストにデータを追加し、
# getする際はDHTに対してgetを発行するためのデータをこのリストからランダム
# に選び、そのkeyを用いて探索を行う. また value も一時的に保持しておき、取得できた内容と
# 一致しているか確認する
all_data_list : List['KeyValue'] = []

# 検証を分かりやすくするために何ノード目として生成されたか
# のデバッグ用IDを持たせるためのカウンタ
already_born_node_num = 0

is_stabiize_finished = False

lock_of_all_data = threading.Lock()

done_stabilize_successor_cnt = 0

class ChordUtil:
    # 任意の文字列をハッシュ値（定められたbit数で表現される整数値）に変換しint型で返す
    # アルゴリズムはSHA1, 160bitで表現される正の整数となる
    # メモ: 10進数の整数は組み込みの hex関数で 16進数表現での文字列に変換可能
    @classmethod
    def hash_str_to_int(cls, input_str : str) -> int:
        hash_hex_str = hashlib.sha1(input_str.encode()).hexdigest()
        hash_id_num = int(hash_hex_str, 16)
        return hash_id_num

    # 与えたリストの要素のうち、ランダムに選択した1要素を返す
    @classmethod
    def get_random_elem(cls, list_like : List[Any]) -> 'ChordNode':
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
            ret_id = ID_MAX + 1
        return id

    # TODO: idがID空間の最大値に対して何パーセントの位置かを適当な精度の浮動小数の文字列
    #       にして返す
    @classmethod
    def conv_id_to_ratio_str(cls, id : int) -> str:
        ratio = (id / ID_MAX) * 100.0
        return '%2.4f' % ratio

    # ID空間が環状になっていることを踏まえて base_id から前方をたどった場合の
    # ノード間の距離を求める
    # ここで前方とは、IDの値が小さくなる方向である
    @classmethod
    def calc_distance_between_nodes_left_mawari(cls, base_id : int, target_id : int) -> int:
        # 0をまたいだ場合に考えやすくするためにtarget_idを0にずらし、base_idを
        # 同じ数だけずらす
        slided_target_id = 0
        slided_base_id = base_id - target_id
        if(slided_base_id < 0):
            # マイナスの値をとった場合は値0を通り越しているので
            # それにあった値に置き換える
            slided_base_id = ID_MAX - slided_base_id

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
        # 0をまたいだ場合に考えやすくするためにtarget_idを0にずらし、base_idを
        # 同じ数だけずらす
        slided_base_id = 0
        slided_target_id = target_id - base_id
        if(slided_target_id < 0):
            # マイナスの値をとった場合は値0を通り越しているので
            # それにあった値に置き換える
            slided_target_id = ID_MAX - slided_target_id

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

    @classmethod
    def dprint(cls, print_str : str):
        print(str(datetime.datetime.now()) + "," + print_str)

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
        self.finger_table : List['NodeInfo'] = [None] * 160

class ChordNode:

    # join時の処理もコンストラクタで行う
    def __init__(self, node_address : str, first_node = False, second_node = False):
        global already_born_node_num

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

            # # joinメソッド内で仲介ノードを引く際に自身が登録されていないとエラー
            # # となるため、このタイミングで all_node_dict に登録する
            # all_node_dict[self.node_info.address_str] = self

            # TODO: 初期ノードの初期化がこれで問題ないか確認する
            # # 自身を仲介ノード（successorに設定される）としてネットワークに参加する
            # self.join(self.node_info.address_str)

            # successorを None のまま終了する
            return
        elif second_node:
            # 2番目にネットワークに参加するノードの場合

            # 1番目にネットワークに参加したノードはsuccessorを持たない状態となって
            # いるため自身をsuccessorとして設定しておく
            first_node = all_node_dict[node_address]
            first_node.node_info.successor_info = self.node_info

            self.join(node_address)
        else:
            self.join(node_address)

    # node_addressに対応するノードをsuccessorとして設定する
    def join(self, node_address : str):
        # TODO: あとで、ちゃんとノードに定義されたAPIを介して情報をやりとりするようにする必要あり
        successor = all_node_dict[node_address]

        self.node_info.successor_info = successor.node_info

        # TODO: 最低限、一つはエントリが埋まっていないと、stabilize_finger_table自体が
        #       finger_tableを用いて探索を行うことでエントリを埋めていくので、うまく動かない
        #       のではないかと思うので、インデックス0の一番近い範囲のエントリにはとりあえず
        #       successorを設定しておいてみる
        self.node_info.finger_table[0] = self.node_info.successor_info

        # 自ノードの生成ID、自ノードのID（16進表現)、仲介ノード（初期ノード、successorとして設定される）のID(16進表現)
        ChordUtil.dprint("join," + str(self.node_info.born_id) + "," +
              hex(self.node_info.node_id) + "," + hex(self.node_info.successor_info.node_id) + ","
              + self.node_info.address_str + "," + self.node_info.successor_info.address_str + ","
              + ChordUtil.conv_id_to_ratio_str(self.node_info.node_id) + ","
              + ChordUtil.conv_id_to_ratio_str(self.node_info.successor_info.node_id))

    def global_put(self, key_str : str, value_str : str):
        # resolve ID to address of a node which is assigned ID range the ID is included to
        # 注: 現状、ここでは対象のChordNordオブジェクトを直接取得してしまっており、正確にはアドレスの解決ではない
        data_id = ChordUtil.hash_str_to_int(key_str)
        target_node = self.find_successor(data_id)
        if target_node == None:
            ChordUtil.dprint("global_put_1," + str(self.node_info.born_id) + "," + hex(self.node_info.node_id) + ","
                  + hex(data_id) + "," + key_str + "," + value_str)
            return

        target_node.put(key_str, value_str)
        ChordUtil.dprint("global_put_2," + str(self.node_info.born_id) + "," + hex(self.node_info.node_id) + ","
              + ChordUtil.conv_id_to_ratio_str(self.node_info.node_id) + ","
              + hex(target_node.node_info.node_id) + "," + ChordUtil.conv_id_to_ratio_str(target_node.node_info.node_id) + ","
              + str(data_id) + "," + key_str + "," + value_str)

    def put(self, key_str : str, value_str : str):
        key_id_str = str(ChordUtil.hash_str_to_int(key_str))
        self.stored_data[key_id_str] = value_str
        ChordUtil.dprint("put," + str(self.node_info.born_id) + "," + hex(self.node_info.node_id) + "," + key_id_str + "," + key_str + "," + value_str)

    # 得られた value の文字列を返す
    def global_get(self, data_id : int, key_str : str):
      # resolve ID to address of a node which is assigned ID range the ID is included to
        # 注: 現状、ここでは対象のChordNordオブジェクトを直接取得してしまっており、正確にはアドレスの解決ではない
        ChordUtil.dprint("global_get_0," + str(self.node_info.born_id) + "," + hex(self.node_info.node_id) + ","
                         + hex(data_id) + "," + key_str)

        target_node = self.find_successor(data_id)
        if target_node == None:
            ChordUtil.dprint("global_get_1," + str(self.node_info.born_id) + "," + hex(self.node_info.node_id) + ","
                  + hex(data_id) + "," + key_str)
            return

        key_id_str = str(data_id)
        got_value_str = target_node.get(key_id_str)
        ChordUtil.dprint("global_get_2," + str(self.node_info.born_id) + "," + hex(self.node_info.node_id) + ","
              + ChordUtil.conv_id_to_ratio_str(self.node_info.node_id) + ","
              + hex(target_node.node_info.node_id) + "," + ChordUtil.conv_id_to_ratio_str(target_node.node_info.node_id) + ","
              + key_id_str + "," + key_str + "," + got_value_str)
        return got_value_str

    # 得られた value の文字列を返す
    def get(self, id_str : str):
        ret_value_str = None
        try:
            ret_value_str = self.stored_data[id_str]
        except:
            ret_value_str = "ASKED_KEY_NOT_FOUND"

        ChordUtil.dprint("get," + str(self.node_info.born_id) + "," + hex(self.node_info.node_id) + "," + id_str + "," + ret_value_str)
        return ret_value_str

    # TODO: global_delete (ひとまずglobal_getとglobal_putだけ実装するので後で良い）
    def global_delete(self, key_str):
        print("not implemented yet")
        
    # TODO: delete (ひとまずgetとputだけ実装するので後で良い）
    def delete(self, key_str):
        print("not implemented yet")

    # id が自身の正しい predecessor でないかチェックし、そうであった場合、経路表の情報を更新する
    # 本メソッドはstabilize処理の中で用いられる
    def check_predecessor(self, id : int, node_info : 'NodeInfo'):
        if self.node_info.predecessor_info == None:
            # 未設定状態なので確認するまでもなく、predecessorらしいと判断し
            # 経路情報に設定し、処理を終了する
            self.node_info.predecessor_info = node_info
            ChordUtil.dprint("check_predecessor_1," + str(self.node_info.born_id) + ","
                  + hex(self.node_info.node_id) + "," + hex(self.node_info.successor_info.node_id) + ","
                  + self.node_info.address_str + "," + self.node_info.successor_info.address_str + ","
                  + ChordUtil.conv_id_to_ratio_str(self.node_info.node_id) + ","
                  + ChordUtil.conv_id_to_ratio_str(self.node_info.successor_info.node_id))

            return

        ChordUtil.dprint("check_predecessor_2," + str(self.node_info.born_id) + ","
              + hex(self.node_info.node_id) + "," + hex(self.node_info.successor_info.node_id) + ","
              + self.node_info.address_str + "," + self.node_info.successor_info.address_str + ","
              + ChordUtil.conv_id_to_ratio_str(self.node_info.node_id) + ","
              + ChordUtil.conv_id_to_ratio_str(self.node_info.successor_info.node_id))

        distance_check = ChordUtil.calc_distance_between_nodes_left_mawari(self.node_info.node_id, id)
        distance_cur = ChordUtil.calc_distance_between_nodes_left_mawari(self.node_info.node_id, self.node_info.predecessor_info.node_id)

        # 確認を求められたノードの方が現在の predecessor より predecessorらしければ
        # 経路表の情報を更新する
        if distance_check < distance_cur:
            self.node_info.predecessor_info = node_info
            ChordUtil.dprint("check_predecessor_3," + str(self.node_info.born_id) + ","
                  + hex(self.node_info.node_id) + "," + hex(self.node_info.successor_info.node_id) + ","
                  + self.node_info.address_str + "," + self.node_info.successor_info.address_str + ","
                  + ChordUtil.conv_id_to_ratio_str(self.node_info.node_id) + ","
                  + ChordUtil.conv_id_to_ratio_str(self.node_info.successor_info.node_id))

    # successorおよびpredicessorに関するstabilize処理を行う
    # predecessorはこの呼び出しで初めて設定される
    def stabilize_successor(self):
        ChordUtil.dprint("stablize_succesor_1," + str(self.node_info.born_id) + ","
              + hex(self.node_info.node_id) + "," + hex(self.node_info.successor_info.node_id) + ","
              + self.node_info.address_str + "," + self.node_info.successor_info.address_str + ","
              + ChordUtil.conv_id_to_ratio_str(self.node_info.node_id) + ","
              + ChordUtil.conv_id_to_ratio_str(self.node_info.successor_info.node_id))

        # TODO: ここでjoin時には分からなかった自身の担当範囲が決定し、自身がjoinする
        #       までその範囲を担当していたノードから保持しているデータの委譲（コピーでも
        #       良いはず）を受ける必要があるかもしれない.
        #       　そうでなければ、successorを必要十分な数だけ複数持つことで委譲を不要
        #       とするか、定期的にデータを委譲する処理を走らせるかしてデータへの到達性
        #       を担保するのかもしれない.
        #       　ただし、全ノードが揃って、stabilizeも十分に行われた後にしか
        #       putを行わず、ノードの離脱が発生しないという条件であれば、保持データの
        #       委譲は不要にでき、successorも増やすことなく到達性が担保できるはずである.
        #       しかし、現実的にはそのようなシチュエーションは想定できないので、
        #       本シミュレータをひとまず動かすための暫定実装でのみ許される条件であろう.

        # 自身のsuccessorに、当該ノードが認識しているpredecessorを訪ねる
        successor_info = self.node_info.successor_info
        if successor_info.predecessor_info == None:
            # successor が predecessor を未設定であった場合は自身を predecessor として保持させて
            # 処理を終了する
            successor_info.predecessor_info = self.node_info
            ChordUtil.dprint("stablize_succesor_2," + str(self.node_info.born_id) + ","
                  + hex(self.node_info.node_id) + "," + hex(self.node_info.successor_info.node_id) + ","
                  + hex(self.node_info.successor_info.node_id) + ","
                  + self.node_info.address_str + "," + self.node_info.successor_info.address_str + ","
                  + ChordUtil.conv_id_to_ratio_str(self.node_info.node_id) + ","
                  + ChordUtil.conv_id_to_ratio_str(self.node_info.successor_info.node_id))
            return

        ChordUtil.dprint("stablize_succesor_3," + str(self.node_info.born_id) + ","
              + hex(self.node_info.node_id) + "," + hex(self.node_info.successor_info.node_id) + ","
              + hex(self.node_info.successor_info.node_id) + ","
              + self.node_info.address_str + "," + self.node_info.successor_info.address_str + ","
              + ChordUtil.conv_id_to_ratio_str(self.node_info.node_id) + ","
              + ChordUtil.conv_id_to_ratio_str(self.node_info.successor_info.node_id))

        pred_id_of_successor = successor_info.predecessor_info.node_id

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

    # FingerTableに関するstabilize処理を行う
    # 一回の呼び出しでランダムに選択した1エントリを更新する
    # FingerTableのエントリはこの呼び出しによって埋まっていく
    def stabilize_finger_table(self):
        ChordUtil.dprint("stabilize_finger_table_1," + str(self.node_info.born_id) + "," +
              hex(self.node_info.node_id) + "," + self.node_info.address_str + ","
              + ChordUtil.conv_id_to_ratio_str(self.node_info.node_id))

        length = len(self.node_info.finger_table)
        idx = random.randint(0, length - 1)
        # FingerTableの各要素はインデックスを idx とすると 2^IDX 先までを担当する、もしくは
        # 担当するノードに最も近いノードが格納される
        update_id = ChordUtil.overflow_check_and_conv(self.node_info.node_id + 2**idx)
        found_node = self.find_successor(update_id)
        if found_node == None:
            ChordUtil.dprint("stabilize_finger_table_2," + str(self.node_info.born_id) + "," +
                  hex(self.node_info.node_id) + "," + self.node_info.address_str + ","
                  + ChordUtil.conv_id_to_ratio_str(self.node_info.node_id))
            return

        self.node_info.finger_table[idx] = found_node.node_info

        ChordUtil.dprint("stabilize_finger_table_3," + str(self.node_info.born_id) + "," +
              hex(self.node_info.node_id) + "," + self.node_info.address_str + ","
              + ChordUtil.conv_id_to_ratio_str(self.node_info.node_id)
              + str(idx) + "," + hex(found_node.node_info.node_id) + ","
              + ChordUtil.conv_id_to_ratio_str(found_node.node_info.node_id))

    # id（int）で識別されるデータを担当するノードの名前解決を行う
    # TODO: あとで、実システムと整合するよう、ノードに定義されたAPIを介して情報をやりとりし、
    #       ノードオブジェクトを直接得るのではなく、all_node_dictを介して得るようにする必要あり
    def find_successor(self, id : int):
        # try:
        ChordUtil.dprint("find_successor_1," + str(self.node_info.born_id) + ","
              + hex(self.node_info.node_id) + ","
              + ChordUtil.conv_id_to_ratio_str(self.node_info.node_id) + "," + hex(id))
        # except TypeError:
        #     print("TypeError occur!!")
        #     print(self.node_info.node_id)
        #     print(type(self.node_info.node_id))

        n_dash = self.find_predecessor(id)
        if n_dash == None:
            ChordUtil.dprint("find_successor_2," + str(self.node_info.born_id) + ","
                + hex(self.node_info.node_id) + ","
                  + ChordUtil.conv_id_to_ratio_str(self.node_info.node_id) + "," + hex(id))
            return None

        ChordUtil.dprint("find_successor_3," + str(self.node_info.born_id) + ","
              + hex(self.node_info.node_id) + "," + hex(n_dash.node_info.node_id) + ","
              + hex(n_dash.node_info.successor_info.node_id) + ","
              + ChordUtil.conv_id_to_ratio_str(self.node_info.node_id) + ","
              + ChordUtil.conv_id_to_ratio_str(n_dash.node_info.node_id) + ","
              + ChordUtil.conv_id_to_ratio_str(n_dash.node_info.successor_info.node_id) + ","
              + hex(id))

        return all_node_dict[n_dash.node_info.successor_info.address_str]

    # id(int)　の前で一番近い位置に存在するノードを探索する
    # TODO: あとで、実システムと整合するよう、ノードに定義されたAPIを介して情報をやりとりし、
    #       ノードオブジェクトを直接得るのではなく、all_node_dictを介して得るようにする必要あり
    def find_predecessor(self, id: int):
        ChordUtil.dprint("find_predecessor_1," + str(self.node_info.born_id) + "," + hex(self.node_info.node_id))
        if self.node_info.predecessor_info == None:
            # predecessorが他ノードによる stabilize_successor によって埋まっていなければ
            # 探索は行わず Noneを返す
            ChordUtil.dprint("find_predecessor_2," + str(self.node_info.born_id) + "," + hex(self.node_info.node_id))
            return None

        n_dash = self
        #while not (n_dash.node_info.predecessor_info.node_id < id and id <= n_dash.node_info.successor_info.node_id):
        while not ChordUtil.exist_between_two_nodes_right_mawari(n_dash.node_info.predecessor_info.node_id, n_dash.node_info.successor_info.node_id, id):
            ChordUtil.dprint("find_predecessor_3," + str(self.node_info.born_id) + "," + hex(self.node_info.node_id) + "," +
                  hex(n_dash.node_info.node_id))
            n_dash_found = n_dash.closest_preceding_finger(id)
            if n_dash_found.node_info.node_id == n_dash.node_info.node_id:
                # 見つかったノードが、n_dash と同じで、変わらなかった場合
                # 同じを経路表を用いて探索することになり、結果は同じになり無限ループと
                # なってしまうため n_dash_found を探索結果として返す
                return n_dash_found
            else:
                n_dash = n_dash_found
        return n_dash

    #  自身の持つ経路情報をもとに,  id から前方向に一番近いノードの情報を返す
    def closest_preceding_finger(self, id : int):
        # 範囲の狭いエントリから探索していく
        for entry in self.node_info.finger_table:
            # ランダムに更新しているため埋まっていないエントリも存在し得る
            if entry == None:
                ChordUtil.dprint("closest_preceding_finger_0," + str(self.node_info.born_id) + ","
                      + hex(self.node_info.node_id))
                continue

            ChordUtil.dprint("closest_preceding_finger_1," + str(self.node_info.born_id) + ","
                  + hex(self.node_info.node_id) + "," + hex(entry.node_id))
            #if self.node_info.node_id < entry.node_id and entry.node_id <= id:
            if ChordUtil.exist_between_two_nodes_right_mawari(self.node_info.node_id, id, entry.node_id):
                ChordUtil.dprint("closest_preceding_finger_2," + str(self.node_info.born_id) + ","
                      + hex(self.node_info.node_id) + "," + hex(entry.node_id) + ","
                      + ChordUtil.conv_id_to_ratio_str(self.node_info.node_id) + ","
                      + ChordUtil.conv_id_to_ratio_str(entry.node_id))
                return all_node_dict[entry.address_str]

        ChordUtil.dprint("closest_preceding_finger_3")

        #自身が一番近いpredecessorである
        return self

        # # 自身のsuccessorが一番近いpredecessorである （参考スライドとは異なるがこうしてみる）
        # return all_node_dict[self.node_info.successor_info.address_str]

# ネットワークに存在するノードから1ノードをランダムに取得する
# ChordNodeオブジェクトを返す
def get_a_random_node():
    key_list = list(all_node_dict.keys())
    selected_key = ChordUtil.get_random_elem(key_list)
    return all_node_dict[selected_key]

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

# ランダムに選択したノードに stabilize のアクションをとらせる
# やりとりを行う側（つまりChordNodeクラス）にそのためのメソッドを定義する必要がありそう
def do_stabilize_on_random_node():
    global lock_of_all_data
    global done_stabilize_successor_cnt

    # ロックの取得
    lock_of_all_data.acquire()

    node = get_a_random_node()

    # TODO: 実システムではあり得ないが、stabilize_successor と stabilize_finger_table
    #       が同じChordネットワーク初期化後の同じ時間帯に動作しないようにしてみる
    if done_stabilize_successor_cnt <= 3000:
        node.stabilize_successor()
        done_stabilize_successor_cnt += 1

    ChordUtil.dprint("do_stabilize_on_random_node__successor," + str(node.node_info.born_id) + ","
                     + hex(node.node_info.node_id) + "," + ChordUtil.conv_id_to_ratio_str(node.node_info.node_id) + ","
                     + str(done_stabilize_successor_cnt))

    # ネットワーク上のノードにおいて、successorとpredeessorの情報が適切に設定された
    # 状態とならないと、stabilize_finger_talbleはほどんと意味を成さずに終了してしまう
    # ため、stabilize_successorが十分に呼び出された後で stabilize_finger_tableの
    # 実行は開始する
    if done_stabilize_successor_cnt > 3000:
        ## テーブル長が160と長いので半分の80エントリ（ランダムに行うため重複した場合は80より少なくなる）は
        ## 一気に更新してしまう
        # TODO: ランダムなため重複は生じるがほぼ全てのエントリが一気に更新されるようにしてみる
        for n in range(250):
            ChordUtil.dprint("do_stabilize_on_random_node__ftable," + str(node.node_info.born_id) + ","
                  + hex(node.node_info.node_id) + "," + ChordUtil.conv_id_to_ratio_str(node.node_info.node_id) + ","
                  + str(n))
            node.stabilize_finger_table()

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
    node.global_put(kv_data.key, kv_data.value)
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
        return

    target_data = ChordUtil.get_random_elem(all_data_list)
    target_data_id = target_data.data_id
    target_data_key_str = target_data.key

    node = get_a_random_node()
    node.global_get(target_data_id, target_data_key_str)

    # ロックの解放
    lock_of_all_data.release()

def node_join_th():
    counter = 2
    while counter < 10:
        add_new_node()
        time.sleep(0.1) # sleep 100msec
        counter += 1

def stabilize_th():
    # 実システムではあり得ないが、デバッグプリントが見にくくなることを
    # 避けるため、一度ネットワークが構築され、安定状態になったと思われる
    # タイミングに達したら stabilize 処理は行われなくする

    time.sleep(2) # sleep 2sec
    while is_stabiize_finished == False:
        do_stabilize_on_random_node()
        # 1秒に200ノードを選択し処理が
        # 行われる程度の間隔に設定
        time.sleep(0.005) # sleep 5msec

def data_put_th():
    global is_stabiize_finished

    #全ノードがネットワークに参加し十分に stabilize処理が行われた
    #状態になるまで待つ
    time.sleep(50) # sleep 50sec

    # stabilizeを行うスレッドを動作させなくする
    is_stabiize_finished = True

    while True:
        do_put_on_random_node()
        time.sleep(1) # sleep 1sec

def data_get_th():
    # 最初のputが行われるまで待つ
    time.sleep(54) # sleep 24sec
    while True:
        do_get_on_random_node()
        time.sleep(1) # sleep 1sec

def main():
    global all_node_dict

    # 再現性のため乱数シードを固定
    # ただし、複数スレッドが存在し、個々の処理の終了するタイミングや、どのタイミングで
    # スイッチするかは実行毎に異なる可能性があるため、あまり意味はないかもしれない
    random.seed(1337)

    # 最初の2ノードはここで登録する
    first_node = ChordNode("THIS_VALUE_IS_NOT_USED", first_node=True)
    all_node_dict[first_node.node_info.address_str] = first_node
    second_node = ChordNode(first_node.node_info.address_str, second_node=True)
    all_node_dict[second_node.node_info.address_str] = second_node

    node_join_th_handle = threading.Thread(target=node_join_th, daemon=True)
    node_join_th_handle.start()

    stabilize_th_handle = threading.Thread(target=stabilize_th, daemon=True)
    stabilize_th_handle.start()

    data_put_th_handle = threading.Thread(target=data_put_th, daemon=True)
    data_put_th_handle.start()

    data_get_th_handle = threading.Thread(target=data_get_th, daemon=True)
    data_get_th_handle.start()

    while True:
        time.sleep(1)

if __name__ == '__main__':
    main()
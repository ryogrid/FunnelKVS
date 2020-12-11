# coding:utf-8

import threading
import time
import random
import hashlib
import datetime
import math
import copy
from typing import Dict, List, Any, Optional, cast

ID_SPACE_BITS = 30 # 160 <- sha1での本来の値
ID_SPACE_RANGE = 2**ID_SPACE_BITS # 0を含めての数である点に注意

STABILIZE_SUCCESSOR_BATCH_TIMES = 20 #10 #20
STABILIZE_FTABLE_BATCH_TIMES = 1

# # 160bit符号なし整数の最大値
# # Chordネットワーク上のID空間の上限
# ID_MAX = 2**ID_SPACE_BITS - 1

# 30bit符号なし整数の最大値
# Chordネットワーク上のID空間の上限
# TODO: 検証時の実行時間短縮のためにハッシュ関数で求めた値の代わりに乱数
#       を用いているため bit数 を少なくしている
ID_MAX = ID_SPACE_RANGE - 1

NODE_NUM = 1000
# PUT_DATA_NUM = 100

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

    # # あるノードを経路表に入れる際に適切なエントリに入れるために
    # # 対応するインデックスを求めて返す
    # # 設定するインデックスは node_id より値が大きくてもっとも近いエントリの
    # # インデックスとなる
    # # 値が大きいエントリに入れるのは、担当ノードとしては、そのノードのsuccessorを
    # # 用いるためである
    # @classmethod
    # def calc_idx_of_ftable_from_node_id(cls, from_node_id : int, target_node_id : int) -> int:
    #     distance : int = ChordUtil.calc_distance_between_nodes_right_mawari(from_node_id, target_node_id)
    #     if distance == 0:
    #         # 同じノードを比較しており、2^0である 1 よりも距離が小さいので、finger_tableには入れない
    #         # ようにさせる
    #         return -1
    #
    #     log2_value = math.log2(distance)
    #     ceiled_value = math.floor(log2_value)
    #
    #     return ceiled_value - 1 # 0オリジンのため

    @classmethod
    def dprint(cls, print_str : str):
        print(str(datetime.datetime.now()) + "," + print_str)

    @classmethod
    def print_no_lf(cls, print_str : str):
        print(print_str, end="")

    @classmethod
    def gen_debug_str_of_node(cls, node_info : Optional['NodeInfo']) -> str:
        casted_info : 'NodeInfo' = cast('NodeInfo', node_info)
        return str(casted_info.born_id) + "," + hex(casted_info.node_id) + "," \
               + ChordUtil.conv_id_to_ratio_str(casted_info.node_id)

    @classmethod
    def gen_debug_str_of_data(cls, data_id : int) -> str:
        return hex(data_id) + "," + ChordUtil.conv_id_to_ratio_str(data_id)

    @classmethod
    def get_node_by_address(cls, address : str) -> 'ChordNode':
        return all_node_dict[address]

# all_data_listグローバル変数に格納される形式としてのみ用いる
class KeyValue:
    def __init__(self, key, value):
        self.key : str = key
        self.value : str = value
        # keyのハッシュ値
        if key == None:
            self.data_id = None
        else:
            self.data_id : int = ChordUtil.hash_str_to_int(key)

class NodeInfo:

    def __init__(self):
        self.node_id : int = None
        self.address_str : str = None

        # デバッグ用のID（実システムには存在しない）
        # 何ノード目として生成されたかの値
        self.born_id : int = None

        # NodeInfoオブジェクトを保持.
        # ある時点で取得したものが保持されており、変化する場合のあるフィールド
        # の内容は最新の内容となっているとは限らないため注意が必要.
        # そのような情報が必要な場合はChordNodeオブジェクトから参照し、
        # 必要であれば、その際に下のフィールドにdeepcopyを設定しなおさ
        # なければならない.
        self.successor_info : Optional['NodeInfo'] = None
        self.predecessor_info : Optional['NodeInfo'] = None

        # NodeInfoオブジェクトを要素として持つリスト
        # インデックスの小さい方から狭い範囲が格納される形で保持する
        # sha1で生成されるハッシュ値は160bit符号無し整数であるため要素数は160となる

        # TODO: 現在は ID_SPACE_BITS が検証時の実行時間の短縮のため30となっている
        self.finger_table : List['NodeInfo'] = [None] * ID_SPACE_BITS

    def get_partial_deepcopy_inner(self, node_info : Optional['NodeInfo']) -> Optional['NodeInfo']:
        if node_info == None:
            return None
        
        casted_node_info : 'NodeInfo' = cast('NodeInfo', node_info)
        ret_node_info : 'NodeInfo' = NodeInfo()

        ret_node_info.node_id = copy.copy(casted_node_info.node_id)
        ret_node_info.address_str = copy.copy(casted_node_info.address_str)
        ret_node_info.born_id = copy.copy(casted_node_info.born_id)
        ret_node_info.successor_info = None
        ret_node_info.predecessor_info = None

        return ret_node_info

    # 単純にdeepcopyするとチェーン構造になっているものが全てコピーされてしまう
    # ため、そこの考慮を行い、また、finger_tableはコピーしない形での deepcopy
    # を返す.
    # 上述の考慮により、コピーした NodeInfoオブジェクト の successor_infoと
    # predecessor_infoは deepcopy の対象ではあるが、それらの中の同名のフィールド
    # にはNoneが設定される. これにより、あるノードがコピーされた NodeInfo を保持
    # した場合、predecessor や successorは辿ることができるが、その先は辿ることが
    # 直接的にはできないことになる（predecessor や successorの ChordNodeオブジェクト
    # を引いてやれば可能）
    # 用途としては、あるノードの node_info を他のノードが取得し保持する際に利用される
    # ことを想定して実装されている.
    def get_partial_deepcopy(self) -> 'NodeInfo':
        ret_node_info : 'NodeInfo' = NodeInfo()

        ret_node_info.node_id = copy.copy(self.node_id)
        ret_node_info.address_str = copy.copy(self.address_str)
        ret_node_info.born_id = copy.copy(self.born_id)
        ret_node_info.successor_info = self.get_partial_deepcopy_inner(self.successor_info)
        ret_node_info.predecessor_info = self.get_partial_deepcopy_inner(self.predecessor_info)

        return ret_node_info

class ChordNode:
    QUERIED_DATA_NOT_FOUND_STR = "QUERIED_DATA_WAS_NOT_FOUND"

    # global_get内で探索した担当ノードにgetをかけて、データを持っていないと
    # レスポンスがあった際に、持っていないか辿っていくsuccessorの上限数
    GLOBAL_GET_SUCCESSOR_TRY_MAX_NODES = 5

    # 取得が NOT_FOUNDになった場合はこのクラス変数に格納して次のget処理の際にリトライさせる
    # なお、このシミュレータの実装上、このフィールドは一つのデータだけ保持できれば良い
    need_getting_retry_data_id : int = -1
    need_getting_retry_node : Optional['ChordNode'] = None

    # join処理もコンストラクタで行ってしまう
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
            # 最初の1ノードなので、joinメソッド内で行われるsuccessor からの
            # データの委譲は必要ない

            # joinの処理の中でsuccessorをfinger_tableのインデックス0に設定する
            # が、first nodeは stabilize_successorでsuccessorを張り替える
            # 際にその処理を行う
            return
        else:
            self.join(node_address)

    # node_addressに対応するノードに問い合わせを行い、教えてもらったノードをsuccessorとして設定する
    def join(self, node_address : str):
        # TODO: あとで、ちゃんとノードに定義されたAPIを介して情報をやりとりするようにする必要あり

        tyukai_node = ChordUtil.get_node_by_address(node_address)
        # 仲介ノードに自身のsuccessorになるべきノードを探してもらう
        successor = tyukai_node.search_node(self.node_info.node_id)
        self.node_info.successor_info = successor.node_info.get_partial_deepcopy()

        # successorから自身が担当することになるID範囲のデータの委譲を受け、格納する
        tantou_data_list : List['KeyValue'] = successor.get_copies_of_my_tantou_data(self.node_info.node_id, False)
        for key_value in tantou_data_list:
            self.stored_data[str(key_value.data_id)] = key_value.value

        # finger_tableのインデックス0は必ずsuccessorになるはずなので、設定しておく
        self.node_info.finger_table[0] = self.node_info.successor_info.get_partial_deepcopy()

        # successorから見たpredecessorおよび、自身から見たpredecessorの情報は
        # このタイミングで更新可能なはずなのでここで一度stabilize_successorを呼び出してしまう
        self.stabilize_successor()

        # 上記のstabilize_successorの呼び出しにより、successorが元々 predecessor の情報を保持
        # していた場合は、自身にそのノードが predecessor として設定されているはず
        # そして、その場合、自身の predecessor には自身を successorとして認識してもらわないと困る
        # のでそこの確認処理を行わせる
        if self.node_info.predecessor_info != None:
            predecessor_node = ChordUtil.get_node_by_address(cast('NodeInfo', self.node_info.predecessor_info).address_str)
            predecessor_node.stabilize_successor()

        # 自ノードの情報、仲介ノードの情報、successorとして設定したノードの情報
        ChordUtil.dprint("join," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(tyukai_node.node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(self.node_info.successor_info))

    def global_put(self, data_id : int, value_str : str):
        target_node = self.search_node(data_id)
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

        target_node = self.search_node(data_id)
        got_value_str = target_node.get(data_id)

        # # TODO: 返ってきた値が ChordNode.QUERIED_DATA_NOT_FOUND_STR だった場合、target_nodeから
        # #       一定数のsuccessorを辿ってそれぞれにも data_id に対応するデータを持っていないか問い合わせるようにする
        # if got_value_str == ChordNode.QUERIED_DATA_NOT_FOUND_STR:
        #     tried_node_num = 0
        #     # 最初は処理の都合上、最初にgetをかけたノードを設定する
        #     cur_successor = target_node
        #     while tried_node_num < ChordNode.GLOBAL_GET_SUCCESSOR_TRY_MAX_NODES:
        #         cur_successor = ChordUtil.get_node_by_address(cast('NodeInfo',cur_successor.node_info.successor_info).address_str)
        #         got_value_str = cur_successor.get(data_id)
        #         tried_node_num += 1
        #         ChordUtil.dprint("global_get_2," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
        #                          + ChordUtil.gen_debug_str_of_node(target_node.node_info) + ","
        #                          + ChordUtil.gen_debug_str_of_data(data_id) + ","
        #                          + got_value_str + "," + str(tried_node_num))
        #         if got_value_str != ChordNode.QUERIED_DATA_NOT_FOUND_STR:
        #             # データが円環上でIDが小さくなっていく方向（反時計時計回りの方向）を前方とした場合に
        #             # 後方に位置するsuccessorを辿ることでデータを取得することができた
        #             break

        # リトライを試みたであろう時の処理
        if ChordNode.need_getting_retry_data_id != -1:
            if got_value_str != ChordNode.QUERIED_DATA_NOT_FOUND_STR:
                # リトライに成功した
                ChordUtil.dprint("global_get_2_5,retry success")
                # リトライは不要なためクリア
                ChordNode.need_getting_retry_data_id = -1
                ChordNode.need_getting_retry_node = None
            else:
                # リトライに失敗した（何もしない）
                ChordUtil.dprint("global_get_2_5,retry failed")
                pass

        # 取得に失敗した場合はリトライに必要な情報をクラス変数に設定しておく
        if got_value_str == ChordNode.QUERIED_DATA_NOT_FOUND_STR:
            ChordNode.need_getting_retry_data_id = data_id
            ChordNode.need_getting_retry_node = self

        ChordUtil.dprint("global_get_3," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
              + ChordUtil.gen_debug_str_of_node(target_node.node_info) + ","
              + ChordUtil.gen_debug_str_of_data(data_id) + "," + got_value_str)
        return got_value_str

    # 得られた value の文字列を返す
    def get(self, data_id : int) -> str:
        ret_value_str = None
        try:
            ret_value_str = self.stored_data[str(data_id)]
        except:
            ret_value_str = ChordNode.QUERIED_DATA_NOT_FOUND_STR

        ChordUtil.dprint("get," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_data(data_id) + "," + ret_value_str)
        return ret_value_str

    # some_id をChordネットワーク上で担当するノードを返す
    def search_node(self, some_id : int) -> 'ChordNode':
        ChordUtil.dprint("search_node_0," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_data(some_id))

        found_node = self.find_successor(some_id)
        if found_node == None:
            # TODO: ノード探索が失敗した場合は、一定時間を空けてリトライするようにする
            ChordUtil.dprint("search_node_1," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                             + ChordUtil.gen_debug_str_of_data(some_id))
            raise Exception("appropriate node was not found.")

        ChordUtil.dprint("search_node_2," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(found_node.node_info) + ","
                         + ChordUtil.gen_debug_str_of_data(some_id))

        return found_node

    # TODO: Deleteの実装
    # def global_delete(self, key_str):
    #     print("not implemented yet")
    #
    # def delete(self, key_str):
    #     print("not implemented yet")

    # TODO: 自身が保持しているデータを一部取り除いて返す.
    #       取り除くデータは時計周りに辿った際に 引数 node_id と 自身の node_id
    #       の間に data_id が位置するデータである.
    #       　join呼び出し時、新たに参加してきた新規ノードに、successorとなる自身が、担当から外れる
    #       範囲のデータの委譲（ここではコピー）を行うために、新規ノードから呼び出される形で用いられる.
    #       　なお、Chordでは仕組み上、経路表の更新の早かったパスと遅かったパスで、同一の data_id
    #       に対する担当ノードの探索結果が異なるタイミングが発生し得るが、新規ノードの参加直後において
    #       は、本メソッド呼び出しにおける自身と呼び出し元の新規ノードの2つで異なる場合が考えられる.
    #       その場合において、本メソッドが上述のようにコピーする形で委譲処理を行っているため、一方が保持
    #       するデータが更新されていた場合に、データの一貫性が崩れる可能性がある点に注意が必要である.
    #       　正しくは、データはコピーするのではなく引き渡してしまう（保持しているデータは削除する）べき
    #       だが、ひとまず、（global_getの発行元がリトライなどを行わない前提で、）global_get で
    #       データ取得に失敗するケースを無くすため、保持しているデータの削除は行わない
    def get_copies_of_my_tantou_data(self, node_id : int, rest_copy : bool = True) -> List['KeyValue']:
        ret_datas : List['KeyValue'] = []
        for key, value in self.stored_data.items():
            data_id : int = int(key)

            # Chordネットワークを右回りにたどった時に、データの id (data_id) が呼び出し元の node_id から
            # 自身の node_id の間に位置する場合は、そのデータの担当は自身から変わらないため、渡すデータから
            # 除外する
            if ChordUtil.exist_between_two_nodes_right_mawari(node_id, self.node_info.node_id, data_id):
                continue

            # 文字列の参照をそのまま用いてしまうが、文字列はイミュータブルであるため
            # 問題ない
            item = KeyValue(None, value)
            item.data_id = data_id
            ret_datas.append(item)

        # データを委譲する際に元々持っていたノードからは削除するよう指定されていた場合
        if rest_copy == False:
            for kv in ret_datas:
                del self.stored_data[str(kv.data_id)]

        return ret_datas

    # id が自身の正しい predecessor でないかチェックし、そうであった場合、経路表の情報を更新する
    # 本メソッドはstabilize処理の中で用いられる
    def check_predecessor(self, id : int, node_info : 'NodeInfo'):
        if self.node_info.predecessor_info == None:
            # 未設定状態なので確認するまでもなく、predecessorらしいと判断し
            # 経路情報に設定し、処理を終了する
            self.node_info.predecessor_info = node_info.get_partial_deepcopy()
            ChordUtil.dprint("check_predecessor_1," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                  + ChordUtil.gen_debug_str_of_node(self.node_info.successor_info) + ","
                  + ChordUtil.gen_debug_str_of_node(self.node_info.predecessor_info))

            return

        ChordUtil.dprint("check_predecessor_2," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
              + ChordUtil.gen_debug_str_of_node(self.node_info.successor_info))

        distance_check = ChordUtil.calc_distance_between_nodes_left_mawari(self.node_info.node_id, id)
        distance_cur = ChordUtil.calc_distance_between_nodes_left_mawari(self.node_info.node_id, cast('NodeInfo',self.node_info.predecessor_info).node_id)

        # 確認を求められたノードの方が現在の predecessor より predecessorらしければ
        # 経路表の情報を更新する
        if distance_check < distance_cur:
            self.node_info.predecessor_info = node_info.get_partial_deepcopy()

            ChordUtil.dprint("check_predecessor_3," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                  + ChordUtil.gen_debug_str_of_node(self.node_info.successor_info) + ","
                  + ChordUtil.gen_debug_str_of_node(self.node_info.predecessor_info))

    # successorおよびpredicessorに関するstabilize処理を行う
    # predecessorはこの呼び出しで初めて設定される
    def stabilize_successor(self):
        ChordUtil.dprint("stabilize_successor_1," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
              + ChordUtil.gen_debug_str_of_node(self.node_info.successor_info))

        # firstノードに対する考慮（ノード作成時に自身をsuccesorに設定しているために自身だけ
        # でsuccessorチェーンのループを作ったままになってしまうことを回避する）
        if self.node_info.predecessor_info != None and (self.node_info.node_id == self.node_info.successor_info.node_id):
            ChordUtil.dprint("stabilize_successor_1_5," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                             + ChordUtil.gen_debug_str_of_node(self.node_info.successor_info))
            # secondノードがjoin済みであれば、当該ノードのstabilize_successorによって
            # secondノードがpredecessorとして設定されているはずなので、succesorをそちら
            # に張り替える
            self.node_info.successor_info = self.node_info.predecessor_info.get_partial_deepcopy()
            # finger_tableのインデックス0は必ずsuccessorになるはずなので、設定しておく
            self.node_info.finger_table[0] = self.node_info.successor_info.get_partial_deepcopy()

        # 自身のsuccessorに、当該ノードが認識しているpredecessorを訪ねる
        # 自身が保持している successor_infoのミュータブルなフィールドは最新の情報でない
        # 場合があるため、successorのChordNodeオブジェクトを引いて、そこから最新のnode_info
        # の参照を得る
        successor = ChordUtil.get_node_by_address(self.node_info.successor_info.address_str)
        successor_info = successor.node_info
        # successor_info = self.node_info.successor_info
        if successor_info.predecessor_info == None:
            # successor が predecessor を未設定であった場合は自身を predecessor として保持させて
            # 処理を終了する
            successor_info.predecessor_info = self.node_info.get_partial_deepcopy()

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
            successor_obj = ChordUtil.get_node_by_address(successor_info.address_str)
            successor_obj.check_predecessor(self.node_info.node_id, self.node_info)

            distance_unknown = ChordUtil.calc_distance_between_nodes_left_mawari(successor_obj.node_info.node_id, pred_id_of_successor)
            distance_me = ChordUtil.calc_distance_between_nodes_left_mawari(successor_obj.node_info.node_id, self.node_info.node_id)
            if distance_unknown < distance_me:
                # successorの認識しているpredecessorが自身ではなく、かつ、そのpredecessorが
                # successorから自身に対して前方向にたどった場合の経路中に存在する場合
                # 自身の認識するsuccessorの情報を更新する

                self.node_info.successor_info = successor_obj.node_info.predecessor_info.get_partial_deepcopy()

                # 新たなsuccessorに対して自身がpredecessorでないか確認を要請し必要であれ
                # ば情報を更新してもらう
                new_successor_obj = ChordUtil.get_node_by_address(self.node_info.successor_info.address_str)
                new_successor_obj.check_predecessor(self.node_info.node_id, self.node_info)

                ChordUtil.dprint("stabilize_successor_4," + ChordUtil.gen_debug_str_of_node(self.node_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(self.node_info.successor_info) + ","
                                 + ChordUtil.gen_debug_str_of_node(new_successor_obj.node_info))

    # FingerTableに関するstabilize処理を行う
    # 一回の呼び出しで1エントリを更新する
    # FingerTableのエントリはこの呼び出しによって埋まっていく
    def stabilize_finger_table(self, idx):
        ChordUtil.dprint("stabilize_finger_table_1," + ChordUtil.gen_debug_str_of_node(self.node_info))

        # FingerTableの各要素はインデックスを idx とすると 2^IDX 先のIDを担当する、もしくは
        # 担当するノードに最も近いノードが格納される
        update_id = ChordUtil.overflow_check_and_conv(self.node_info.node_id + 2**idx)
        found_node = self.find_successor(update_id)
        # found_node = self.find_predecessor(update_id)
        if found_node == None:
            ChordUtil.dprint("stabilize_finger_table_2," + ChordUtil.gen_debug_str_of_node(self.node_info))
            return

        self.node_info.finger_table[idx] = found_node.node_info.get_partial_deepcopy()

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

        return ChordUtil.get_node_by_address(n_dash.node_info.successor_info.address_str)

    # id(int)　の前で一番近い位置に存在するノードを探索する
    def find_predecessor(self, id: int):
        ChordUtil.dprint("find_predecessor_1," + ChordUtil.gen_debug_str_of_node(self.node_info))

        n_dash = self
        # n_dash と n_dashのsuccessorの 間に id が位置するような n_dash を見つけたら、ループを終了し n_dash を return する
        #while not (n_dash.node_info.predecessor_info.node_id < id and id <= n_dash.node_info.successor_info.node_id):
        while not ChordUtil.exist_between_two_nodes_right_mawari(cast('NodeInfo',n_dash.node_info).node_id, cast('NodeInfo', n_dash.node_info.successor_info).node_id, id):
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
                return ChordUtil.get_node_by_address(entry.address_str)

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
    return ChordUtil.get_node_by_address(selected_key)

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
    all_node_num = len(list(all_node_dict.values()))

    while counter < all_node_num * 2:
        ChordUtil.print_no_lf(str(cur_node_info.born_id) + "," + ChordUtil.conv_id_to_ratio_str(cur_node_info.node_id) + " -> ")

        #cur_node_info = cur_node_info.successor_info

        # 各ノードはsuccessorの情報を保持しているが、successorのsuccessorは保持しないようになって
        # いるため、単純にsuccessorのチェーンを辿ることはできないため、各ノードから最新の情報を
        # 得ることに対応する形とする
        cur_node_info = ChordUtil.get_node_by_address(cur_node_info.address_str).node_info.successor_info
        if cur_node_info == None:
            break
        counter += 1
    print("")

    # 続いてpredecessor方向に辿る
    counter = 0
    cur_node_info = get_a_random_node().node_info
    ChordUtil.print_no_lf("check_nodes_connectivity__pred")
    print(",", flush=True, end="")
    while counter < all_node_num * 2:
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
    shuffled_node_list_ftable = shuffled_node_list_ftable * (STABILIZE_FTABLE_BATCH_TIMES * ID_SPACE_BITS)

    cur_node_num = len(node_list)
    selected_operation = "" # "successor" or "ftable"
    cur_ftable_idx = 0

    while True:
        # ロックの取得
        lock_of_all_data.acquire()

        try:
            # まず行う処理を決定する
            if done_stabilize_successor_cnt >= STABILIZE_SUCCESSOR_BATCH_TIMES * cur_node_num \
                and done_stabilize_ftable_cnt >= STABILIZE_FTABLE_BATCH_TIMES * cur_node_num * ID_SPACE_BITS:
                # 関数呼び出し時点で存在した全ノードについて、2種双方が規定回数の stabilze処理を完了したため
                # 関数を終了する

                # ノードの接続状況をデバッグ出力
                check_nodes_connectivity()
                return
            elif done_stabilize_successor_cnt >= STABILIZE_SUCCESSOR_BATCH_TIMES * cur_node_num:
                # 一方は完了しているので他方を実行する
                selected_operation = "ftable"
            elif done_stabilize_ftable_cnt >= STABILIZE_FTABLE_BATCH_TIMES * cur_node_num * ID_SPACE_BITS:
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
                # 1ノードの1エントリを更新する
                # 更新するエントリのインデックスはこの関数の呼び出し時点の全ノード
                # で共通に0からインクリメントされていく
                node = shuffled_node_list_ftable.pop()
                ChordUtil.dprint(
                    "do_stabilize_on_random_node__ftable," + ChordUtil.gen_debug_str_of_node(node.node_info) + ","
                    + str(cur_ftable_idx))
                node.stabilize_finger_table(cur_ftable_idx)
                done_stabilize_ftable_cnt += 1

                if done_stabilize_ftable_cnt % cur_node_num == 0:
                    # 全ノードについて同一インデックスのエントリの更新が済んだので
                    # 次のインデックスに移る
                    cur_ftable_idx += 1

                if cur_ftable_idx >= ID_SPACE_BITS:
                    # 全インデックスのエントリの更新が終わったらまた0からスタートする
                    cur_ftable_idx = 0

                # # 対象ノードについてテーブルの下から順に全て更新する
                # for idx in range(0, ID_SPACE_BITS):
                #     ChordUtil.dprint(
                #         "do_stabilize_on_random_node__ftable," + ChordUtil.gen_debug_str_of_node(node.node_info) + ","
                #         + str(idx))
                #     node.stabilize_finger_table(idx)
                # done_stabilize_ftable_cnt += 1
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

    if ChordNode.need_getting_retry_data_id != -1:
        # doing retry
        target_data_id = ChordNode.need_getting_retry_data_id
        node = cast('ChordNode', ChordNode.need_getting_retry_node)
    else:
        target_data = ChordUtil.get_random_elem(all_data_list)
        target_data_id = target_data.data_id
        node = get_a_random_node()

    node.global_get(target_data_id)

    # ロックの解放
    lock_of_all_data.release()

def node_join_th():
    while already_born_node_num < NODE_NUM:
        add_new_node()
        time.sleep(1)  # sleep 1sec # sleep 3sec

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
    time.sleep(0.5) #次に生成するノードが同一のアドレス文字列を持つことを避けるため
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

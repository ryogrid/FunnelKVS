# coding:utf-8

import threading
import time
import random
from typing import List, cast

import modules.gval as gval
from modules.node_info import NodeInfo
from modules.chord_util import ChordUtil, KeyValue
from modules.chord_node import ChordNode, NodeIsDownedExceptiopn
from modules.stabilizer import Stabilizer

# ネットワークに存在するノードから1ノードをランダムに取得する
# is_aliveフィールドがFalseとなっているダウン状態となっているノードは返らない
def get_a_random_node() -> ChordNode:
    alive_nodes_list : List[ChordNode] = list(filter(lambda node: node.is_alive == True,list(gval.all_node_dict.values())))
    return ChordUtil.get_random_elem(alive_nodes_list)


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
    start_node_info : NodeInfo = cur_node_info
    # ノードの総数（is_aliveフィールドがFalseのものは除外して算出）
    all_node_num = len(list(filter(lambda node: node.is_alive == True ,list(gval.all_node_dict.values()))))
    ChordUtil.print_no_lf("check_nodes_connectivity__succ,all_node_num=" + str(all_node_num) + ",already_born_node_num=" + str(gval.already_born_node_num))
    print(",", flush=True, end="")

    while counter < all_node_num:
        ChordUtil.print_no_lf(str(cur_node_info.born_id) + "," + ChordUtil.conv_id_to_ratio_str(cur_node_info.node_id) + " -> ")

        # 各ノードはsuccessorの情報を保持しているが、successorのsuccessorは保持しないようになって
        # いるため、単純にsuccessorのチェーンを辿ることはできないため、各ノードから最新の情報を
        # 得ることに対応する形とする

        try:
            cur_node_info = ChordUtil.get_node_by_address(cur_node_info.address_str).node_info.successor_info_list[0]
        except NodeIsDownedExceptiopn:
            print("")
            ChordUtil.dprint("check_nodes_connectivity__succ,NODE_IS_DOWNED")
            return

        if cur_node_info == None:
            print("", flush=True, end="")
            raise Exception("no successor having node was detected!")
        counter += 1
    print("")

    # 2ノード目が参加して以降をチェック対象とする
    # successorを辿って最初のノードに戻ってきているはずだが、そうなっていない場合は successorの
    # チェーン構造が正しく構成されていないことを意味するためエラーとして終了する
    if all_node_num >=2 and cur_node_info.node_id != start_node_info.node_id:
        ChordUtil.dprint("check_nodes_connectivity_succ_err,chain does not includes all node. all_node_num = "
                         + str(all_node_num) + ","
                         + ChordUtil.gen_debug_str_of_node(start_node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(cur_node_info))
        # raise exception("SUCCESSOR_CHAIN_IS_NOT_CONSTRUCTED_COLLECTLY")
    else:
        ChordUtil.dprint("check_nodes_connectivity_succ_success,chain includes all node. all_node_num = "
                         + str(all_node_num) + ","
                         + ChordUtil.gen_debug_str_of_node(start_node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(cur_node_info))

    # 続いてpredecessor方向に辿る
    counter = 0
    cur_node_info = get_a_random_node().node_info
    start_node_info = cur_node_info
    ChordUtil.print_no_lf("check_nodes_connectivity__pred,all_node_num=" + str(all_node_num) + ",already_born_node_num=" + str(gval.already_born_node_num))
    print(",", flush=True, end="")
    while counter < all_node_num:
        ChordUtil.print_no_lf(str(cur_node_info.born_id) + "," + ChordUtil.conv_id_to_ratio_str(cur_node_info.node_id) + " -> ")
        try:
            cur_node_info = ChordUtil.get_node_by_address(cur_node_info.address_str).node_info.predecessor_info
        except NodeIsDownedExceptiopn:
            print("")
            ChordUtil.dprint("check_nodes_connectivity__pred,NODE_IS_DOWNED")
            return

        # 2ノード目から本来チェック可能であるべきだが、stabilize処理の実行タイミングの都合で
        # 2ノード目がjoinした後、いくらかpredecessorがNoneの状態が生じ、そのタイミングで本チェックが走る場合が
        # あり得るため、余裕を持たせて5ノード目以降からチェックする
        if cur_node_info == None:
            if all_node_num >= 5:
                print("", flush=True, end="")
                raise Exception("no predecessor having node was detected!")
            else:
                # 後続の処理は走らないようにする
                return

        counter += 1
    print("")

    # 2ノード目から本来チェック可能であるべきだが、stabilize処理の実行タイミングの都合で
    # 2ノード目がjoinした後、いくらかpredecessorがNoneの状態が生じ、そのタイミングで本チェックが走る場合が
    # あり得るため、余裕を持たせて5ノード目以降からチェックする
    # successorを辿って最初のノードに戻ってきているはずだが、そうなっていない場合は successorの
    # チェーン構造が正しく構成されていないことを意味するためエラーとして終了する
    if all_node_num >=5 and cur_node_info.node_id != start_node_info.node_id:
        ChordUtil.dprint("check_nodes_connectivity_pred_err,chain does not includes all node. all_node_num = "
                         + str(all_node_num) + ","
                         + ChordUtil.gen_debug_str_of_node(start_node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(cur_node_info))
        # raise Exception("PREDECESSOR_CHAIN_IS_NOT_CONSTRUCTED_COLLECTLY")
    else:
        ChordUtil.dprint("check_nodes_connectivity_pred_success,chain includes all node. all_node_num = "
                         + str(all_node_num) + ","
                         + ChordUtil.gen_debug_str_of_node(start_node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(cur_node_info))

# ランダムに仲介ノードを選択し、そのノードに仲介してもらう形でネットワークに参加させる
def add_new_node():
    # # ロックの取得
    # gval.lock_of_all_data.acquire()

    if Stabilizer.need_join_retry_node != None:
        # 前回の呼び出しが失敗していた場合はリトライを行う
        tyukai_node = ChordNode.need_join_retry_tyukai_node
        new_node = ChordNode.need_join_retry_node
        new_node.stabilizer.join(tyukai_node.node_info.address_str)
        if Stabilizer.need_join_retry_node == None:
            # リトライ情報が再設定されていないためリトライに成功したと判断
            ChordUtil.dprint(
                "add_new_node_1,retry of join is succeeded," + ChordUtil.gen_debug_str_of_node(new_node.node_info))
        else:
            ChordUtil.dprint(
                "add_new_node_2,retry of join is failed," + ChordUtil.gen_debug_str_of_node(new_node.node_info))
    else:
        tyukai_node = get_a_random_node()
        new_node = ChordNode(tyukai_node.node_info.address_str)

    if Stabilizer.need_join_retry_node == None:
        # join処理(リトライ時以外はChordNodeクラスのコンストラクタ内で行われる)が成功していれば
        gval.all_node_dict[new_node.node_info.address_str] = new_node

    # # ロックの解放
    # gval.lock_of_all_data.release()

# all_node_id辞書のvaluesリスト内から重複なく選択したノードに stabilize のアクションをとらせていく
def do_stabilize_once_at_all_node():
    done_stabilize_successor_cnt = 0
    done_stabilize_ftable_cnt = 0

    node_list = list(gval.all_node_dict.values())

    # 各リストはpopメソッドで要素を取り出して利用されていく
    # 同じノードは複数回利用されるため、その分コピーしておく（参照がコピーされるだけ）
    shuffled_node_list_successor : List[ChordNode] = random.sample(node_list, len(node_list))
    shuffled_node_list_successor = shuffled_node_list_successor * gval.STABILIZE_SUCCESSOR_BATCH_TIMES
    shuffled_node_list_ftable : List[ChordNode] = random.sample(node_list, len(node_list))
    shuffled_node_list_ftable = shuffled_node_list_ftable * (gval.STABILIZE_FTABLE_BATCH_TIMES * gval.ID_SPACE_BITS)

    cur_node_num = len(node_list)
    selected_operation = "" # "successor" or "ftable"
    cur_ftable_idx = 0

    while True:
        # # ロックの取得
        # gval.lock_of_all_data.acquire()

        try:
            # まず行う処理を決定する
            if done_stabilize_successor_cnt >= gval.STABILIZE_SUCCESSOR_BATCH_TIMES * cur_node_num \
                and done_stabilize_ftable_cnt >= gval.STABILIZE_FTABLE_BATCH_TIMES * cur_node_num * gval.ID_SPACE_BITS:
                # 関数呼び出し時点で存在した全ノードについて、2種双方が規定回数の stabilze処理を完了したため
                # 関数を終了する

                # ノードの接続状況をデバッグ出力
                check_nodes_connectivity()
                return
            elif done_stabilize_successor_cnt >= gval.STABILIZE_SUCCESSOR_BATCH_TIMES * cur_node_num:
                # 一方は完了しているので他方を実行する
                selected_operation = "ftable"
            elif done_stabilize_ftable_cnt >= gval.STABILIZE_FTABLE_BATCH_TIMES * cur_node_num * gval.ID_SPACE_BITS:
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
                if node.is_alive == True:
                    node.stabilizer.stabilize_successor()
                    ChordUtil.dprint("do_stabilize_on_random_node__successor," + ChordUtil.gen_debug_str_of_node(node.node_info) + ","
                                       + str(done_stabilize_successor_cnt))
                done_stabilize_successor_cnt += 1
            else: # "ftable"
                # 1ノードの1エントリを更新する
                # 更新するエントリのインデックスはこの関数の呼び出し時点の全ノード
                # で共通に0からインクリメントされていく
                node = shuffled_node_list_ftable.pop()
                if node.is_alive == True:
                    ChordUtil.dprint(
                        "do_stabilize_on_random_node__ftable," + ChordUtil.gen_debug_str_of_node(node.node_info) + ","
                        + str(cur_ftable_idx))
                    node.stabilizer.stabilize_finger_table(cur_ftable_idx)
                done_stabilize_ftable_cnt += 1

                if done_stabilize_ftable_cnt % cur_node_num == 0:
                    # 全ノードについて同一インデックスのエントリの更新が済んだので
                    # 次のインデックスに移る
                    cur_ftable_idx += 1

                if cur_ftable_idx >= gval.ID_SPACE_BITS:
                    # 全インデックスのエントリの更新が終わったらまた0からスタートする
                    cur_ftable_idx = 0

        finally:
            pass
            # # ロックの解放
            # gval.lock_of_all_data.release()


# 適当なデータを生成し、IDを求めて、そのIDなデータを担当するChordネットワーク上のノードの
# アドレスをよろしく解決し、見つかったノードにputの操作を依頼する
def do_put_on_random_node():

    # # ロックの取得
    # gval.lock_of_all_data.acquire()

    is_retry = False

    if ChordNode.need_put_retry_data_id != -1:
        # 前回の呼び出し時に global_putが失敗しており、リトライが必要
        # TODO: listの形で保持されるようになったリトライ情報に対応する.
        #       リトライ処理する際は、設定されていた情報をローカルに移動させる
        #       on do_put_on_random_node

        # TODO: リスト化されたリトライ情報へのアクセスも排他制御をしないといけない
        #       気がするが・・・・きっと大丈夫だろう

        is_retry = True

        # key と value の値は共通としているため、記録してあった value の値を key としても用いる
        kv_data = KeyValue(ChordNode.need_put_retry_data_value, ChordNode.need_put_retry_data_value)
        # data_id は乱数で求めるというインチキをしているため、記録してあったもので上書きする
        kv_data.data_id = ChordNode.need_put_retry_data_id
        node = ChordNode.need_put_retry_node
    else:
        unixtime_str = str(time.time())
        # ミリ秒精度で取得したUNIXTIMEを文字列化してkeyとvalueに用いる
        kv_data = KeyValue(unixtime_str, unixtime_str)
        node = get_a_random_node()

    # 成功した場合はTrueが返るのでその場合だけ all_node_dictに追加する
    if node.global_put(kv_data.data_id, kv_data.value_data):
        gval.all_data_list.append(kv_data)

    if is_retry:
        # TODO: リトライが成功したかはローカルに保持している情報がlist内に再設定されていないかで判定する
        #       on do_put_on_random_node
        if ChordNode.need_put_retry_data_id == -1:
            # リトライ情報が再設定されていないためリトライに成功したと判断
            ChordUtil.dprint(
                "do_put_on_random_node_1,retry of global_put is succeeded," + ChordUtil.gen_debug_str_of_node(node.node_info) + ","
                + ChordUtil.gen_debug_str_of_data(kv_data.data_id))
        else:
            ChordUtil.dprint(
                "do_put_on_random_node_2,retry of global_put is failed," + ChordUtil.gen_debug_str_of_node(node.node_info) + ","
                + ChordUtil.gen_debug_str_of_data(kv_data.data_id))

    # # ロックの解放
    # gval.lock_of_all_data.release()

# グローバル変数であるall_data_listからランダムにデータを選択し、そのデータのIDから
# Chordネットワーク上の担当ノードのアドレスをよろしく解決し、見つかったノードにgetの操作を依頼する
def do_get_on_random_node():
    # # ロックの取得
    # gval.lock_of_all_data.acquire()

    # まだ put が行われていなかったら何もせずに終了する
    if len(gval.all_data_list) == 0:
        # gval.lock_of_all_data.release()
        return

    is_retry = False

    if ChordNode.need_getting_retry_data_id != -1:
        # doing retry
        # TODO: listの形で保持されるようになったリトライ情報に対応する.
        #       リトライ処理する際は、設定されていた情報をローカルに移動させる
        #       on do_get_on_random_node
        is_retry = True
        target_data_id = ChordNode.need_getting_retry_data_id
        node = cast('ChordNode', ChordNode.need_getting_retry_node)
    else:
        target_data = ChordUtil.get_random_elem(gval.all_data_list)
        target_data_id = target_data.data_id
        node = get_a_random_node()

    node.global_get(target_data_id)

    if is_retry:
        # TODO: リトライが成功したかはローカルに保持している情報がlist内に再設定されていないかで判定する
        #       on do_get_on_random_node
        if ChordNode.need_getting_retry_data_id == -1:
            # リトライ情報が再設定されていないためリトライに成功したと判断
            ChordUtil.dprint(
                "do_get_on_random_node_1,retry of global_get is succeeded," + ChordUtil.gen_debug_str_of_node(
                    node.node_info) + ","
                + ChordUtil.gen_debug_str_of_data(target_data_id))
        else:
            ChordUtil.dprint(
                "do_get_on_random_node_2,retry of global_get is failed," + ChordUtil.gen_debug_str_of_node(
                    node.node_info) + ","
                + ChordUtil.gen_debug_str_of_data(target_data_id))

    # # ロックの解放
    # gval.lock_of_all_data.release()

# グローバル変数であるall_node_dictからランダムにノードを選択し
# ダウンさせる（is_aliveフィールドをFalseに設定する）
def do_kill_a_random_node():
    # # ロックの取得
    # gval.lock_of_all_data.acquire()

    node = get_a_random_node()
    node.is_alive = False
    ChordUtil.dprint(
        "do_kill_a_random_node,"
        + ChordUtil.gen_debug_str_of_node(node.node_info))

    # # ロックの解放
    # gval.lock_of_all_data.release()

def node_join_th():
    while gval.already_born_node_num < gval.NODE_NUM_MAX:
        add_new_node()
        time.sleep(gval.JOIN_INTERVAL_SEC)

def stabilize_th():
    # TODO: stabilize_th も複数スレッド化しないとダメだろうか？（一番しないといけないものな気もする）
    # TODO: スレッド番号を採番して、whileループの先頭でデバッグプリントする
    #       on stabilize_th
    while True:
        # 内部で適宜ロックを解放することで他のスレッドの処理も行えるようにしつつ
        # 呼び出し時点でのノードリストを対象に stabilize 処理を行う
        do_stabilize_once_at_all_node()

def data_put_th():
    # TODO: スレッド番号を採番して、whileループの先頭でデバッグプリントする
    #       on data_puth_th
    while True:
        do_put_on_random_node()
        time.sleep(gval.PUT_INTERVAL_SEC)

def data_get_th():
    # TODO: スレッド番号を採番して、whileループの先頭でデバッグプリントする
    #       on data_get_th
    while True:
        # 内部でデータのputが一度も行われていなければreturnしてくるので
        # putを行うスレッドと同時に動作を初めても問題ないようにはなっている
        do_get_on_random_node()
        # エンドレスで行うのでデバッグプリントのサイズが大きくなり過ぎないよう
        # sleepを挟む
        time.sleep(gval.GET_INTERVAL_SEC)

def node_kill_th():
    # TODO: スレッド番号を採番して、whileループの先頭でデバッグプリントする
    #       on data_get_th
    while True:
        # ネットワークに存在するノードが5ノードを越えたらノードをダウンさせる処理を有効にする
        # しかし、リトライされなければならない処理が存在した場合は抑制する
        if len(gval.all_node_dict) > 5 \
                and (ChordNode.need_getting_retry_data_id == -1
                     and ChordNode.need_put_retry_data_id == -1
                     and Stabilizer.need_join_retry_node == None) :
            do_kill_a_random_node()

        time.sleep(gval.NODE_KILL_INTERVAL_SEC)

def main():
    # 再現性のため乱数シードを固定
    # ただし、複数スレッドが存在し、個々の処理の終了するタイミングや、どのタイミングで
    # スイッチするかは実行毎に異なる可能性があるため、あまり意味はないかもしれない
    random.seed(1337)

    # 最初の1ノードはここで登録する
    first_node = ChordNode("THIS_VALUE_IS_NOT_USED", first_node=True)
    gval.all_node_dict[first_node.node_info.address_str] = first_node
    time.sleep(0.5) #次に生成するノードが同一のアドレス文字列を持つことを避けるため

    node_join_th_handle = threading.Thread(target=node_join_th, daemon=True)
    node_join_th_handle.start()

    # TODO: 同一処理を行う複数スレッドを立てる?
    #       (立てる際はタイミングをズラすようループに一定ms程度のsleepを挟むこと)
    #       stabilize
    stabilize_th_handle = threading.Thread(target=stabilize_th, daemon=True)
    stabilize_th_handle.start()

    # TODO: 同一処理を行う複数スレッドを立てる
    #       (立てる際はタイミングをズラすようループに一定ms程度のsleepを挟むこと)
    #       put
    data_put_th_handle = threading.Thread(target=data_put_th, daemon=True)
    data_put_th_handle.start()

    # TODO: 同一処理を行う複数スレッドを立てる
    #       (立てる際はタイミングをズラすようループに一定ms程度のsleepを挟むこと)
    #       get
    data_get_th_handle = threading.Thread(target=data_get_th, daemon=True)
    data_get_th_handle.start()

    # TODO: 同一処理を行う複数スレッドを立てる
    #       (立てる際はタイミングをズラすようループに一定ms程度のsleepを挟むこと)
    #       kill
    node_kill_th_handle = threading.Thread(target=node_kill_th, daemon=True)
    node_kill_th_handle.start()

    while True:
        time.sleep(1)

if __name__ == '__main__':
    main()

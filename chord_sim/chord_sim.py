# coding:utf-8

import threading
import time
import random
from typing import Dict, List, Any, Optional, cast

import modules.gval as gval
from modules.node_info import NodeInfo
from modules.chord_util import ChordUtil, KeyValue
from modules.chord_node import ChordNode

# ネットワークに存在するノードから1ノードをランダムに取得する
# ChordNodeオブジェクトを返す
def get_a_random_node() -> ChordNode:
    key_list : List[str] = list(gval.all_node_dict.keys())
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
    start_node_info : NodeInfo = cur_node_info
    all_node_num = len(list(gval.all_node_dict.values()))
    ChordUtil.print_no_lf("check_nodes_connectivity__succ,all_node_num=" + str(all_node_num) + ",already_born_node_num=" + str(gval.already_born_node_num))
    print(",", flush=True, end="")

    while counter < all_node_num:
        ChordUtil.print_no_lf(str(cur_node_info.born_id) + "," + ChordUtil.conv_id_to_ratio_str(cur_node_info.node_id) + " -> ")

        # 各ノードはsuccessorの情報を保持しているが、successorのsuccessorは保持しないようになって
        # いるため、単純にsuccessorのチェーンを辿ることはできないため、各ノードから最新の情報を
        # 得ることに対応する形とする
        cur_node_info = ChordUtil.get_node_by_address(cur_node_info.address_str).node_info.successor_info_list[0]
        if cur_node_info == None:
            print("", flush=True, end="")
            raise Exception("no successor having node was detected!")
        counter += 1
    print("")

    # 2ノード目が参加して以降をチェック対象とする
    # successorを辿って最初のノードに戻ってきているはずだが、そうなっていない場合は successorの
    # チェーン構造が正しく構成されていないことを意味するためエラーとして終了する
    if all_node_num >=2 and cur_node_info.node_id != start_node_info.node_id:
        ChordUtil.dprint("check_nodes_connectivity_succ_err,chain does not include all node. all_node_num = "
                         + str(all_node_num) + ","
                         + ChordUtil.gen_debug_str_of_node(start_node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(cur_node_info))
        print("", flush=True, end="")
        raise Exception("SUCCESSOR_CHAIN_IS_NOT_CONSTRUCTED_COLLECTLY")

    # 続いてpredecessor方向に辿る
    counter = 0
    cur_node_info = get_a_random_node().node_info
    start_node_info = cur_node_info
    ChordUtil.print_no_lf("check_nodes_connectivity__pred,all_node_num=" + str(all_node_num) + ",already_born_node_num=" + str(gval.already_born_node_num))
    print(",", flush=True, end="")
    while counter < all_node_num:
        ChordUtil.print_no_lf(str(cur_node_info.born_id) + "," + ChordUtil.conv_id_to_ratio_str(cur_node_info.node_id) + " -> ")
        cur_node_info = ChordUtil.get_node_by_address(cur_node_info.address_str).node_info.predecessor_info

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
        ChordUtil.dprint("check_nodes_connectivity_succ_err,chain does not include all node. all_node_num = "
                         + str(all_node_num) + ","
                         + ChordUtil.gen_debug_str_of_node(start_node_info) + ","
                         + ChordUtil.gen_debug_str_of_node(cur_node_info)
                         , flush=True)
        raise Exception("PREDECESSOR_CHAIN_IS_NOT_CONSTRUCTED_COLLECTLY")

# ランダムに仲介ノードを選択し、そのノードに仲介してもらう形でネットワークに参加させる
def add_new_node():
    # ロックの取得
    gval.lock_of_all_data.acquire()

    tyukai_node = get_a_random_node()
    new_node = ChordNode(tyukai_node.node_info.address_str)
    gval.all_node_dict[new_node.node_info.address_str] = new_node

    # ロックの解放
    gval.lock_of_all_data.release()

# all_node_id辞書のvaluesリスト内から重複なく選択したノードに stabilize のアクションをとらせていく
def do_stabilize_once_at_all_node():
    done_stabilize_successor_cnt = 0
    done_stabilize_ftable_cnt = 0

    node_list = list(gval.all_node_dict.values())

    # 各リストはpopメソッドで要素を取り出して利用されていく
    # 同じノードは複数回利用されるため、その分コピーしておく（参照がコピーされるだけ）
    shuffled_node_list_successor = random.sample(node_list, len(node_list))
    shuffled_node_list_successor = shuffled_node_list_successor * gval.STABILIZE_SUCCESSOR_BATCH_TIMES
    shuffled_node_list_ftable = random.sample(node_list, len(node_list))
    shuffled_node_list_ftable = shuffled_node_list_ftable * (gval.STABILIZE_FTABLE_BATCH_TIMES * gval.ID_SPACE_BITS)

    cur_node_num = len(node_list)
    selected_operation = "" # "successor" or "ftable"
    cur_ftable_idx = 0

    while True:
        # ロックの取得
        gval.lock_of_all_data.acquire()

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

                if cur_ftable_idx >= gval.ID_SPACE_BITS:
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
            gval.lock_of_all_data.release()



# 適当なデータを生成し、IDを求めて、そのIDなデータを担当するChordネットワーク上のノードの
# アドレスをよろしく解決し、見つかったノードにputの操作を依頼する
def do_put_on_random_node():
    # ロックの取得
    gval.lock_of_all_data.acquire()

    unixtime_str = str(time.time())
    # ミリ秒精度で取得したUNIXTIMEを文字列化してkeyとvalueに用いる
    kv_data = KeyValue(unixtime_str, unixtime_str)
    node = get_a_random_node()
    node.global_put(kv_data.data_id, kv_data.value)
    gval.all_data_list.append(kv_data)

    # ロックの解放
    gval.lock_of_all_data.release()

# グローバル変数であるall_data_listからランダムにデータを選択し、そのデータのIDから
# Chordネットワーク上の担当ノードのアドレスをよろしく解決し、見つかったノードにgetの操作を依頼する
def do_get_on_random_node():
    # ロックの取得
    gval.lock_of_all_data.acquire()

    # まだ put が行われていなかったら何もせずに終了する
    if len(gval.all_data_list) == 0:
        gval.lock_of_all_data.release()
        return

    if ChordNode.need_getting_retry_data_id != -1:
        # doing retry
        target_data_id = ChordNode.need_getting_retry_data_id
        node = cast('ChordNode', ChordNode.need_getting_retry_node)
    else:
        target_data = ChordUtil.get_random_elem(gval.all_data_list)
        target_data_id = target_data.data_id
        node = get_a_random_node()

    node.global_get(target_data_id)

    # ロックの解放
    gval.lock_of_all_data.release()

def node_join_th():
    while gval.already_born_node_num < gval.NODE_NUM_MAX:
        add_new_node()
        time.sleep(gval.JOIN_INTERVAL_SEC)

def stabilize_th():
    while True:
        # 内部で適宜ロックを解放することで他のスレッドの処理も行えるようにしつつ
        # 呼び出し時点でのノードリストを対象に stabilize 処理を行う
        do_stabilize_once_at_all_node()

def data_put_th():
    while True:
        do_put_on_random_node()
        time.sleep(gval.PUT_INTERVAL_SEC)

def data_get_th():
    while True:
        # 内部でデータのputが一度も行われていなければreturnしてくるので
        # putを行うスレッドと同時に動作を初めても問題ないようにはなっている
        do_get_on_random_node()
        # エンドレスで行うのでデバッグプリントのサイズが大きくなり過ぎないよう
        # sleepを挟む
        time.sleep(gval.GET_INTERVAL_SEC)

def main():
    # 再現性のため乱数シードを固定
    # ただし、複数スレッドが存在し、個々の処理の終了するタイミングや、どのタイミングで
    # スイッチするかは実行毎に異なる可能性があるため、あまり意味はないかもしれない
    random.seed(1337)

    # 最初の1ノードはここで登録する
    first_node = ChordNode("THIS_VALUE_IS_NOT_USED", first_node=True)
    gval.all_node_dict[first_node.node_info.address_str] = first_node
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

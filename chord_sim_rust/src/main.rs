/*
# coding:utf-8

import threading
from threading import Thread
import time
import random

import modules.gval as gval
from modules.node_info import NodeInfo
from modules.chord_util import ChordUtil, KeyValue, DataIdAndValue, ErrorCode, PResult, NodeIsDownedExceptiopn, InternalControlFlowException
from modules.chord_node import ChordNode
from modules.stabilizer import Stabilizer

# ネットワークに存在するノードから1ノードをランダムに取得する
# is_aliveフィールドがFalseとなっているダウン状態となっているノードは返らない
def get_a_random_node() -> ChordNode:
    with gval.lock_of_all_node_dict:
        alive_nodes_list : List[ChordNode] = list(
            filter(lambda node: node.is_alive == True and node.is_join_op_finished == True, list(gval.all_node_dict.values()))
        )
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
    with gval.lock_of_all_node_dict:
        all_node_num = len(list(filter(lambda node: node.is_alive == True ,list(gval.all_node_dict.values()))))
    ChordUtil.print_no_lf("check_nodes_connectivity__succ,all_node_num=" + str(all_node_num) + ",already_born_node_num=" + str(gval.already_born_node_num))
    print(",", flush=True, end="")

    while counter < all_node_num:
        ChordUtil.print_no_lf(str(cur_node_info.born_id) + "," + ChordUtil.conv_id_to_ratio_str(cur_node_info.node_id) + " -> ")

        # 各ノードはsuccessorの情報を保持しているが、successorのsuccessorは保持しないようになって
        # いるため、単純にsuccessorのチェーンを辿ることはできないため、各ノードから最新の情報を
        # 得ることに対応する形とする

        ret = ChordUtil.get_node_by_address(cur_node_info.address_str)
        if (ret.is_ok):
            cur_node_info : 'NodeInfo' = cast('ChordNode', ret.result).node_info.successor_info_list[0]
        else:  # ret.err_code == ErrorCode.InternalControlFlowException_CODE || ret.err_code == ErrorCode.NodeIsDownedException_CODE
            if cast(int, ret.err_code) == ErrorCode.NodeIsDownedException_CODE:
                print("")
                ChordUtil.dprint("check_nodes_connectivity__succ,NODE_IS_DOWNED")
                return
            else: #cast(int, ret.err_code) == ErrorCode.InternalControlFlowException_CODE
                # join中のノードのノードオブジェクトを get_node_by_address しようとした場合に
                # TargetNodeDoesNotExistExceptionがraiseされてくるのでその場合は、対象ノードのstabilize_successorはあきらめる
                print("")
                ChordUtil.dprint("check_nodes_connectivity__succ,TARGET_NODE_DOES_NOT_EXIST_EXCEPTION_IS_RAISED")
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
        ret = ChordUtil.get_node_by_address(cur_node_info.address_str)
        if (ret.is_ok):
            cur_node_info: 'ChordNode' = cast('ChordNode', ret.result).node_info.predecessor_info
        else:  # ret.err_code == ErrorCode.InternalControlFlowException_CODE || ret.err_code == ErrorCode.NodeIsDownedException_CODE
            if cast(int, ret.err_code) == ErrorCode.NodeIsDownedException_CODE:
                print("")
                ChordUtil.dprint("check_nodes_connectivity__pred,NODE_IS_DOWNED")
                return
            else: #cast(int, ret.err_code) == ErrorCode.InternalControlFlowException_CODE
                # join中のノードのノードオブジェクトを get_node_by_address しようとした場合に
                # TargetNodeDoesNotExistExceptionがraiseされてくるのでその場合は、対象ノードのstabilize_successorはあきらめる
                print("")
                ChordUtil.dprint("check_nodes_connectivity__pred,TARGET_NODE_DOES_NOT_EXIST_EXCEPTION_IS_RAISED")
                return

        if cur_node_info == None:
            # 先を追っていけないのでチェックを終了する
            ChordUtil.dprint("check_nodes_connectivity__pred,PREDECESSOR_INFO_IS_NONE")
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

# TODO: 実システム化する際は、リトライ処理は各オペレーションに対応するRESTインタフェースの呼び出し
#       の中で行う形に書き直す必要あり

# ランダムに仲介ノードを選択し、そのノードに仲介してもらう形でネットワークに参加させる
def add_new_node():
    # # ロックの取得
    # gval.lock_of_all_data.acquire()

    if Stabilizer.need_join_retry_node != None:
        # 前回の呼び出しが失敗していた場合はリトライを行う
        tyukai_node = cast('ChordNode', Stabilizer.need_join_retry_tyukai_node)
        new_node = cast('ChordNode', Stabilizer.need_join_retry_node)
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
        # join処理のうち、ネットワーク参加時に必ずしも完了していなくてもデータの整合性やネットワークの安定性に
        # に問題を生じさせないような処理をここで行う（当該処理がノード内のタスクキューに入っているのでそれを実行する形にする）
        new_node.tqueue.exec_first()

    # # ロックの解放
    # gval.lock_of_all_data.release()

def do_stabilize_successor_th(node_list : List[ChordNode]):
    for times in range(0, gval.STABILIZE_SUCCESSOR_BATCH_TIMES):
        for node in node_list:
            # try:
                #node.stabilizer.stabilize_successor()
            ret = node.stabilizer.stabilize_successor()
            if (ret.is_ok):
                pass
            else:  # ret.err_code == ErrorCode.InternalControlFlowException_CODE
                # join中のノードのノードオブジェクトを get_node_by_address しようとした場合に
                # InternalCtronlFlowExceptionがraiseされてくるのでその場合は、対象ノードのstabilize_finger_tableはあきらめる
                ChordUtil.dprint(
                    "do_stabilize_successor_th," + ChordUtil.gen_debug_str_of_node(node.node_info)
                    + ",STABILIZE_FAILED_DUE_TO_INTERNAL_CONTROL_FLOW_EXCEPTION_RAISED")

def do_stabilize_ftable_th(node_list : List[ChordNode]):
    for times in range(0, gval.STABILIZE_FTABLE_BATCH_TIMES):
        for table_idx in range(0, gval.ID_SPACE_BITS):
            for node in node_list:
                ret = node.stabilizer.stabilize_finger_table(table_idx)
                if (ret.is_ok):
                    pass
                else:  # ret.err_code == ErrorCode.InternalControlFlowException_CODE
                    # join中のノードのノードオブジェクトを get_node_by_address しようとした場合に
                    # InternalCtronlFlowExceptionがraiseされてくるのでその場合は、対象ノードのstabilize_finger_tableはあきらめる
                    ChordUtil.dprint(
                        "do_stabilize_ftable_th," + ChordUtil.gen_debug_str_of_node(node.node_info)
                        + ",STABILIZE_FAILED_DUE_TO_INTERNAL_CONTROL_FLOW_EXCEPTION_RAISED")

def do_stabilize_onace_at_all_node_successor(node_list : List[ChordNode]) -> List[Thread]:
    list_len = len(node_list)
    range_start = 0
    # 小数点以下切り捨て
    basic_pass_node_cnt = int(list_len / gval.STABILIZE_THREAD_NUM)
    thread_list : List[Thread] = []
    for thread_idx in range(0, gval.STABILIZE_THREAD_NUM):
        if thread_idx == gval.STABILIZE_THREAD_NUM - 1:
            thread = threading.Thread(target=do_stabilize_successor_th, name="successor-" + str(thread_idx),
                                      args=([node_list[range_start:-1]]))
        else:
            thread = threading.Thread(target=do_stabilize_successor_th, name="successor-" + str(thread_idx),
                                      args=([node_list[range_start:range_start + basic_pass_node_cnt]]))
            range_start += basic_pass_node_cnt
        thread.start()
        thread_list.append(thread)

    return thread_list


def do_stabilize_onace_at_all_node_ftable(node_list : List[ChordNode]) -> List[Thread]:
    list_len = len(node_list)
    range_start = 0
    # 小数点以下切り捨て
    basic_pass_node_cnt = int(list_len / gval.STABILIZE_THREAD_NUM)
    thread_list : List[Thread] = []
    for thread_idx in range(0, gval.STABILIZE_THREAD_NUM):
        if thread_idx == gval.STABILIZE_THREAD_NUM - 1:
            thread = threading.Thread(target=do_stabilize_ftable_th, name="ftable-" + str(thread_idx),
                                      args=([node_list[range_start:-1]]))
        else:
            thread = threading.Thread(target=do_stabilize_successor_th, name="ftable-" + str(thread_idx),
                                      args=([node_list[range_start:range_start + basic_pass_node_cnt]]))
            range_start += basic_pass_node_cnt
        thread.start()
        thread_list.append(thread)

    return thread_list

# all_node_id辞書のvaluesリスト内から重複なく選択したノードに stabilize のアクションをとらせていく
def do_stabilize_once_at_all_node():
    ChordUtil.dprint("do_stabilize_once_at_all_node_0,START")
    with gval.lock_of_all_node_dict:
        node_list = list(gval.all_node_dict.values())
        shuffled_node_list : List[ChordNode] = random.sample(node_list, len(node_list))
    thread_list_succ : List[Thread] = do_stabilize_onace_at_all_node_successor(shuffled_node_list)
    thread_list_ftable : List[Thread] = do_stabilize_onace_at_all_node_ftable(shuffled_node_list)

    # 全てのスレッドが終了するまで待つ
    # 一つの呼び出しごとにブロックするが、その間に別にスレッドが終了しても
    # スレッドの処理が終了していることは担保できるため問題ない
    for thread in thread_list_succ:
        thread.join()
    for thread in thread_list_ftable:
        thread.join()

    check_nodes_connectivity()

# 適当なデータを生成し、IDを求めて、そのIDなデータを担当するChordネットワーク上のノードの
# アドレスをよろしく解決し、見つかったノードにputの操作を依頼する
def do_put_on_random_node():

    # # ロックの取得
    # gval.lock_of_all_data.acquire()

    is_retry = False

    if ChordNode.need_put_retry_data_id != -1:
        # 前回の呼び出し時に global_putが失敗しており、リトライが必要

        is_retry = True

        # key と value の値は共通としているため、記録してあった value の値を key としても用いる
        kv_data = KeyValue(ChordNode.need_put_retry_data_value, ChordNode.need_put_retry_data_value)
        # data_id は乱数で求めるというインチキをしているため、記録してあったもので上書きする
        kv_data.data_id = ChordNode.need_put_retry_data_id
        node = cast('ChordNode', ChordNode.need_put_retry_node)
    else:
        # ミリ秒精度で取得したUNIXTIMEを文字列化してkeyに用いる
        unixtime_str = str(time.time())

        # valueは乱数を生成して、それを16進表示したもの
        random_num = random.randint(0, gval.ID_SPACE_RANGE - 1)
        kv_data = KeyValue(unixtime_str, hex(random_num))

        # データの更新を行った場合のget時の整合性のチェックのため2回に一回はput済みの
        # データのIDを keyとして用いる
        if gval.already_issued_put_cnt % 2 != 0:
            random_kv_elem : 'KeyValue' = ChordUtil.get_random_data()
            data_id = random_kv_elem.data_id
            kv_data.data_id = data_id

        node = get_a_random_node()

    # 成功した場合はTrueが返るのでその場合だけ all_data_listに追加する
    if node.endpoints.rrpc__global_put(cast(int, kv_data.data_id), kv_data.value_data):
        with gval.lock_of_all_data_list:
            gval.all_data_list.append(kv_data)

    if is_retry:
        if ChordNode.need_put_retry_data_id == -1:
            # リトライ情報が再設定されていないためリトライに成功したと判断
            ChordUtil.dprint(
                "do_put_on_random_node_1,retry of global_put is succeeded," + ChordUtil.gen_debug_str_of_node(node.node_info) + ","
                + ChordUtil.gen_debug_str_of_data(cast(int, kv_data.data_id)))
        else:
            ChordUtil.dprint(
                "do_put_on_random_node_2,retry of global_put is failed," + ChordUtil.gen_debug_str_of_node(node.node_info) + ","
                + ChordUtil.gen_debug_str_of_data(cast(int, kv_data.data_id)))

    # # ロックの解放
    # gval.lock_of_all_data.release()

# グローバル変数であるall_data_listからランダムにデータを選択し、そのデータのIDから
# Chordネットワーク上の担当ノードのアドレスをよろしく解決し、見つかったノードにgetの操作を依頼する
def do_get_on_random_node():
    # # ロックの取得
    # gval.lock_of_all_data.acquire()

    # まだ put が行われていなかったら何もせずに終了する
    with gval.lock_of_all_data_list:
        if len(gval.all_data_list) == 0:
            # gval.lock_of_all_data.release()
            return

    is_retry = False

    if ChordNode.need_getting_retry_data_id != -1:
        # doing retry

        #リトライを行うためカウンタをインクリメントする
        gval.global_get_retry_cnt += 1

        # リトライ回数が規定回数に達したらデータの所在を出力する
        if gval.global_get_retry_cnt == gval.GLOBAL_GET_RETRY_CNT_LIMIT_TO_DEBEUG_PRINT:
            ChordUtil.print_data_placement_info(ChordNode.need_getting_retry_data_id, after_notfound_limit=True)
        else:
            ChordUtil.print_data_placement_info(ChordNode.need_getting_retry_data_id)

        is_retry = True
        target_data_id = ChordNode.need_getting_retry_data_id
        node = cast('ChordNode', ChordNode.need_getting_retry_node)
    else:
        #リトライではない (リトライが無事終了した場合を含む) ためカウンタをリセットする
        gval.global_get_retry_cnt = 0

        with gval.lock_of_all_data_list:
            target_data = ChordUtil.get_random_elem(gval.all_data_list)
        target_data_id = target_data.data_id

        # ログの量の増加が懸念されるが global_getを行うたびに、取得対象データの所在を出力する
        ChordUtil.print_data_placement_info(target_data_id)

        node = get_a_random_node()

    got_result : str = node.endpoints.rrpc__global_get(target_data_id)

    # 関数内関数
    def print_data_consistency():
        # TODO: gval.all_data_list は 検索のコストを考えると dict にした方がいいかも
        #       at do_get_on_random_node
        with gval.lock_of_all_data_list:
            for idx in reversed(range(0, len(gval.all_data_list))):
                if gval.all_data_list[idx].data_id == target_data_id:
                    latest_elem = gval.all_data_list[idx]

        if got_result == latest_elem.value_data:
            ChordUtil.dprint(
                "do_get_on_random_node_1," + ChordUtil.gen_debug_str_of_node(node.node_info) + ","
                + ChordUtil.gen_debug_str_of_data(target_data_id) + ","
                + got_result
                + ",OK_GOT_VALUE_WAS_LATEST")
        else:
            ChordUtil.dprint(
                "do_get_on_random_node_1," + ChordUtil.gen_debug_str_of_node(node.node_info) + ","
                + ChordUtil.gen_debug_str_of_data(target_data_id) + ","
                + got_result
                + ",WARN__GOT_VALUE_WAS_INCONSISTENT")

    if is_retry:
        if ChordNode.need_getting_retry_data_id == -1:
            # リトライ情報が再設定されていないためリトライに成功したと判断

            print_data_consistency()

            ChordUtil.dprint(
                "do_get_on_random_node_2,retry of global_get is succeeded," + ChordUtil.gen_debug_str_of_node(
                    node.node_info) + ","
                + ChordUtil.gen_debug_str_of_data(target_data_id))
        else:
            ChordUtil.dprint(
                "do_get_on_random_node_2,retry of global_get is failed," + ChordUtil.gen_debug_str_of_node(
                    node.node_info) + ","
                + ChordUtil.gen_debug_str_of_data(target_data_id))
    else:
        if ChordNode.need_getting_retry_data_id == -1:
            # global_getが成功していた場合のみチェックを行う
            print_data_consistency()

    # # ロックの解放
    # gval.lock_of_all_data.release()

# グローバル変数であるall_node_dictからランダムにノードを選択し
# ダウンさせる（is_aliveフィールドをFalseに設定する）
def do_kill_a_random_node():
    # # ロックの取得
    # gval.lock_of_all_data.acquire()

    node = get_a_random_node()
    try:
        with gval.lock_of_all_node_dict:
            if len(gval.all_node_dict) > 10 \
                    and (ChordNode.need_getting_retry_data_id == -1
                         and ChordNode.need_put_retry_data_id == -1
                         and Stabilizer.need_join_retry_node == None):
                node.is_alive = False
                ChordUtil.dprint(
                    "do_kill_a_random_node_1,"
                    + ChordUtil.gen_debug_str_of_node(node.node_info))
                with node.node_info.lock_of_datastore:
                    for key, value in node.data_store.stored_data.items():
                        data_id: str = key
                        sv_entry : DataIdAndValue = value
                        ChordUtil.dprint("do_kill_a_random_node_2,"
                                         + ChordUtil.gen_debug_str_of_node(node.node_info) + ","
                                         + hex(int(data_id)) + "," + hex(sv_entry.data_id))
    finally:
        # node.node_info.lock_of_datastore.release()
        # node.node_info.lock_of_succ_infos.release()
        # node.node_info.lock_of_pred_info.release()
        pass

    # # ロックの解放
    # gval.lock_of_all_data.release()

# TODO: 対応する処理を行うスクリプトの類が必要 node_join_th
def node_join_th():
    while gval.already_born_node_num < gval.NODE_NUM_MAX:
        if gval.already_born_node_num == gval.KEEP_NODE_NUM:
            time.sleep(60.0)
            gval.is_network_constructed = True
            gval.JOIN_INTERVAL_SEC = 120.0 #20.0
            # # TODO: デバッグのために1000ノードに達したらjoinを止める。後で元に戻すこと!
            # #       at node_join_th
            # break

        add_new_node()
        time.sleep(gval.JOIN_INTERVAL_SEC)

def stabilize_th():
    while True:
        # 内部で適宜ロックを解放することで他のスレッドの処理も行えるようにしつつ
        # 呼び出し時点でのノードリストを対象に stabilize 処理を行う
        do_stabilize_once_at_all_node()

# TODO: RESTでエンドポイントを叩くテストプログラムが必要 data_put_th
def data_put_th():
    while gval.is_network_constructed == False:
        time.sleep(1)

    while True:
        do_put_on_random_node()
        time.sleep(gval.PUT_INTERVAL_SEC)

# TODO: RESTでエンドポイントを叩くテストプログラムが必要 data_get_th
def data_get_th():
    while gval.is_network_constructed == False:
        time.sleep(1)

    while True:
        # 内部でデータのputが一度も行われていなければreturnしてくるので
        # putを行うスレッドと同時に動作を初めても問題ないようにはなっている
        do_get_on_random_node()
        # エンドレスで行うのでデバッグプリントのサイズが大きくなり過ぎないよう
        # sleepを挟む
        time.sleep(gval.GET_INTERVAL_SEC)

# TODO: 適当に選んだプロセスをkillするスクリプトなりが必要 node_kill_th
def node_kill_th():
    while gval.is_network_constructed == False:
        time.sleep(1)

    while True:
        # # ネットワークに存在するノードが10ノードを越えたらノードをダウンさせる処理を有効にする
        # # しかし、リトライされなければならない処理が存在した場合および partial_join_opの実行が必要なノードが
        # # 存差異する場合は抑制する
        # if len(gval.all_node_dict) > 10 \
        #         and (ChordNode.need_getting_retry_data_id == -1
        #              and ChordNode.need_put_retry_data_id == -1
        #              and Stabilizer.need_join_retry_node == None
        #              and gval.is_waiting_partial_join_op_exists == False) :
        #     do_kill_a_random_node()
        do_kill_a_random_node()

        time.sleep(gval.NODE_KILL_INTERVAL_SEC)

def main():
    # result1 : PResult[Optional[NodeInfo]] = ChordUtil.generic_test_ok(NodeInfo())
    # print(result1)
    # result2 : PResult[Optional[NodeInfo]] = ChordUtil.generic_test_err(ErrorCode.NodeIsDownedException_CODE)
    # print(result2)
    #
    # ret = ChordUtil.generic_test_ok(NodeInfo())
    # if ret.is_ok:
    #     casted_ret : 'NodeInfo' = cast('NodeInfo', ret.result)
    #     print("Ok")
    # else:
    #     casted_ret: int = cast(int, ret.err_code)
    #     print(casted_ret)
    #
    # ret = ChordUtil.generic_test_err(ErrorCode.NodeIsDownedException_CODE)
    # if ret.is_ok:
    #     casted_ret : 'NodeInfo' = print(cast('NodeInfo', ret.result))
    #     print("Ok")
    # else:
    #     casted_ret : int = cast(int, ret.err_code)
    #     print(casted_ret)

    # 再現性のため乱数シードを固定
    # ただし、複数スレッドが存在し、個々の処理の終了するタイミングや、どのタイミングで
    # スイッチするかは実行毎に異なる可能性があるため、あまり意味はないかもしれない
    random.seed(1337)

    # 最初の1ノードはここで登録する
    first_node = ChordNode("THIS_VALUE_IS_NOT_USED", first_node=True)
    first_node.is_join_op_finished = True
    gval.all_node_dict[first_node.node_info.address_str] = first_node
    time.sleep(0.5) #次に生成するノードが同一のアドレス文字列を持つことを避けるため

    node_join_th_handle = threading.Thread(target=node_join_th, daemon=True)
    node_join_th_handle.start()

    stabilize_th_handle = threading.Thread(target=stabilize_th, daemon=True)
    stabilize_th_handle.start()

    data_put_th_handle = threading.Thread(target=data_put_th, daemon=True)
    data_put_th_handle.start()

    data_get_th_handle = threading.Thread(target=data_get_th, daemon=True)
    data_get_th_handle.start()

    node_kill_th_handle = threading.Thread(target=node_kill_th, daemon=True)
    node_kill_th_handle.start()

    while True:
        time.sleep(1)

if __name__ == '__main__':
    main()
*/

//#![allow(dead_code)] 
// disables several lint warnings
#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(non_upper_case_globals)]
#![allow(unused_assignments)]
#![allow(dead_code)]
#![allow(non_snake_case)]

#![feature(proc_macro_hygiene)]
#![feature(decl_macro)]
/*
#[macro_use]
extern crate rocket;
*/


#[macro_use] extern crate lazy_static;

// utility macros

// IN:  Arc<ReentrandMutex<RefCell<T>>>
// OUT: &RefCell<T>
macro_rules! get_refcell_from_arc_with_locking {
    ($arc:expr) => (
        &*(($arc.as_ref()).borrow().lock());
    );
}

// IN:  &RefCell<T>
// OUT: &mut RefMut<T>
macro_rules! get_refmut_from_refcell {
    ($refcell:expr) => (
        &mut ($refcell).borrow_mut();
    );
}

// IN:  &RefCell<T>
// OUT: &Ref<T>
macro_rules! get_ref_from_refcell {
    ($refcell:expr) => (
        &($refcell).borrow();
    );
}

macro_rules! ArRmRs_new {
    ($wrapped:expr) => (
        Arc::new(const_reentrant_mutex(RefCell::new($wrapped)))
    );    
}

pub mod gval;
pub mod chord_node;
pub mod node_info;
pub mod chord_util;
pub mod stabilizer;
pub mod router;
pub mod data_store;
pub mod taskqueue;
pub mod endpoints;

type ArRmRs<T> = Arc<ReentrantMutex<RefCell<T>>>;

use std::{borrow::{Borrow, BorrowMut}, io::Write, sync::Arc, thread};
use std::cell::{RefMut, RefCell, Ref};
use std::io::{stdout, stdin};
use std::sync::{Mutex, mpsc};
use std::sync::atomic::Ordering;

use parking_lot::{ReentrantMutex, ReentrantMutexGuard, const_reentrant_mutex};

// ネットワークに存在するノードから1ノードをランダムに取得する
// is_aliveフィールドがFalseとなっているダウン状態となっているノード
// および、is_join_op_finishedフィールドがFalseでjoin処理が完全に完了していない
// ノードは返らない
// TODO: 注 -> (Rust) 何のロックもとっていない状態で終了するので、取得したオブジェクト単位でロックを
//       とるのであれば、呼び出し元で取得した値のロックをとって利用する形にする必要がある
//       スレッドが並列動作しないようにさせるのであれば、各スレッドの1回の処理に対応する処理ブロック
//       がgval::global_datasのロックをとったまま行われるよう記述する必要がある
fn get_a_random_node() -> ArRmRs<chord_node::ChordNode>{
    let gd_refcell = get_refcell_from_arc_with_locking!(gval::global_datas);
    let gd_ref = get_ref_from_refcell!(gd_refcell);
    let mut tmp_vec = vec![];
    for (k, v) in &gd_ref.borrow().all_node_dict{
        let node_refcell = get_refcell_from_arc_with_locking!(*v);
        let node_refmut = get_ref_from_refcell!(node_refcell);
        if node_refmut.is_join_op_finished.load(Ordering::Relaxed) == true && node_refmut.is_join_op_finished.load(Ordering::Relaxed) == true {
            tmp_vec.push(v);
        }
    }
    let rand_val = chord_util::get_rnd_int_with_limit(tmp_vec.len() as i32);
    let node_arc = tmp_vec.get(rand_val as usize);
    let ret = Arc::clone(*(node_arc.unwrap()));

    return ret;
}

fn get_first_data_no_arg() -> ArRmRs<chord_util::KeyValue> {
    let gd_refcell = get_refcell_from_arc_with_locking!(gval::global_datas);
    let gd_ref = get_ref_from_refcell!(gd_refcell);
    let kv_arc = gd_ref.all_data_list.get(0).unwrap().clone();
    return Arc::clone( &kv_arc);
}

fn get_node_from_map(key: &String) -> ArRmRs<chord_node::ChordNode>{
    let gd_refcell = get_refcell_from_arc_with_locking!(gval::global_datas);
    let gd_ref = get_ref_from_refcell!(gd_refcell);
    let node_arc = gd_ref.all_node_dict.get(key).unwrap().clone();
    return Arc::clone(&node_arc);
}

// マルチスレッド動作のテスト用コード（1）
fn ftable_mod_and_search_th(){
    loop{
        println!("thread-{:?}", thread::current().id());

        {
            // 新たなChordNodeを生成し all_node_dictに追加し、target_node のfinger_table
            // の適当なインデックスに対応する NodeInfo を設定する

            let new_node_id = chord_util::get_rnd_int_with_limit(gval::ID_MAX);
            let new_node_address = new_node_id.to_string();

            let new_node = chord_node::ChordNode::new();
            new_node.is_join_op_finished.store(true, Ordering::Relaxed);

            let cloned_new_node_info: node_info::NodeInfo;
            {
                let new_node_info_refcell = get_refcell_from_arc_with_locking!(new_node.node_info);
                let new_node_info_mutref = get_refmut_from_refcell!(new_node_info_refcell);
                new_node_info_mutref.node_id = new_node_id;
                // new_nodeのnode_infoをディープコピーしたオブジェクトを生成しておく
                cloned_new_node_info = new_node_info_mutref.clone();
            }

            let gd_refcell = get_refcell_from_arc_with_locking!(gval::global_datas);
            // 新たに生成したChordNodeを node_id を string としたものをキーに
            // all_node_dictに追加する
            {
                let gd_refmut = get_refmut_from_refcell!(gd_refcell);
                gd_refmut.all_node_dict.insert(
                    new_node_address,
                    ArRmRs_new!(new_node)
                );
            }

            println!("before get_a_random_node");

            // finger_tableの更新と、finger_tableの探索を行うノード
            let target_node_arrmrs = get_a_random_node();

            println!("after get_a_random_node");

            let target_node_refcell = get_refcell_from_arc_with_locking!(target_node_arrmrs);
            let target_node_ref = get_ref_from_refcell!(target_node_refcell);

            {
                let ninfo_refcell = get_refcell_from_arc_with_locking!(target_node_ref.node_info);
                let ninfo_mutref = get_refmut_from_refcell!(ninfo_refcell);

                {
                    // target_node の fingerテーブルの適当な要素を更新
                    let ftable_len = ninfo_mutref.finger_table.len() as i32;// ftable_refmut.len() as i32;

                    ninfo_mutref.finger_table[chord_util::get_rnd_int_with_limit(ftable_len) as usize] = Some(cloned_new_node_info);

                    // ftable_refmutを有効なままにすると、後続の処理の内容によってはborrowの際にpanicを生じさせてしまうため
                    // ここで無効にする
                }

                // ninfo_mutrefをここで無効にしておく
            }

            let target_node_ninfo_cloned = Arc::clone(&target_node_ref.node_info);
            let target_node_ninfo_refcell = get_refcell_from_arc_with_locking!(target_node_ninfo_cloned);
            let target_node_ninfo_ref = get_ref_from_refcell!(target_node_ninfo_refcell);

            let found_node_arc = router::closest_preceding_finger(Arc::clone(&target_node_arrmrs), target_node_ninfo_ref, chord_util::get_rnd_int_with_limit(gval::ID_MAX));
            let found_node_refcell = get_refcell_from_arc_with_locking!(found_node_arc);
            let found_node_ref = get_ref_from_refcell!(found_node_refcell);

            // TODO: 以下のprintlnを実行するとクラッシュして落ちる
            //       おそらくだが、NodeInfoの再帰的な構造に対する対処をせずにcloneを用いたか、そもそもの構造によって
            //       よって無限に参照先を追いかけるというようなことが起きているものと思われる
            //       error: process didn't exit successfully: `target\debug\chord_sim_rust.exe`
            //       (exit code: 0xc00000fd, STATUS_STACK_OVERFLOW)
            // println!("{:?}", found_node_ref);

            let found_node_info_refcell = get_refcell_from_arc_with_locking!(found_node_ref.node_info);
            let found_node_info_ref = get_ref_from_refcell!(found_node_info_refcell);
            println!{"node id: {:?}", found_node_info_ref.node_id};
        
        // 無効化および解放されていない参照やリソースを全てここで始末する
        }

        // ロックや可変参照を持たない状態になったので1秒間 sleep して他のスレッドが動作できるようにする
        std::thread::sleep(std::time::Duration::from_millis(1000));        
    }
}

fn example_th() {
    loop{
        let kv_arc_at_heap : Box<ArRmRs<chord_util::KeyValue>>;
        {
            let gd_refcell = get_refcell_from_arc_with_locking!(gval::global_datas);
            let gd_ref = get_ref_from_refcell!(gd_refcell);
            let kv_arc = gd_ref.all_data_list.get(0).unwrap().clone();
            kv_arc_at_heap = Box::new(Arc::clone( &kv_arc));
            let kv_refcell = get_refcell_from_arc_with_locking!(kv_arc);
            let kv_refmut = get_refmut_from_refcell!(kv_refcell);    
            kv_refmut.data_id = Some((kv_refmut.data_id).unwrap() + 1);
            println!{"{:?}", kv_refmut.data_id};

            for (k, v) in &gd_ref.all_node_dict {
                println!("{:?} {:?}", k, &(&*v.lock()).borrow());
            }
        }
        stdout().flush().unwrap();

        std::thread::sleep(std::time::Duration::from_millis(1000));
    }
}

fn main_old() {
/////// 要素もそれ自身もmutableなグローバルアクセス可能な形で定義されたコレクションの操作の例 //////
    // Vecを操作している処理のブロック
    {
        //ReentrantMutexとRefCellを用いた場合
        let refcell_gd = get_refcell_from_arc_with_locking!(gval::global_datas);        
        {
            let refmut_gd = get_refmut_from_refcell!(refcell_gd);
            refmut_gd.all_data_list.push(
                Arc::new(
                const_reentrant_mutex(
                    RefCell::new(
                            chord_util::KeyValue::new(Some("kanbayashi".to_string()),"sugoi".to_string())
                        )
                    )
                )
            );
        }

        let first_elem : ArRmRs<chord_util::KeyValue>;
        {
            first_elem = get_first_data_no_arg();
            let refcell = get_refcell_from_arc_with_locking!(first_elem);
            let ref_kv = get_ref_from_refcell!(refcell);

            println!("{:?}", ref_kv);
        }

        let refcell = get_refcell_from_arc_with_locking!(first_elem);
        let refmut_kv = get_refmut_from_refcell!(refcell);
        
        let num = 2u32.pow(10u32);
        refmut_kv.value_data = num.to_string();
        refmut_kv.value_data ="yabai".to_string();
        println!("{:?}", refmut_kv);
    }

    //let gd_ref = get_ref_from_arc!(gval::global_datas, gd_refcell, let gd_refcell:&RefCell<gval::GlobalDatas>, gd_tmp, let gd_tmp:&ReentrantMutexGuard<RefCell<gval::GlobalDatas>>);

/*
    // HashMapを操作している処理のブロック
    {
// all_node_dictのHashMapが格納する要素のvalueの型をKeyValue型からChordNode型に変更した結果
// 修正しないと動作しなくなったのでひとまずコメントアウト

        let refcell_gd = get_refcell_from_arc_with_locking!(gval::global_datas);
        {
            let refmut_gd = get_refmut_from_refcell!(refcell_gd);
            refmut_gd.all_node_dict.insert(
                "ryo_grid".to_string(),
                chord_node::ChordNode::powerful_new() 
            );
        }

        let one_elem : ArRmRs<chord_node::ChordNode>;
        {
            one_elem = get_node_from_map(&"ryo_grid".to_string());
            let one_elem_tmp = get_refcell_from_arc_with_locking!(one_elem);
            let one_elem_to_print = get_ref_from_refcell!(one_elem_tmp);

            println!("{:?}", one_elem_to_print);
        }

        let refcell_node = get_refcell_from_arc_with_locking!(one_elem);

        let refcell_router = get_refcell_from_arc_with_locking!(refcell_node.borrow().router);
        let ref_router = get_ref_from_refcell!(refcell_router);
        let found_node = ref_router.closest_preceding_finger(Arc::clone(&one_elem), 1024);
        println!("{:?}", found_node);

        // stringはcloneでディープコピーできるようだ
        let _cloned_string = "clone_base".to_string().clone();
    }
*/
////////////////////////////////////////////////////////////////////////////////////////////////


/*
    // 複数のスレッドで GLOBAL_DATAS に触ってみる
    let mut thread_handles = vec![];
    // thead-1
    thread_handles.push(std::thread::spawn(example_th));
    // thead-2
    thread_handles.push(std::thread::spawn(example_th));

    // スレッドの処理終了の待ち合わせ
    for handle in thread_handles {
        handle.join().unwrap();
    }
*/

/*
    // finger_table を触るコードを実際のコードを模してマルチスレッドで動かしてみる
    let mut thread_handles = vec![];
    // thead-1
    thread_handles.push(std::thread::spawn(ftable_mod_and_search_th));
    // thead-2
    thread_handles.push(std::thread::spawn(ftable_mod_and_search_th));

    // スレッドの処理終了の待ち合わせ
    for handle in thread_handles {
        handle.join().unwrap();
    }
*/

/*
    for dummy in 1..21{
        println!("{:?}", hash_str_to_int(&"".to_string()));
    }
*/

    println!("Hello, world!");
}

// stabilize_successorの呼び出しが一通り終わったら確認するのに利用する
// ランダムに選択したノードからsuccessor方向にsuccessorの繋がりでノードを辿って
// 行って各ノードの情報を出力する
// また、predecessorの方向にpredecesorの繋がりでもたどって出力する
pub fn check_nodes_connectivity() {
    chord_util::dprint("check_nodes_connectivity_1");
    let mut counter : int = 0;
    // まずはsuccessor方向に辿る
    let cur_node_info : NodeInfo = get_a_random_node().node_info;
    let start_node_info : NodeInfo = cur_node_info;
    // ノードの総数（is_aliveフィールドがFalseのものは除外して算出）

    //with gval.lock_of_all_node_dict:
    let all_node_num = len(list(filter(lambda node: node.is_alive == True ,list(gval.all_node_dict.values()))));
    chord_util::print_no_lf("check_nodes_connectivity__succ,all_node_num=" + str(all_node_num) + ",already_born_node_num=" + str(gval.already_born_node_num));

    while counter < all_node_num {
        chord_util::print_no_lf(str(cur_node_info.born_id) + "," + chord_util::conv_id_to_ratio_str(cur_node_info.node_id) + " -> ");

        // 各ノードはsuccessorの情報を保持しているが、successorのsuccessorは保持しないようになって
        // いるため、単純にsuccessorのチェーンを辿ることはできないため、各ノードから最新の情報を
        // 得ることに対応する形とする

        let ret = chord_util::get_node_by_address(cur_node_info.address_str);
        if ret.is_ok {
            cur_node_info : 'NodeInfo' = cast('ChordNode', ret.result).node_info.successor_info_list[0];
        } else {  // ret.err_code == ErrorCode.InternalControlFlowException_CODE || ret.err_code == ErrorCode.NodeIsDownedException_CODE
            if cast(int, ret.err_code) == ErrorCode.NodeIsDownedException_CODE {
                print("");
                chord_util::dprint("check_nodes_connectivity__succ,NODE_IS_DOWNED");
                return;
            } else { //cast(int, ret.err_code) == ErrorCode.InternalControlFlowException_CODE
                // join中のノードのノードオブジェクトを get_node_by_address しようとした場合に
                // TargetNodeDoesNotExistExceptionがraiseされてくるのでその場合は、対象ノードのstabilize_successorはあきらめる
                println("");
                chord_util::dprint("check_nodes_connectivity__succ,TARGET_NODE_DOES_NOT_EXIST_EXCEPTION_IS_RAISED");
                return;
            }
        }
        if cur_node_info == None{
            print("", flush=True, end="");
            print("no successor having node was detected!");
            //raise Exception("no successor having node was detected!")
        }
        counter += 1;
    }
    println("");

    // 2ノード目が参加して以降をチェック対象とする
    // successorを辿って最初のノードに戻ってきているはずだが、そうなっていない場合は successorの
    // チェーン構造が正しく構成されていないことを意味するためエラーとして終了する
    if all_node_num >=2 && cur_node_info.node_id != start_node_info.node_id {
        chord_util::dprint("check_nodes_connectivity_succ_err,chain does not includes all node. all_node_num = "
                         + str(all_node_num) + ","
                         + chord_util::gen_debug_str_of_node(start_node_info) + ","
                         + chord_util::gen_debug_str_of_node(cur_node_info));
        // raise exception("SUCCESSOR_CHAIN_IS_NOT_CONSTRUCTED_COLLECTLY")
    } else {
        chord_util::dprint("check_nodes_connectivity_succ_success,chain includes all node. all_node_num = "
                         + str(all_node_num) + ","
                         + chord_util::gen_debug_str_of_node(start_node_info) + ","
                         + chord_util::gen_debug_str_of_node(cur_node_info));
    }

// TODO: predecessor方向のチェックは後回し
/*
    // 続いてpredecessor方向に辿る
    counter = 0
    cur_node_info = get_a_random_node().node_info
    start_node_info = cur_node_info
    chord_util::print_no_lf("check_nodes_connectivity__pred,all_node_num=" + str(all_node_num) + ",already_born_node_num=" + str(gval.already_born_node_num))
    print(",", flush=True, end="")
    while counter < all_node_num:
        chord_util::print_no_lf(str(cur_node_info.born_id) + "," + chord_util::conv_id_to_ratio_str(cur_node_info.node_id) + " -> ")
        ret = chord_util::get_node_by_address(cur_node_info.address_str)
        if (ret.is_ok):
            cur_node_info: 'ChordNode' = cast('ChordNode', ret.result).node_info.predecessor_info
        else:  # ret.err_code == ErrorCode.InternalControlFlowException_CODE || ret.err_code == ErrorCode.NodeIsDownedException_CODE
            if cast(int, ret.err_code) == ErrorCode.NodeIsDownedException_CODE:
                print("")
                chord_util::dprint("check_nodes_connectivity__pred,NODE_IS_DOWNED")
                return
            else: #cast(int, ret.err_code) == ErrorCode.InternalControlFlowException_CODE
                # join中のノードのノードオブジェクトを get_node_by_address しようとした場合に
                # TargetNodeDoesNotExistExceptionがraiseされてくるのでその場合は、対象ノードのstabilize_successorはあきらめる
                print("")
                chord_util::dprint("check_nodes_connectivity__pred,TARGET_NODE_DOES_NOT_EXIST_EXCEPTION_IS_RAISED")
                return

        if cur_node_info == None:
            # 先を追っていけないのでチェックを終了する
            chord_util::dprint("check_nodes_connectivity__pred,PREDECESSOR_INFO_IS_NONE")
            return

        counter += 1

    print("")

    // 2ノード目から本来チェック可能であるべきだが、stabilize処理の実行タイミングの都合で
    // 2ノード目がjoinした後、いくらかpredecessorがNoneの状態が生じ、そのタイミングで本チェックが走る場合が
    // あり得るため、余裕を持たせて5ノード目以降からチェックする
    // successorを辿って最初のノードに戻ってきているはずだが、そうなっていない場合は successorの
    // チェーン構造が正しく構成されていないことを意味するためエラーとして終了する
    if all_node_num >=5 and cur_node_info.node_id != start_node_info.node_id:
        chord_util::dprint("check_nodes_connectivity_pred_err,chain does not includes all node. all_node_num = "
                         + str(all_node_num) + ","
                         + chord_util::gen_debug_str_of_node(start_node_info) + ","
                         + chord_util::gen_debug_str_of_node(cur_node_info))
        # raise Exception("PREDECESSOR_CHAIN_IS_NOT_CONSTRUCTED_COLLECTLY")
    else:
        chord_util::dprint("check_nodes_connectivity_pred_success,chain includes all node. all_node_num = "
                         + str(all_node_num) + ","
                         + chord_util::gen_debug_str_of_node(start_node_info) + ","
                         + chord_util::gen_debug_str_of_node(cur_node_info))
*/
}

/*
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
    with gval.lock_of_all_node_dict:
        all_node_num = len(list(filter(lambda node: node.is_alive == True ,list(gval.all_node_dict.values()))))
    ChordUtil.print_no_lf("check_nodes_connectivity__succ,all_node_num=" + str(all_node_num) + ",already_born_node_num=" + str(gval.already_born_node_num))
    print(",", flush=True, end="")

    while counter < all_node_num:
        ChordUtil.print_no_lf(str(cur_node_info.born_id) + "," + ChordUtil.conv_id_to_ratio_str(cur_node_info.node_id) + " -> ")

        # 各ノードはsuccessorの情報を保持しているが、successorのsuccessorは保持しないようになって
        # いるため、単純にsuccessorのチェーンを辿ることはできないため、各ノードから最新の情報を
        # 得ることに対応する形とする

        ret = ChordUtil.get_node_by_address(cur_node_info.address_str)
        if (ret.is_ok):
            cur_node_info : 'NodeInfo' = cast('ChordNode', ret.result).node_info.successor_info_list[0]
        else:  # ret.err_code == ErrorCode.InternalControlFlowException_CODE || ret.err_code == ErrorCode.NodeIsDownedException_CODE
            if cast(int, ret.err_code) == ErrorCode.NodeIsDownedException_CODE:
                print("")
                ChordUtil.dprint("check_nodes_connectivity__succ,NODE_IS_DOWNED")
                return
            else: #cast(int, ret.err_code) == ErrorCode.InternalControlFlowException_CODE
                # join中のノードのノードオブジェクトを get_node_by_address しようとした場合に
                # TargetNodeDoesNotExistExceptionがraiseされてくるのでその場合は、対象ノードのstabilize_successorはあきらめる
                print("")
                ChordUtil.dprint("check_nodes_connectivity__succ,TARGET_NODE_DOES_NOT_EXIST_EXCEPTION_IS_RAISED")
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
        ret = ChordUtil.get_node_by_address(cur_node_info.address_str)
        if (ret.is_ok):
            cur_node_info: 'ChordNode' = cast('ChordNode', ret.result).node_info.predecessor_info
        else:  # ret.err_code == ErrorCode.InternalControlFlowException_CODE || ret.err_code == ErrorCode.NodeIsDownedException_CODE
            if cast(int, ret.err_code) == ErrorCode.NodeIsDownedException_CODE:
                print("")
                ChordUtil.dprint("check_nodes_connectivity__pred,NODE_IS_DOWNED")
                return
            else: #cast(int, ret.err_code) == ErrorCode.InternalControlFlowException_CODE
                # join中のノードのノードオブジェクトを get_node_by_address しようとした場合に
                # TargetNodeDoesNotExistExceptionがraiseされてくるのでその場合は、対象ノードのstabilize_successorはあきらめる
                print("")
                ChordUtil.dprint("check_nodes_connectivity__pred,TARGET_NODE_DOES_NOT_EXIST_EXCEPTION_IS_RAISED")
                return

        if cur_node_info == None:
            # 先を追っていけないのでチェックを終了する
            ChordUtil.dprint("check_nodes_connectivity__pred,PREDECESSOR_INFO_IS_NONE")
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
*/

pub fn add_new_node(){
    // ロックの取得
    // ここで取得した値が無効とならない限り gval::global_datasへの別スレッドでのアクセスはブロックされる
    let gd_refcell = get_refcell_from_arc_with_locking!(gval::global_datas);

    // gval.lock_of_all_data.acquire()

// TODO: (rust) リトライ関連の処理は後回し
/*
    if stabilizer::need_join_retry_node != None {
        // 前回の呼び出しが失敗していた場合はリトライを行う
        let tyukai_node = stabilizer::need_join_retry_tyukai_node;
        let new_node = stabilizer::need_join_retry_node;
        stabilizer::join(new_node, tyukai_node.node_info.address_str);
        if stabilizer::need_join_retry_node == None {
            // リトライ情報が再設定されていないためリトライに成功したと判断
            chord_util::dprint(
                "add_new_node_1,retry of join is succeeded," + chord_util::gen_debug_str_of_node(new_node.node_info));
        } else {
            chord_util::dprint(
                "add_new_node_2,retry of join is failed," + chord_util::gen_debug_str_of_node(new_node.node_info));
        }
    } else {
        let tyukai_node = get_a_random_node();
        let new_node = chord_node::ChordNode(tyukai_node.node_info.address_str);
    }
*/

    let tyukai_node = get_a_random_node();
    let new_node = chord_node::ChordNode(tyukai_node.node_info.address_str);

    // TODO: (rust) ひとまずjoin処理が成功していようがいまいが all_node_dictに追加してしまう
    //              後で要修正
    gval::all_node_dict[new_node.node_info.address_str] = new_node;

// TODO: join処理が成功したか否かで処理を変えるルートは後回し。また、レプリカ関連の処理も後回し
/*
    if stabilizer::need_join_retry_node == None {
        // join処理(リトライ時以外はChordNodeクラスのコンストラクタ内で行われる)が成功していれば
        gval::all_node_dict[new_node.node_info.address_str] = new_node;
        // join処理のうち、ネットワーク参加時に必ずしも完了していなくてもデータの整合性やネットワークの安定性に
        // に問題を生じさせないような処理をここで行う（当該処理がノード内のタスクキューに入っているのでそれを実行する形にする）
        new_node.tqueue.exec_first();
    }
*/
}

/*
# ランダムに仲介ノードを選択し、そのノードに仲介してもらう形でネットワークに参加させる
def add_new_node():
    # # ロックの取得
    # gval.lock_of_all_data.acquire()

    if Stabilizer.need_join_retry_node != None:
        # 前回の呼び出しが失敗していた場合はリトライを行う
        tyukai_node = cast('ChordNode', Stabilizer.need_join_retry_tyukai_node)
        new_node = cast('ChordNode', Stabilizer.need_join_retry_node)
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
        # join処理のうち、ネットワーク参加時に必ずしも完了していなくてもデータの整合性やネットワークの安定性に
        # に問題を生じさせないような処理をここで行う（当該処理がノード内のタスクキューに入っているのでそれを実行する形にする）
        new_node.tqueue.exec_first()

    # # ロックの解放
    # gval.lock_of_all_data.release()
*/

/*
def do_stabilize_successor_th(node_list : List[ChordNode]):
    for times in range(0, gval.STABILIZE_SUCCESSOR_BATCH_TIMES):
        for node in node_list:
            # try:
                #node.stabilizer.stabilize_successor()
            ret = node.stabilizer.stabilize_successor()
            if (ret.is_ok):
                pass
            else:  # ret.err_code == ErrorCode.InternalControlFlowException_CODE
                # join中のノードのノードオブジェクトを get_node_by_address しようとした場合に
                # InternalCtronlFlowExceptionがraiseされてくるのでその場合は、対象ノードのstabilize_finger_tableはあきらめる
                ChordUtil.dprint(
                    "do_stabilize_successor_th," + ChordUtil.gen_debug_str_of_node(node.node_info)
                    + ",STABILIZE_FAILED_DUE_TO_INTERNAL_CONTROL_FLOW_EXCEPTION_RAISED")
*/

/*
def do_stabilize_ftable_th(node_list : List[ChordNode]):
    for times in range(0, gval.STABILIZE_FTABLE_BATCH_TIMES):
        for table_idx in range(0, gval.ID_SPACE_BITS):
            for node in node_list:
                ret = node.stabilizer.stabilize_finger_table(table_idx)
                if (ret.is_ok):
                    pass
                else:  # ret.err_code == ErrorCode.InternalControlFlowException_CODE
                    # join中のノードのノードオブジェクトを get_node_by_address しようとした場合に
                    # InternalCtronlFlowExceptionがraiseされてくるのでその場合は、対象ノードのstabilize_finger_tableはあきらめる
                    ChordUtil.dprint(
                        "do_stabilize_ftable_th," + ChordUtil.gen_debug_str_of_node(node.node_info)
                        + ",STABILIZE_FAILED_DUE_TO_INTERNAL_CONTROL_FLOW_EXCEPTION_RAISED")
*/

pub fn do_stabilize_once_at_all_node_ftable_without_new_th(node_list : Vec<chord_node::ChordNode>){
    for times in range(0, gval.STABILIZE_FTABLE_BATCH_TIMES) {
        for table_idx in range(0, gval.ID_SPACE_BITS) {
            for node in node_list {
                let ret = stabilizer::stabilize_finger_table(node, table_idx);
                if ret.is_ok {
                    //do nothing
                } else {  // ret.err_code == ErrorCode.InternalControlFlowException_CODE
                    // join中のノードのノードオブジェクトを get_node_by_address しようとした場合に
                    // InternalCtronlFlowExceptionがraiseされてくるのでその場合は、対象ノードのstabilize_finger_tableはあきらめる
                    chord_util::dprint(
                        &("do_stabilize_ftable_th,".to_string() + chord_util::gen_debug_str_of_node(node.node_info).as_str()
                        + ",STABILIZE_FAILED_DUE_TO_INTERNAL_CONTROL_FLOW_EXCEPTION_RAISED"));
                }
            }
        }
    }
}

/*
def do_stabilize_onace_at_all_node_ftable(node_list : List[ChordNode]) -> List[Thread]:
    list_len = len(node_list)
    range_start = 0
    # 小数点以下切り捨て
    basic_pass_node_cnt = int(list_len / gval.STABILIZE_THREAD_NUM)
    thread_list : List[Thread] = []
    for thread_idx in range(0, gval.STABILIZE_THREAD_NUM):
        if thread_idx == gval.STABILIZE_THREAD_NUM - 1:
            thread = threading.Thread(target=do_stabilize_ftable_th, name="ftable-" + str(thread_idx),
                                      args=([node_list[range_start:-1]]))
        else:
            thread = threading.Thread(target=do_stabilize_successor_th, name="ftable-" + str(thread_idx),
                                      args=([node_list[range_start:range_start + basic_pass_node_cnt]]))
            range_start += basic_pass_node_cnt
        thread.start()
        thread_list.append(thread)

    return thread_list
*/

// all_node_id辞書のvaluesリスト内から重複なく選択したノードに stabilize のアクションをとらせていく
pub fn do_stabilize_once_at_all_node(){
    // ロックの取得
    // ここで取得した値が無効とならない限り gval::global_datasへの別スレッドでのアクセスはブロックされる
    let gd_refcell = get_refcell_from_arc_with_locking!(gval::global_datas);

    chord_util::dprint(&("do_stabilize_once_at_all_node_0,START".to_string()));
    //with gval.lock_of_all_node_dict:
    let node_list = list(gval.all_node_dict.values());
    let shuffled_node_list : List[ChordNode] = random.sample(node_list, len(node_list));

    // TODO: (rust) 暫定実装としてスレッドを新たに立ち上げず全てのノードについて処理をする
    //              後で複数スレッドで行う形に戻すこと（必要だろうか・・・）
    do_stabilize_once_at_all_node_ftable_without_new_th(shuffled_node_list);

    // TODO: (rust) successorのstabilizeは後回し
    //thread_list_succ : List[Thread] = do_stabilize_onace_at_all_node_successor(shuffled_node_list)

    // TODO: (rust) 複数スレッドでの stabilizeも後回し
    //let thread_list_ftable : List[Thread] = do_stabilize_onace_at_all_node_ftable(shuffled_node_list)

    // 全てのスレッドが終了するまで待つ
    // 一つの呼び出しごとにブロックするが、その間に別にスレッドが終了しても
    // スレッドの処理が終了していることは担保できるため問題ない
/*
    for thread in thread_list_succ:
        thread.join()
*/
/*
    for thread in thread_list_ftable:
        thread.join();
*/

    check_nodes_connectivity();
}

/*
# all_node_id辞書のvaluesリスト内から重複なく選択したノードに stabilize のアクションをとらせていく
def do_stabilize_once_at_all_node():
    ChordUtil.dprint("do_stabilize_once_at_all_node_0,START")
    with gval.lock_of_all_node_dict:
        node_list = list(gval.all_node_dict.values())
        shuffled_node_list : List[ChordNode] = random.sample(node_list, len(node_list))
    thread_list_succ : List[Thread] = do_stabilize_onace_at_all_node_successor(shuffled_node_list)
    thread_list_ftable : List[Thread] = do_stabilize_onace_at_all_node_ftable(shuffled_node_list)

    # 全てのスレッドが終了するまで待つ
    # 一つの呼び出しごとにブロックするが、その間に別にスレッドが終了しても
    # スレッドの処理が終了していることは担保できるため問題ない
    for thread in thread_list_succ:
        thread.join()
    for thread in thread_list_ftable:
        thread.join()

    check_nodes_connectivity()
*/

pub fn node_join_th(){
    let mut abnn_tmp: i32;
    unsafe{
        abnn_tmp = gval::already_born_node_num.load(Ordering::Relaxed) as i32;
    }
    
    while abnn_tmp < gval::NODE_NUM_MAX {
        if abnn_tmp == gval::KEEP_NODE_NUM {            
            std::thread::sleep(std::time::Duration::from_millis((60 * 1000) as u64));
            unsafe{
                gval::is_network_constructed.store(true, Ordering::Relaxed);
                gval::JOIN_INTERVAL_SEC.store(120, Ordering::Relaxed); //20.0;
            }
            // // TODO: デバッグのために1000ノードに達したらjoinを止める。後で元に戻すこと!
            // //       at node_join_th
            // break
        }

        let jinterval_sec_tmp: i32;
        unsafe{
            jinterval_sec_tmp = gval::JOIN_INTERVAL_SEC.load(Ordering::Relaxed) as i32;
        }
        add_new_node();
        std::thread::sleep(std::time::Duration::from_millis((jinterval_sec_tmp * 1000) as u64));
        unsafe{
            abnn_tmp = gval::already_born_node_num.load(Ordering::Relaxed) as i32;
        }        
    }
}

/*
# TODO: 対応する処理を行うスクリプトの類が必要 node_join_th
def node_join_th():
    while gval.already_born_node_num < gval.NODE_NUM_MAX:
        if gval.already_born_node_num == gval.KEEP_NODE_NUM:
            time.sleep(60.0)
            gval.is_network_constructed = True
            gval.JOIN_INTERVAL_SEC = 120.0 #20.0
            # # TODO: デバッグのために1000ノードに達したらjoinを止める。後で元に戻すこと!
            # #       at node_join_th
            # break

        add_new_node()
        time.sleep(gval.JOIN_INTERVAL_SEC)
*/

pub fn stabilize_th(){
    loop{
        // 内部で適宜ロックを解放することで他のスレッドの処理も行えるようにしつつ
        // 呼び出し時点でのノードリストを対象に stabilize 処理を行う
        do_stabilize_once_at_all_node();
    }
}

/*        
def stabilize_th():
    while True:
        # 内部で適宜ロックを解放することで他のスレッドの処理も行えるようにしつつ
        # 呼び出し時点でのノードリストを対象に stabilize 処理を行う
        do_stabilize_once_at_all_node()
*/
fn main() {
    // 最初の1ノードはここで登録する
    let first_node = chord_node::new_and_join("".to_string(), true);
    let first_node_refcell = get_refcell_from_arc_with_locking!(first_node);
    let first_node_refmut = get_refmut_from_refcell!(first_node_refcell);
    let first_node_ni_refcell = get_refcell_from_arc_with_locking!(first_node_refmut.node_info);
    let first_node_ni_refmut = get_refmut_from_refcell!(first_node_ni_refcell);

    first_node_refmut.is_join_op_finished.store(true, Ordering::Relaxed);

    {
        let gd_refcell = get_refcell_from_arc_with_locking!(gval::global_datas);
        let gd_refmut = get_refmut_from_refcell!(gd_refcell);
        gd_refmut.all_node_dict.insert(first_node_ni_refmut.address_str.clone(), Arc::clone(&first_node));
    }    

    // 次に生成するノードが同一のアドレス文字列を持つことを避けるため
    std::thread::sleep(std::time::Duration::from_millis(500));

    // 複数のスレッドで GLOBAL_DATAS に触ってみる
    let mut thread_handles = vec![];
    thread_handles.push(std::thread::spawn(node_join_th));
    thread_handles.push(std::thread::spawn(stabilize_th));

    // スレッド終了の待ち合わせ（終了してくるスレッドは基本的に無い）
    for handle in thread_handles {
        handle.join().unwrap();
    }
/*    
    node_join_th_handle = threading.Thread(target=node_join_th, daemon=True)
    node_join_th_handle.start()

    stabilize_th_handle = threading.Thread(target=stabilize_th, daemon=True)
    stabilize_th_handle.start()
*/

    // data_put_th_handle = threading.Thread(target=data_put_th, daemon=True)
    // data_put_th_handle.start()

    // data_get_th_handle = threading.Thread(target=data_get_th, daemon=True)
    // data_get_th_handle.start()

    // node_kill_th_handle = threading.Thread(target=node_kill_th, daemon=True)
    // node_kill_th_handle.start()

    println!("Hello, world!");
/*
    while True:
        time.sleep(1)
*/
}
/*    
    # 再現性のため乱数シードを固定
    # ただし、複数スレッドが存在し、個々の処理の終了するタイミングや、どのタイミングで
    # スイッチするかは実行毎に異なる可能性があるため、あまり意味はないかもしれない
    random.seed(1337)

    # 最初の1ノードはここで登録する
    first_node = ChordNode("THIS_VALUE_IS_NOT_USED", first_node=True)
    first_node.is_join_op_finished = True
    gval.all_node_dict[first_node.node_info.address_str] = first_node
    time.sleep(0.5) #次に生成するノードが同一のアドレス文字列を持つことを避けるため

    node_join_th_handle = threading.Thread(target=node_join_th, daemon=True)
    node_join_th_handle.start()

    stabilize_th_handle = threading.Thread(target=stabilize_th, daemon=True)
    stabilize_th_handle.start()

    data_put_th_handle = threading.Thread(target=data_put_th, daemon=True)
    data_put_th_handle.start()

    data_get_th_handle = threading.Thread(target=data_get_th, daemon=True)
    data_get_th_handle.start()

    node_kill_th_handle = threading.Thread(target=node_kill_th, daemon=True)
    node_kill_th_handle.start()

    while True:
        time.sleep(1)
*/
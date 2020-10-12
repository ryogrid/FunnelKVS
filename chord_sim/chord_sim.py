# coding:utf-8

# アドレス文字列をキーとしてとり、対応するノードのChordNodeオブジェクトを返すハッシュ
# IPアドレスが分かれば、対応するノードと通信できることと対応している
all_node_dict = {}

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

    def __init__(self):
        print("hello Chord!")

    def put(self, key_str, value_str):
        print("not implemented yet")

    def get(self, key_str):
        print("not implemented yet")

    def delete(self, key_str):
        print("not implemented yet")

def main():
    print("not implemented yet")

if __name__ == '__main__':
    main()
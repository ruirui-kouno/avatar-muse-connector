#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
pair.py — アバターと MUSE をつなぐための接続ファイルを自動生成する。

ユーザーが JSON を手で編集することは一切ない。このスクリプトが:
  1. ペアリングコード（6桁）を発行する
  2. 接続設定 config.local.json を作る（アクセスキーは表示しない）
  3. MUSE に渡すだけの接続ファイル muse-connector.json を作る
までを全部やる。

使い方:
  python3 pair.py [--bridge http://127.0.0.1:8124]

アクセスキーの取得:
  - このPCでブリッジが動いている場合、ブリッジのキー置き場
    （~/workspace/avatar-org/bridge/access-key.txt）から自動で取り込む。
  - 無い場合は、ブリッジ側で表示されたキーを1回だけ貼り付ける
    （入力は画面に表示されない）。
"""
import argparse
import getpass
import json
import os
import secrets
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
OWNER_KEY_FILE = os.path.expanduser('~/workspace/avatar-org/bridge/access-key.txt')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--bridge', default='http://127.0.0.1:8124',
                    help='ブリッジのURL（既定 http://127.0.0.1:8124）')
    args = ap.parse_args()

    code = f'{secrets.randbelow(1000000):06d}'

    key = None
    source = None
    try:
        with open(OWNER_KEY_FILE, encoding='utf-8') as f:
            key = f.read().strip()
        source = 'このPCのブリッジから自動で取り込みました'
    except OSError:
        pass
    if not key:
        key = getpass.getpass('ブリッジのアクセスキーを貼り付けてください'
                              '（画面には表示されません）: ').strip()
        source = '入力されたキーを使います'
    if not key:
        print('キーが空のため中止しました')
        return 1

    cfg = {'bridge_url': args.bridge, 'access_key': key,
           'pairing_code': code}
    cfg_path = os.path.join(HERE, 'config.local.json')
    with open(cfg_path, 'w', encoding='utf-8') as f:
        json.dump(cfg, f, ensure_ascii=False, indent=1)
    os.chmod(cfg_path, 0o600)

    connector = {'mcpServers': {
        'avatar-muse': {
            'command': 'python3',
            'args': [os.path.join(HERE, 'avatar_mcp_server.py')]}}}
    conn_path = os.path.join(HERE, 'muse-connector.json')
    with open(conn_path, 'w', encoding='utf-8') as f:
        json.dump(connector, f, ensure_ascii=False, indent=1)

    print('ペアリングコード:', code)
    print('接続設定を作りました:', cfg_path, f'（{source}）')
    print('MUSE に渡すファイル:', conn_path)
    print()
    print('つぎの手順:')
    print('  1. 自分の MUSE に muse-connector.json をコネクタとして追加する')
    print('  2. MUSE に「アバターで接続テストをして」と言う')
    print('  3. 統括が一言喋れば接続完了です')
    return 0


if __name__ == '__main__':
    sys.exit(main())

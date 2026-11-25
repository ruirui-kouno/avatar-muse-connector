#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
pair_claim.py — アバターアプリ（v2）が出した番号だけで接続を完了する。

ユーザーの手順は「自分のMUSEに番号を伝える」だけ。このスクリプトは
MUSE 側で実行され、次を行う:
  1. 中継（avatar-relay2）の POST /pair-claim に番号を提示する
     （番号そのものが合言葉。合鍵はまだ持っていないので付けない）
  2. 返ってきた接続情報（中継URL・部屋ID・脳キー）を
     ~/.avatar2/config.json に保存する（権限600、脳キーは表示しない）

保存後は、同梱の brain.py デーモン（python3 brain.py run）がその設定を
読んで中継へ常時接続する。claim は冪等なので、回線の揺れに備えて
数回まで再試行する。

使い方:
  python3 pair_claim.py 123456 [--relay https://avatar-relay2.kouno2025kouno.workers.dev]

番号の有効期限は10分。期限切れや番号違いは理由をそのまま返す。
依存は Python 標準ライブラリのみ（urllib が通らない環境では curl に切替）。
"""
import argparse
import json
import os
import subprocess
import sys
import time
import urllib.request
import urllib.error

DEFAULT_RELAY = 'https://avatar-relay2.kouno2025kouno.workers.dev'
CONFIG_PATH = os.path.expanduser('~/.avatar2/config.json')
UA = 'Mozilla/5.0 (avatar-brain)'


def post_json(url, body):
    data = json.dumps(body).encode('utf-8')
    req = urllib.request.Request(url, data=data, method='POST',
                                 headers={'Content-Type': 'application/json',
                                          'User-Agent': UA})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, json.loads(resp.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read().decode('utf-8'))
        except Exception:
            return e.code, {}
    except Exception:
        # urllib が通らない環境（プロキシ等）では curl で肩代わりする
        out = subprocess.run(
            ['curl', '-s', '-X', 'POST', url, '-H', 'Content-Type: application/json',
             '-H', 'User-Agent: ' + UA,
             '-d', data.decode('utf-8'), '-w', '\n%{http_code}'],
            capture_output=True, text=True, timeout=40)
        text = out.stdout or ''
        payload, _, code = text.rpartition('\n')
        try:
            return int(code), json.loads(payload) if payload else {}
        except Exception:
            return 0, {}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('code', help='アバターアプリに表示された番号')
    ap.add_argument('--relay', default=DEFAULT_RELAY, help='中継のURL')
    args = ap.parse_args()

    code = (args.code or '').strip()
    if not (code.isdigit() and len(code) == 6):
        print('番号は6桁の数字で伝えてください')
        return 1

    relay = args.relay.rstrip('/')
    status, res = 0, {}
    for attempt in range(4):  # claim は冪等。回線・bot判定の揺れに備えて再試行する
        status, res = post_json(relay + '/pair-claim', {'code': code})
        if status in (200, 404, 410):
            break
        time.sleep(1.5 * (attempt + 1))
    if status == 404:
        print('その番号は見つかりませんでした。アプリに出ている番号を確認してください')
        return 1
    if status == 410:
        print('その番号は有効期限（10分）を過ぎています。アプリで新しい番号を出してください')
        return 1
    cfg = res.get('config') or {}
    if status != 200 or not res.get('ok') or not cfg.get('brain_key'):
        print(f'接続を完了できませんでした（応答: {status}）')
        return 1

    os.makedirs(os.path.dirname(CONFIG_PATH), exist_ok=True)
    with open(CONFIG_PATH, 'w', encoding='utf-8') as f:
        json.dump(cfg, f, ensure_ascii=False, indent=1)
    os.chmod(CONFIG_PATH, 0o600)

    print('接続を受け付けました。接続情報はこの端末に保存しました（脳キーは表示しません）。')
    print('次に、同梱の脳デーモンを常駐させてください: python3 brain.py run')
    print('デーモンが中継に接続すると、アプリに「つながりました」と表示されます。')
    return 0


if __name__ == '__main__':
    sys.exit(main())

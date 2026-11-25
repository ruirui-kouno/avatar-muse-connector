#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
avatar_mcp_server.py — アバター v2 の「身体」を MUSE に繋ぐ MCP アダプター。

設計（v2）:
  - 脳はユーザーの自分の MUSE。ここは「声と体」の受け口だけを担う。
  - 接続は中継（avatar-relay2）の部屋モデル。ペアリングは番号 claim で
    ~/.avatar2/config.json に保存される（pair_claim.py）。
  - 同梱の脳デーモン brain.py が中継と WebSocket で常時接続し、発話の保持・
    相槌・VOICEVOX 合成・声の機械決定を担う。このサーバーはデーモンの
    ローカル API（127.0.0.1、ポートは ~/.avatar2/daemon.json）の上に被せる
    薄い層で、デーモン本体は作り直さない。
  - MCP は stdio トランスポート、依存は Python 標準ライブラリのみ。

道具（ツール）:
  avatar_listen(timeout_sec)              次の発話を待って返す
  avatar_say(text, utterance_id, ...)     発話への回答（id指定）／発話に
                                          紐づかない発声（id無し＝通知）
  avatar_report(title, sections, ...)     報告形式で順に喋る
  avatar_status()                         デーモン・接続・エンジンの状態

v1 との違い:
  - avatar_set_background は廃止。v2 では背景はアプリ側（ガチャ／
    コレクション）で選ぶもので、脳側から切り替える経路は無い。
  - 声は発話のキャラからデーモンが機械決定する。ツール側は話者を選ばない
    （選ぶとキャラと声が食い違うため）。
脳キー等の秘密値はログ・出力に出さない。
"""
import json
import os
import sys
import time
import urllib.request
import urllib.error

HOME2 = os.environ.get('AVATAR2_HOME', os.path.expanduser('~/.avatar2'))
CONFIG_PATH = os.path.join(HOME2, 'config.json')
STATE_PATH = os.path.join(HOME2, 'daemon.json')
UA = 'Mozilla/5.0 (avatar-brain)'


def daemon_base():
    """デーモンのローカル API のベース URL。状態ファイルが無ければ None。"""
    try:
        with open(STATE_PATH, encoding='utf-8') as f:
            st = json.load(f)
        return f"http://127.0.0.1:{st['api_port']}"
    except Exception:
        return None


def daemon_call(method, path, body=None, timeout=120):
    base = daemon_base()
    if base is None:
        raise RuntimeError('脳デーモンの状態ファイルが見つかりません。'
                           '同梱の brain.py を起動してください: python3 brain.py run')
    data = json.dumps(body).encode('utf-8') if body is not None else None
    headers = {'User-Agent': UA}
    if data is not None:
        headers['Content-Type'] = 'application/json'
    req = urllib.request.Request(base + path, data=data, method=method,
                                 headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode('utf-8'))
    except Exception as e:
        raise RuntimeError('脳デーモンに接続できませんでした'
                           f'（{type(e).__name__}）。'
                           '同梱の brain.py が起動しているか確認してください: '
                           'python3 brain.py run')


# ---------------- ツール実装 ----------------

def tool_listen(args):
    timeout_sec = float(args.get('timeout_sec') or 120)
    deadline = time.time() + max(1.0, min(timeout_sec, 1800.0))
    while True:
        remain = deadline - time.time()
        if remain <= 0:
            return {'ok': True, 'utterance': None,
                    'note': '時間内に発話はありませんでした'}
        res = daemon_call('GET', f'/next?timeout={min(remain, 55):.0f}',
                          timeout=min(remain, 55) + 15)
        item = res.get('utterance')
        if item:
            return {'ok': True, 'utterance': item}


def tool_say(args):
    text = (args.get('text') or '').strip()
    if not text:
        raise ValueError('text が必要です')
    uid = args.get('utterance_id')
    title = args.get('title')
    if uid:
        # 発話への回答。声はデーモンが発話のキャラから機械決定する。
        # 同じ id への複数回の呼び出しは「続き」として送られるので、
        # 最初の一文を先に送ると初声が速くなる。
        res = daemon_call('POST', '/say',
                          {'utterance_id': uid, 'text': text,
                           'title': title or '回答'},
                          timeout=300)
    else:
        # 発話に紐づかない発声（朝の報告など）。char_id は任意。
        res = daemon_call('POST', '/notify',
                          {'text': text, 'char_id': args.get('char_id'),
                           'title': title or 'お知らせ'},
                          timeout=300)
    return {'ok': res.get('ok'), 'credit': res.get('credit')}


def tool_report(args):
    title = args.get('title') or '報告'
    sections = []
    for s in args.get('sections') or []:
        text = s.strip() if isinstance(s, str) else (s.get('text') or '').strip()
        if text:
            sections.append(text)
    if not sections:
        raise ValueError('sections に読み上げるテキストが必要です')
    uid = args.get('utterance_id')
    sent = 0
    credit = None
    for i, text in enumerate(sections):
        if uid:
            res = daemon_call('POST', '/say',
                              {'utterance_id': uid, 'text': text,
                               'title': title if i == 0 else '回答'},
                              timeout=300)
        else:
            res = daemon_call('POST', '/notify',
                              {'text': text, 'char_id': args.get('char_id'),
                               'title': title if i == 0 else 'お知らせ'},
                              timeout=300)
        if not res.get('ok'):
            return {'ok': False, 'sent': sent, 'credit': credit}
        sent += 1
        credit = res.get('credit') or credit
    return {'ok': True, 'sent': sent, 'credit': credit}


def tool_status(args):
    paired = os.path.isfile(CONFIG_PATH)
    try:
        res = daemon_call('GET', '/status', timeout=10)
        return {'ok': res.get('ok'), 'daemon': 'running', 'paired': paired,
                'connected': res.get('connected'),
                'pending': res.get('pending'), 'engine': res.get('engine')}
    except Exception as e:
        return {'ok': False, 'daemon': 'not running', 'paired': paired,
                'note': str(e)}


TOOLS = [
    {'name': 'avatar_listen',
     'description': 'ユーザーがアバターに話しかけるのを待ち、次の発話を返す'
                    '（会話の戻り経路）。発話には id・本文・キャラID・'
                    '呼び名・位置情報が含まれる。時間内に発話が無ければ '
                    'utterance=null で返る。',
     'inputSchema': {'type': 'object',
                     'properties': {
                         'timeout_sec': {'type': 'number',
                                         'description': '待つ秒数（既定120）'}}},
     'handler': tool_listen},
    {'name': 'avatar_say',
     'description': 'アバターに喋らせる。発話への回答は utterance_id を'
                    '付ける（声は発話のキャラの固定ボイスで自動決定され、'
                    '同じ id への複数回は続きになる。最初の一文を先に送ると'
                    '初声が速い）。utterance_id 無しは発話に紐づかない発声'
                    '（報告・お知らせ）で、char_id で声のキャラを指定できる。',
     'inputSchema': {'type': 'object',
                     'properties': {
                         'text': {'type': 'string', 'description': '喋るテキスト'},
                         'utterance_id': {'type': 'string',
                                          'description': '回答先の発話ID（任意）'},
                         'char_id': {'type': 'string',
                                     'description': '発声キャラID（任意・通知用）'},
                         'title': {'type': 'string',
                                   'description': '見出し（任意）'}},
                     'required': ['text']},
     'handler': tool_say},
    {'name': 'avatar_report',
     'description': '報告形式で喋らせる。本文（sections）を短い文の列で'
                    '渡すと、順に読み上げる。utterance_id を付けると、その'
                    '発話への回答として（続き扱いで）送る。朝の報告などに使う。',
     'inputSchema': {'type': 'object',
                     'properties': {
                         'title': {'type': 'string', 'description': '報告の題'},
                         'sections': {'type': 'array',
                                      'description': '読み上げる文の列（文字列）',
                                      'items': {'type': 'string'}},
                         'utterance_id': {'type': 'string',
                                          'description': '回答先の発話ID（任意）'},
                         'char_id': {'type': 'string',
                                     'description': '発声キャラID（任意・通知用）'}},
                     'required': ['sections']},
     'handler': tool_report},
    {'name': 'avatar_status',
     'description': '脳デーモンの起動・中継への接続・音声エンジン・'
                    '未処理の発話数を確認する。',
     'inputSchema': {'type': 'object', 'properties': {}},
     'handler': tool_status},
]


# ---------------- MCP (stdio, JSON-RPC) ----------------

def send(obj):
    sys.stdout.write(json.dumps(obj, ensure_ascii=False) + '\n')
    sys.stdout.flush()


def result_text(payload, is_error=False):
    return {'content': [{'type': 'text',
                         'text': json.dumps(payload, ensure_ascii=False)}],
            'isError': is_error}


def handle(msg):
    method = msg.get('method')
    mid = msg.get('id')
    if method == 'initialize':
        return {'jsonrpc': '2.0', 'id': mid,
                'result': {'protocolVersion': '2024-11-05',
                           'capabilities': {'tools': {}},
                           'serverInfo': {'name': 'avatar-muse',
                                          'version': '2.0.0'}}}
    if method == 'ping':
        return {'jsonrpc': '2.0', 'id': mid, 'result': {}}
    if method == 'tools/list':
        tools = [{k: t[k] for k in ('name', 'description', 'inputSchema')}
                 for t in TOOLS]
        return {'jsonrpc': '2.0', 'id': mid, 'result': {'tools': tools}}
    if method == 'tools/call':
        params = msg.get('params') or {}
        name = params.get('name')
        tool = next((t for t in TOOLS if t['name'] == name), None)
        if tool is None:
            return {'jsonrpc': '2.0', 'id': mid,
                    'result': result_text({'ok': False,
                                           'error': f'unknown tool: {name}'},
                                          is_error=True)}
        try:
            out = tool['handler'](params.get('arguments') or {})
            return {'jsonrpc': '2.0', 'id': mid, 'result': result_text(out)}
        except Exception as e:
            return {'jsonrpc': '2.0', 'id': mid,
                    'result': result_text({'ok': False, 'error': str(e)},
                                          is_error=True)}
    if mid is not None:
        return {'jsonrpc': '2.0', 'id': mid,
                'error': {'code': -32601, 'message': f'method not found: {method}'}}
    return None  # 通知には応答しない


def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except Exception:
            continue
        resp = handle(msg)
        if resp is not None:
            send(resp)


if __name__ == '__main__':
    main()

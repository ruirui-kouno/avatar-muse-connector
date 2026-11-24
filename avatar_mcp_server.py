#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
avatar_mcp_server.py — アバター「身体」ブリッジを MUSE に繋ぐ MCP アダプター。

設計（plugin-design-20261123.md）:
  - 脳はユーザーの自分の MUSE。ここは「声と体」の受け口だけを担う。
  - 既存ブリッジ（bridge_server.py）の HTTP API の上に被せる薄い層で、
    ブリッジ本体は作り直さない。
  - MCP は stdio トランスポート、依存は Python 標準ライブラリのみ。

道具（ツール）:
  avatar_say(text, emotion)          一言〜短い文を喋る（表情指定つき）
  avatar_listen(timeout_sec)         ユーザーの次の発話を待ってテキストで返す
  avatar_report(title, sections)     報告形式で喋る＋「本日の重点」パネル表示
  avatar_set_background(name)        背景切替イベントをステージへ送る
  avatar_status()                    接続とキャラの状態確認

接続情報の解決順:
  1. 環境変数 AVATAR_BRIDGE_URL / AVATAR_ACCESS_KEY
  2. このスクリプトと同じ場所の config.local.json（pair.py が生成）
  3. ~/.avatar-muse/config.json
  4. オーナー環境の既定: http://127.0.0.1:8124 と
     ~/workspace/avatar-org/bridge/access-key.txt（存在する場合のみ）
アクセスキーの値はログ・出力に出さない。
"""
import json
import os
import sys
import time
import urllib.request
import urllib.error

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_BRIDGE = 'http://127.0.0.1:8124'
OWNER_KEY_FILE = os.path.expanduser('~/workspace/avatar-org/bridge/access-key.txt')

EXPRESSIONS = ('happy', 'serious', 'relaxed')


def load_config():
    cfg = {}
    for path in (os.path.join(HERE, 'config.local.json'),
                 os.path.expanduser('~/.avatar-muse/config.json')):
        try:
            with open(path, encoding='utf-8') as f:
                cfg = json.load(f)
            break
        except Exception:
            continue
    bridge = (os.environ.get('AVATAR_BRIDGE_URL') or cfg.get('bridge_url')
              or DEFAULT_BRIDGE)
    key = os.environ.get('AVATAR_ACCESS_KEY') or cfg.get('access_key')
    if not key:
        try:
            with open(OWNER_KEY_FILE, encoding='utf-8') as f:
                key = f.read().strip()
        except OSError:
            key = None
    return bridge.rstrip('/'), key


BRIDGE, ACCESS_KEY = load_config()


def bridge_call(method, path, body=None, timeout=120):
    data = json.dumps(body).encode() if body is not None else None
    headers = {'Content-Type': 'application/json'}
    if ACCESS_KEY:
        headers['X-Access-Key'] = ACCESS_KEY
    req = urllib.request.Request(BRIDGE + path, data=data, method=method,
                                 headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode('utf-8'))


# ---------------- ツール実装 ----------------

def tool_say(args):
    text = (args.get('text') or '').strip()
    if not text:
        raise ValueError('text が必要です')
    emotion = args.get('emotion')
    if emotion in EXPRESSIONS:
        res = bridge_call('POST', '/say',
                          {'segments': [{'text': text, 'expression': emotion}]},
                          timeout=300)
    else:
        res = bridge_call('POST', '/say', {'text': text}, timeout=300)
    return {'ok': res.get('ok'), 'delivered_to': res.get('delivered_to'),
            'streamed': res.get('streamed')}


def tool_listen(args):
    timeout_sec = float(args.get('timeout_sec') or 120)
    deadline = time.time() + max(1.0, min(timeout_sec, 1800.0))
    while True:
        remain = deadline - time.time()
        if remain <= 0:
            return {'ok': True, 'utterance': None,
                    'note': '時間内に発話はありませんでした'}
        res = bridge_call('GET', f'/next-utterance?timeout={min(remain, 55):.0f}',
                          timeout=min(remain, 55) + 15)
        item = res.get('utterance')
        if item:
            return {'ok': True, 'utterance': item}


def tool_report(args):
    title = args.get('title') or '報告'
    sections = args.get('sections') or []
    segs, items = [], []
    for s in sections:
        if isinstance(s, str):
            text, expr = s.strip(), None
        else:
            text = (s.get('text') or '').strip()
            expr = s.get('expression') or s.get('x_expression')
        if not text:
            continue
        seg = {'text': text}
        if expr in EXPRESSIONS:
            seg['expression'] = expr
        segs.append(seg)
        items.append(text[:40])
    if not segs:
        raise ValueError('sections に読み上げるテキストが必要です')
    panel_items = args.get('panel_items') or items[:5]
    res = bridge_call('POST', '/say',
                      {'title': title, 'segments': segs,
                       'panel': {'title': args.get('panel_title') or '本日の重点',
                                 'items': panel_items}},
                      timeout=600)
    return {'ok': res.get('ok'), 'delivered_to': res.get('delivered_to'),
            'streamed': res.get('streamed')}


BG_INDEX = {'会議室': 0, 'boardroom': 0, '社長室': 1, 'president': 1,
            '夜景': 2, 'night': 2}


def tool_set_background(args):
    name = args.get('name')
    index = args.get('index')
    if index is None:
        if name not in BG_INDEX:
            raise ValueError('背景名は 会議室 / 社長室 / 夜景 のいずれか'
                             '（または index 0〜2）を指定してください')
        index = BG_INDEX[name]
    res = bridge_call('POST', '/control',
                      {'type': 'background', 'index': int(index), 'name': name},
                      timeout=15)
    return {'ok': res.get('ok'), 'delivered_to': res.get('delivered_to'),
            'index': int(index),
            'note': '切替イベントは接続中ステージへ配信済み。ステージ側の'
                    '背景イベント受信は今後の対応項目（README参照）'}


def tool_status(args):
    res = bridge_call('GET', '/health', timeout=10)
    return {'ok': res.get('ok'), 'bridge': BRIDGE,
            'engine': res.get('engine'), 'stages_connected': res.get('stages'),
            'utterances_pending': res.get('utterances_pending'),
            'stats': res.get('stats')}


TOOLS = [
    {'name': 'avatar_say',
     'description': 'アバターに一言〜短い文を喋らせる（声・口パク・表情つき）。'
                    'emotion は happy / serious / relaxed から選べる。',
     'inputSchema': {'type': 'object',
                     'properties': {
                         'text': {'type': 'string', 'description': '喋るテキスト'},
                         'emotion': {'type': 'string',
                                     'enum': list(EXPRESSIONS),
                                     'description': '表情（任意）'}},
                     'required': ['text']},
     'handler': tool_say},
    {'name': 'avatar_listen',
     'description': 'ユーザーがアバターに話しかけるのを待ち、次の発話を'
                    '文字起こしテキストで返す（会話の戻り経路）。'
                    '発話が無いまま時間切れした場合は utterance=null で返る。',
     'inputSchema': {'type': 'object',
                     'properties': {
                         'timeout_sec': {'type': 'number',
                                         'description': '待つ秒数（既定120）'}}},
     'handler': tool_listen},
    {'name': 'avatar_report',
     'description': '報告形式で喋らせる。本文（sections）を順に読み上げ、'
                    '「本日の重点」パネルに要点を表示する。朝の報告などに使う。',
     'inputSchema': {'type': 'object',
                     'properties': {
                         'title': {'type': 'string', 'description': '報告の題'},
                         'sections': {'type': 'array',
                                      'description': '読み上げる文の列（文字列、'
                                                     'または {text, expression}）',
                                      'items': {}},
                         'panel_items': {'type': 'array',
                                         'items': {'type': 'string'},
                                         'description': 'パネルに出す要点（任意）'}},
                     'required': ['sections']},
     'handler': tool_report},
    {'name': 'avatar_set_background',
     'description': 'ステージの背景を切り替える（会議室 / 社長室 / 夜景）。',
     'inputSchema': {'type': 'object',
                     'properties': {
                         'name': {'type': 'string',
                                  'description': '背景名'},
                         'index': {'type': 'integer',
                                   'description': '背景番号 0〜2（任意）'}}},
     'handler': tool_set_background},
    {'name': 'avatar_status',
     'description': 'ブリッジ接続・音声エンジン・接続中ステージ数・'
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
                                          'version': '1.0.0'}}}
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

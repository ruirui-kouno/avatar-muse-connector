#!/usr/bin/env python3
"""avatar brain daemon (v2) — MUSE 側に常駐する唯一の足回りプロセス。

役割は3つだけ:
  1. 中継（avatar-relay2）と WebSocket で持続接続し、発話を即座に受け取る。
  2. 発話が届いた瞬間、そのキャラの固定ボイスで相槌を返す（判断を待たない）。
  3. MUSE の返答テキストを VOICEVOX で合成し、口パクタイムライン付きで返す。

発話は中継側に durable に保持されるため、このプロセスが落ちても発話は消えない。
復帰すれば未回答分が古い順に再送される。見守り・監督・ポーリングは存在しない。

使い方:
  python3 brain.py claim --code 123456 --relay https://avatar-relay2.example
      → 接続番号で claim し、設定を ~/.avatar2/config.json に保存する（1回のHTTPS）。
  python3 brain.py run [--stub-answer 秒]
      → デーモンとして常駐する。--stub-answer は検証用: 受信後その秒数で
        定型の2分割回答を自動で返す（パイプライン遅延の計測用）。
  python3 brain.py listen [--timeout 55]
      → 次の未回答発話を1件 JSON で表示して終了する（MUSE の受け口）。
  python3 brain.py say --id <発話ID> --text "返答文" [--title 見出し]
      → 返答を合成して送る。同じ ID に複数回呼ぶと続きとして送られる
        （最初の一文を先に送り、続きを後から送る分割回答が標準）。
  python3 brain.py notify --text "文" [--char c01]
      → 発話に紐づかない発話（朝の報告など）を送る。
  python3 brain.py status

依存は Python 標準ライブラリのみ。VOICEVOX Engine は初回合成時に自動起動する。
中継に接続している間はエンジンを止めずに温かいまま保つ（常時接続＝ラグなし
の設計）。切断状態で30分使わなければ自動で止まる。
"""
import argparse
import base64
import hashlib
import io
import json
import os
import re
import socket
import ssl
import struct
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request
import wave
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HOME2 = os.environ.get('AVATAR2_HOME', os.path.expanduser('~/.avatar2'))
CONFIG_PATH = os.path.join(HOME2, 'config.json')
STATE_PATH = os.path.join(HOME2, 'daemon.json')
ENGINE_DIR = os.environ.get('AVATAR2_ENGINE_DIR',
                            os.path.expanduser('~/workspace/avatar-org/engine/extracted/linux-cpu-x64'))
ENGINE_URL = 'http://127.0.0.1:50021'
ENGINE_IDLE_STOP = 1800
API_PORT_DEFAULT = 8321
ACK_LINES = ['はい、承知しました', 'はい、少々お待ちください', 'はい、確認しますね']
VOWEL_MAP = {'a': 'a', 'i': 'i', 'u': 'u', 'e': 'e', 'o': 'o',
             'ア': 'a', 'イ': 'i', 'ウ': 'u', 'エ': 'e', 'オ': 'o'}


def log(*a):
    print('[brain]', *a, flush=True)


# ================================================================ WebSocket クライアント
class WSClient:
    """標準ライブラリだけの最小 WebSocket クライアント（プロキシ CONNECT 対応）。"""

    def __init__(self, url):
        self.url = url
        self.sock = None
        self.buf = b''
        self.send_lock = threading.Lock()
        self.alive = False

    def _connect_socket(self, host, port, use_tls):
        proxy = (os.environ.get('https_proxy') or os.environ.get('HTTPS_PROXY') or
                 os.environ.get('http_proxy') or os.environ.get('HTTP_PROXY'))
        no_proxy = (os.environ.get('no_proxy') or os.environ.get('NO_PROXY') or '')
        if proxy and any(host == e.strip() or host.endswith('.' + e.strip())
                         for e in no_proxy.split(',') if e.strip()):
            proxy = None
        raw = None
        if proxy:
            p = urllib.parse.urlparse(proxy)
            raw = socket.create_connection((p.hostname, p.port or 3128), timeout=20)
            auth = ''
            if p.username:
                cred = base64.b64encode(f'{p.username}:{p.password or ""}'.encode()).decode()
                auth = f'Proxy-Authorization: Basic {cred}\r\n'
            raw.sendall(f'CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\n{auth}\r\n'.encode())
            resp = b''
            while b'\r\n\r\n' not in resp:
                chunk = raw.recv(4096)
                if not chunk:
                    raise ConnectionError('proxy closed during CONNECT')
                resp += chunk
            if b' 200 ' not in resp.split(b'\r\n')[0]:
                raise ConnectionError('proxy CONNECT failed: ' + resp.split(b'\r\n')[0].decode('utf-8', 'replace'))
            self.buf = resp.split(b'\r\n\r\n', 1)[1]
        else:
            raw = socket.create_connection((host, port), timeout=20)
        if use_tls:
            ctx = ssl.create_default_context()
            self.sock = ctx.wrap_socket(raw, server_hostname=host)
        else:
            self.sock = raw

    def connect(self):
        u = urllib.parse.urlparse(self.url)
        use_tls = u.scheme in ('wss', 'https')
        host = u.hostname
        port = u.port or (443 if use_tls else 80)
        path = u.path + (('?' + u.query) if u.query else '')
        self._connect_socket(host, port, use_tls)
        key = base64.b64encode(os.urandom(16)).decode()
        req = (f'GET {path} HTTP/1.1\r\nHost: {host}\r\nUpgrade: websocket\r\n'
               f'Connection: Upgrade\r\nSec-WebSocket-Key: {key}\r\n'
               f'Sec-WebSocket-Version: 13\r\n'
               f'User-Agent: Mozilla/5.0 (avatar-brain)\r\n\r\n')
        self.sock.sendall(req.encode())
        resp = self.buf
        self.buf = b''
        while b'\r\n\r\n' not in resp:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise ConnectionError('closed during handshake')
            resp += chunk
        head, _, rest = resp.partition(b'\r\n\r\n')
        if b' 101 ' not in head.split(b'\r\n')[0]:
            raise ConnectionError('handshake failed: ' + head.split(b'\r\n')[0].decode('utf-8', 'replace'))
        self.buf = rest
        self.alive = True

    def _read(self, n):
        while len(self.buf) < n:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionError('closed')
            self.buf += chunk
        out, self.buf = self.buf[:n], self.buf[n:]
        return out

    def send(self, text):
        data = text.encode('utf-8')
        mask = os.urandom(4)
        header = bytearray([0x81])
        ln = len(data)
        if ln < 126:
            header.append(0x80 | ln)
        elif ln < 65536:
            header.append(0x80 | 126)
            header += struct.pack('>H', ln)
        else:
            header.append(0x80 | 127)
            header += struct.pack('>Q', ln)
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(data))
        with self.send_lock:
            self.sock.sendall(bytes(header) + mask + masked)

    def send_pong(self, data):
        mask = os.urandom(4)
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(data))
        with self.send_lock:
            self.sock.sendall(bytes([0x8A, 0x80 | len(data)]) + mask + masked)

    def recv(self):
        payload = b''
        while True:
            h = self._read(2)
            fin = h[0] & 0x80
            opcode = h[0] & 0x0F
            ln = h[1] & 0x7F
            if ln == 126:
                ln = struct.unpack('>H', self._read(2))[0]
            elif ln == 127:
                ln = struct.unpack('>Q', self._read(8))[0]
            data = self._read(ln) if ln else b''
            if opcode == 0x8:
                raise ConnectionError('close frame')
            if opcode == 0x9:
                self.send_pong(data)
                continue
            if opcode == 0xA:
                continue
            payload += data
            if fin:
                return payload.decode('utf-8', 'replace')

    def close(self):
        self.alive = False
        try:
            self.sock.close()
        except Exception:
            pass


# ================================================================ VOICEVOX 合成
class Synth:
    def __init__(self):
        self.proc = None
        self.spawned = False
        self.last_use = 0.0
        self.lock = threading.Lock()
        self._alive_ok_at = 0.0   # エンジン生存を確認した最終時刻（確認HTTPの省略用）
        self.keep_warm = None     # True を返す間はアイドル停止しない（接続中は止めない）

    def engine_alive(self):
        try:
            with urllib.request.urlopen(ENGINE_URL + '/version', timeout=2) as r:
                if r.status == 200:
                    self._alive_ok_at = time.time()
                    return True
                return False
        except Exception:
            return False

    def ensure_engine(self, force=False):
        # 直近10秒以内に生存確認か合成が成功していれば、確認HTTPを省略する
        if not force and time.time() - self._alive_ok_at < 10:
            return
        if self.engine_alive():
            return
        run = os.path.join(ENGINE_DIR, 'run')
        if not os.path.isfile(run):
            raise RuntimeError('VOICEVOX Engine が見つかりません: ' + run)
        self.proc = subprocess.Popen(
            [run, '--host', '127.0.0.1', '--port', '50021'],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.spawned = True
        for _ in range(60):
            if self.engine_alive():
                return
            time.sleep(0.5)
        raise RuntimeError('VOICEVOX Engine の起動に失敗しました')

    def post(self, path, params):
        url = ENGINE_URL + path + '?' + urllib.parse.urlencode(params)
        req = urllib.request.Request(url, method='POST')
        with urllib.request.urlopen(req, timeout=30) as r:
            out = r.read()
        self._alive_ok_at = time.time()
        return out

    def post_json(self, path, params, payload):
        url = ENGINE_URL + path + '?' + urllib.parse.urlencode(params)
        req = urllib.request.Request(url, data=json.dumps(payload).encode('utf-8'),
                                     headers={'Content-Type': 'application/json'},
                                     method='POST')
        with urllib.request.urlopen(req, timeout=60) as r:
            out = r.read()
        self._alive_ok_at = time.time()
        return out

    def build_segment(self, query):
        t = float(query.get('prePhonemeLength', 0.0))
        visemes = []
        for phrase in query['accent_phrases']:
            for mora in phrase['moras']:
                t += float(mora.get('consonant_length') or 0.0)
                vlen = float(mora.get('vowel_length') or 0.0)
                v = VOWEL_MAP.get(mora.get('vowel', ''))
                if v and vlen > 0:
                    visemes.append({'t': round(t, 4), 'd': round(vlen, 4), 'v': v})
                t += vlen
            pause = phrase.get('pause_mora')
            if pause:
                t += float(pause.get('vowel_length') or 0.0)
        t += float(query.get('postPhonemeLength', 0.0))
        return t, visemes

    def _parse_segments(self, text):
        parts = [p.strip() for p in re.split(r'(?<=[。！？\n])', text) if p.strip()]
        if not parts:
            parts = [text]
        segs = []
        for body in parts:
            expr = None
            m = re.match(r'^\[(happy|serious|relaxed)\]\s*', body)
            if m:
                expr = m.group(1)
                body = body[m.end():]
            segs.append({'text': body, 'expression': expr})
        for i, s in enumerate(segs):
            if s['expression'] is None:
                s['expression'] = 'happy' if i == 0 else (
                    'relaxed' if i == len(segs) - 1 else 'serious')
        return segs

    def _synth_segment(self, seg, voice):
        try:
            return self._synth_segment_once(seg, voice)
        except Exception:
            # エンジンが落ちていたら起こし直して1回だけ再試行する
            self.ensure_engine(force=True)
            return self._synth_segment_once(seg, voice)

    def _synth_segment_once(self, seg, voice):
        q = json.loads(self.post('/audio_query',
                                 {'text': seg['text'], 'speaker': voice['speaker']}))
        q['pitchScale'] = float(q.get('pitchScale', 0.0)) + float(voice.get('pitch', 0.0))
        q['speedScale'] = float(voice.get('speed', 1.0))
        dur, vis = self.build_segment(q)
        wav = self.post_json('/synthesis', {'speaker': voice['speaker']}, q)
        return wav, dur, vis

    @staticmethod
    def _split_first_chunk(segs):
        """第1文が長い場合、最初の読点で初回チャンクを切り分ける。
        初声までの合成対象を短くするための分割で、文の意味は変えない。"""
        if not segs:
            return segs
        body = segs[0]['text']
        if len(body) <= 24:
            return segs
        idx = body.find('、', 8)
        if idx < 0 or idx >= 24:
            return segs
        rest = body[idx + 1:]
        if not rest:
            return segs
        head = {'text': body[:idx + 1], 'expression': segs[0]['expression']}
        tail = {'text': rest, 'expression': segs[0]['expression']}
        return [head, tail] + segs[1:]

    def synthesize_segments(self, text, voice):
        """文ごとに合成し、1文ずつの package を順に yield する。
        本回答の初声を速くするため（全文の合成完了を待たない）。
        ロックは1文ごとに手放す。長い回答の合成が続いていても、別発話の
        相槌が文の合間に割り込めるようにするため。"""
        for s in self._split_first_chunk(self._parse_segments(text)):
            with self.lock:
                self.ensure_engine()
                self.last_use = time.time()
                wav, dur, vis = self._synth_segment(s, voice)
            timeline = {'version': 1, 'totalDuration': round(dur, 4),
                        'segments': [{'start': 0.0, 'duration': round(dur, 4),
                                      'text': s['text'], 'expression': s['expression']}],
                        'visemes': [{'t': v['t'], 'd': v['d'], 'v': v['v']} for v in vis]}
            yield {'audio_base64': base64.b64encode(wav).decode(),
                   'audio_type': 'audio/wav', 'timeline': timeline,
                   'credit': 'VOICEVOX:' + voice.get('speaker_name', ''),
                   'text': s['text']}

    def synthesize(self, text, voice):
        """text を voice（speaker/pitch/speed）で合成し package 本体を返す。"""
        with self.lock:
            self.ensure_engine()
            self.last_use = time.time()
            segs_in = self._parse_segments(text)
            wav_chunks = []
            seg_meta = []
            all_visemes = []
            start = 0.0
            for s in segs_in:
                wav, dur, vis = self._synth_segment(s, voice)
                wav_chunks.append(wav)
                seg_meta.append({'start': round(start, 4), 'duration': round(dur, 4),
                                 'text': s['text'], 'expression': s['expression']})
                for v in vis:
                    all_visemes.append({'t': round(start + v['t'], 4), 'd': v['d'], 'v': v['v']})
                start += dur + 0.12
            out = io.BytesIO()
            with wave.open(out, 'wb') as wf:
                for i, chunk in enumerate(wav_chunks):
                    with wave.open(io.BytesIO(chunk), 'rb') as wr:
                        if i == 0:
                            wf.setparams(wr.getparams())
                        wf.writeframes(wr.readframes(wr.getnframes()))
                        if i < len(wav_chunks) - 1:
                            silence = b'\x00' * int(wf.getframerate() * 0.12) * wf.getsampwidth() * wf.getnchannels()
                            wf.writeframes(silence)
            total = seg_meta[-1]['start'] + seg_meta[-1]['duration'] if seg_meta else 0.0
            timeline = {'version': 1, 'totalDuration': round(total, 4),
                        'segments': seg_meta, 'visemes': all_visemes}
            return {'audio_base64': base64.b64encode(out.getvalue()).decode(),
                    'audio_type': 'audio/wav', 'timeline': timeline,
                    'credit': 'VOICEVOX:' + voice.get('speaker_name', '')}

    def idle_watcher(self):
        while True:
            time.sleep(60)
            if (self.spawned and self.last_use and
                    time.time() - self.last_use > ENGINE_IDLE_STOP and
                    not (self.keep_warm and self.keep_warm())):
                try:
                    self.proc.terminate()
                except Exception:
                    pass
                self.spawned = False
                log('VOICEVOX Engine を30分無使用のため停止（切断中のみ）')


# ================================================================ デーモン本体
class Daemon:
    def __init__(self, config, stub_answer=None):
        self.config = config
        self.stub_answer = stub_answer
        self.synth = Synth()
        self.ws = None
        self.connected = threading.Event()
        # 接続している間はエンジンを止めない（常時接続で温かいまま保つ）
        self.synth.keep_warm = lambda: self.connected.is_set()
        self.utterance_event = threading.Event()  # 発話到着で /next を即座に起こす
        self._prebuilding = False
        self.pending = {}        # id -> utterance（未回答）
        self.handed = {}         # id -> listen に渡した時刻
        self.answering = set()   # 回答の合成・送信が進行中の id（listen へ二重に出さない）
        self.answered = set()    # このデーモンが回答済みにした id
        self.lock = threading.Lock()
        self.catalog = {'chars': {}}
        self.ack_cache = {}
        self.api_port = API_PORT_DEFAULT
        os.makedirs(HOME2, exist_ok=True)
        os.makedirs(os.path.join(HOME2, 'ack'), exist_ok=True)

    # ---------- カタログ・声 ----------
    def _fetch_catalog_text(self, url):
        """カタログJSONを取得する。gzipで小さく受け、失敗したら再試行する。"""
        import gzip as gz
        req = urllib.request.Request(url, headers={
            'User-Agent': UA, 'Accept-Encoding': 'gzip'})
        with urllib.request.urlopen(req, timeout=10) as r:
            raw = r.read()
        if raw[:2] == b'\x1f\x8b':
            raw = gz.decompress(raw)
        return raw.decode('utf-8')

    def load_catalog(self):
        cache = os.path.join(HOME2, 'catalog.json')
        data = None
        last_err = None
        for attempt in range(4):  # 取得は冪等。回線の揺れに備えて再試行する
            try:
                got = json.loads(self._fetch_catalog_text(
                    self.config['relay'] + '/catalog.json'))
                if not got.get('chars'):
                    raise ValueError('catalog has no chars')
                data = got
                break
            except Exception as e:
                last_err = e
                time.sleep(1.0 * (attempt + 1))
        if data is not None:
            with open(cache, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False)
        else:
            log('カタログ取得に失敗、キャッシュを使用:', last_err)
            if os.path.isfile(cache):
                with open(cache, encoding='utf-8') as f:
                    data = json.load(f)
        if data:
            self.catalog = {'chars': {c['id']: c for c in data.get('chars', [])}}
        log('カタログ読み込み: キャラ', len(self.catalog['chars']), '体')

    def voice_for(self, char_id):
        c = self.catalog['chars'].get(char_id or '')
        if c:
            return {'speaker': c['speaker'], 'pitch': c.get('pitch', 0.0),
                    'speed': c.get('speed', 1.0), 'speaker_name': c.get('speaker_name', ''),
                    'line': c.get('line', ''), 'name': c.get('name', '')}
        return {'speaker': 14, 'pitch': 0.0, 'speed': 1.0,
                'speaker_name': '冥鳴ひまり', 'line': '', 'name': ''}

    # ---------- 送信 ----------
    def ws_send(self, obj):
        if self.ws and self.ws.alive:
            self.ws.send(json.dumps(obj, ensure_ascii=False))

    def send_say(self, utterance_id, kind, pkg):
        pkg = dict(pkg)
        self.ws_send({'t': 'say', 'utterance_id': utterance_id, 'kind': kind, 'pkg': pkg})

    def ack_for(self, u):
        return self.ack_pkg_for_char(u.get('char_id') or 'default')

    def ack_pkg_for_char(self, char_id):
        if char_id in self.ack_cache:
            return self.ack_cache[char_id]
        cache_file = os.path.join(HOME2, 'ack', char_id + '.json')
        if os.path.isfile(cache_file):
            with open(cache_file, encoding='utf-8') as f:
                pkg = json.load(f)
            self.ack_cache[char_id] = pkg
            return pkg
        voice = self.voice_for(None if char_id == 'default' else char_id)
        line = ACK_LINES[0]
        pkg = self.synth.synthesize(line, voice)
        pkg['text'] = line
        pkg['title'] = '相槌'
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(pkg, f, ensure_ascii=False)
        self.ack_cache[char_id] = pkg
        return pkg

    def prebuild_acks(self):
        """全キャラの相槌を先に用意する（キャラ初回の相槌が遅くならないように）。
        キャッシュ済みは読み込むだけ。裏で走らせる前提。"""
        for cid in list(self.catalog['chars'].keys()) or ['default']:
            try:
                self.ack_pkg_for_char(cid)
            except Exception as e:
                log('相槌の先行用意に失敗:', cid, e)

    def prebuild_acks_async(self):
        with self.lock:
            if self._prebuilding:
                return
            self._prebuilding = True

        def _run():
            try:
                self.prebuild_acks()
            finally:
                with self.lock:
                    self._prebuilding = False
        threading.Thread(target=_run, daemon=True).start()

    def on_utterance(self, u):
        uid = u['id']
        # エンジンの先行ウォームは真っ先に始める（相槌・本回答の合成を速くする）
        self._warm_engine_async()
        with self.lock:
            if uid in self.answered:
                return
            self.pending[uid] = u
        self.utterance_event.set()  # listen の待ちを即座に起こす
        try:
            self.ws_send({'t': 'received', 'id': uid})
        except Exception:
            pass
        # 相槌は本回答より必ず先に届ける。キャッシュ済みなら一瞬、
        # 未キャッシュでも同期で用意してから次へ進む（順序の構造保証）
        try:
            pkg = self.ack_for(u)
            self.send_say(uid, 'ack', pkg)
        except Exception as e:
            log('相槌の合成に失敗:', e)
        if self.stub_answer is not None:
            def _stub():
                time.sleep(self.stub_answer)
                # 実エージェントが listen で受け取り済みなら、スタブは答えない（譲る）
                for _ in range(40):
                    with self.lock:
                        if uid not in self.pending or uid in self.answered:
                            return
                        handed_at = self.handed.get(uid, 0)
                    if not handed_at or time.time() - handed_at > 120:
                        break
                    time.sleep(0.5)
                else:
                    return
                self.answer(uid, 'かしこまりました。いま確認しています。'
                            '詳しい内容が分かり次第、続けてお伝えしますね。',
                            title='自動応答（検証用）')
            threading.Thread(target=_stub, daemon=True).start()

    def answer(self, uid, text, title='回答'):
        with self.lock:
            if uid:
                self.answering.add(uid)
            u = self.pending.get(uid)
        try:
            char_id = (u or {}).get('char_id')
            voice = self.voice_for(char_id)
            credit = None
            # 文ごとに合成して先行送信する（初声までの時間を短くする）
            for pkg in self.synth.synthesize_segments(text, voice):
                pkg['title'] = title
                self.send_say(uid, 'answer', pkg)
                credit = pkg['credit']
            with self.lock:
                if uid:
                    self.answered.add(uid)
                    self.pending.pop(uid, None)
                    self.handed.pop(uid, None)
            return {'ok': True, 'credit': credit}
        finally:
            with self.lock:
                if uid:
                    self.answering.discard(uid)

    def notify(self, text, char_id=None, title='お知らせ'):
        voice = self.voice_for(char_id)
        pkg = self.synth.synthesize(text, voice)
        pkg['text'] = text
        pkg['title'] = title
        self.send_say(None, 'notify', pkg)
        return {'ok': True, 'credit': pkg['credit']}

    def preview(self, char_id, req=None):
        voice = self.voice_for(char_id)
        line = voice.get('line') or 'こんにちは'
        pkg = self.synth.synthesize(line, voice)
        pkg['text'] = line
        pkg['title'] = '登場ボイス'
        pkg['req'] = req
        pkg['char_id'] = char_id
        self.send_say(None, 'preview', pkg)

    # ---------- WS ループ ----------
    def _keepalive(self, ws):
        """25秒ごとにアプリ層 ping を送る（中継が pong を返す）。
        アイドル切断を防ぎ、常時接続を維持するための保活。"""
        while self.ws is ws and ws.alive:
            time.sleep(25)
            if self.ws is ws and ws.alive:
                try:
                    ws.send(json.dumps({'t': 'ping'}))
                except Exception:
                    return

    def _warm_engine_async(self):
        threading.Thread(target=lambda: self.synth.ensure_engine()
                         if not self.synth.engine_alive() else None, daemon=True).start()

    def ws_loop(self):
        relay = self.config['relay'].rstrip('/')
        ws_base = relay.replace('https://', 'wss://').replace('http://', 'ws://')
        url = (f"{ws_base}/ws/brain?room={urllib.parse.quote(self.config['room'])}"
               f"&key={urllib.parse.quote(self.config['brain_key'])}")
        backoff = 0.5
        while True:
            try:
                self.ws = WSClient(url)
                self.ws.connect()
                self.connected.set()
                log('中継に接続しました')
                backoff = 0.5
                threading.Thread(target=self._keepalive, args=(self.ws,),
                                 daemon=True).start()
                # 接続のたびに温め直す（切断中に冷えた分を取り戻す）
                self._warm_engine_async()
                self.prebuild_acks_async()
                while True:
                    raw = self.ws.recv()
                    try:
                        msg = json.loads(raw)
                    except ValueError:
                        continue
                    if msg.get('t') == 'utterance':
                        self.on_utterance(msg['u'])
                    elif msg.get('t') == 'hello':
                        for u in msg.get('pending') or []:
                            self.on_utterance(u)
                    elif msg.get('t') == 'preview':
                        threading.Thread(target=self.preview,
                                         args=(msg.get('char_id'), msg.get('req')),
                                         daemon=True).start()
                    elif msg.get('t') == 'speak':
                        threading.Thread(target=self.notify,
                                         args=(msg.get('text') or '', msg.get('char_id')),
                                         daemon=True).start()
            except Exception as e:
                log('中継との接続が切れました:', e, f'({backoff}秒後に再接続)')
                self.connected.clear()
                if self.ws:
                    self.ws.close()
                time.sleep(backoff)
                # 再接続は短い間隔で粘る（発話は中継側に保持され、復帰後に再送される）
                backoff = min(backoff * 2, 5)

    # ---------- ローカル API（listen / say の受け口） ----------
    def api_handler(self):
        daemon = self

        class H(BaseHTTPRequestHandler):
            def log_message(self, fmt, *args):
                pass

            def _json(self, code, obj):
                body = json.dumps(obj, ensure_ascii=False).encode('utf-8')
                self.send_response(code)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):
                u = urllib.parse.urlparse(self.path)
                qs = urllib.parse.parse_qs(u.query)
                if u.path == '/status':
                    return self._json(200, {'ok': True, 'connected': daemon.connected.is_set(),
                                            'pending': len(daemon.pending),
                                            'engine': daemon.synth.engine_alive()})
                if u.path == '/next':
                    timeout = float((qs.get('timeout') or ['50'])[0])
                    deadline = time.time() + timeout
                    while True:
                        # 発話到着で即座に起きる（0.25秒ポーリングの待ちを無くす）
                        daemon.utterance_event.clear()
                        with daemon.lock:
                            now = time.time()
                            for uid, utt in sorted(daemon.pending.items(),
                                                   key=lambda kv: kv[1].get('ts', 0)):
                                if uid in daemon.answering:
                                    continue
                                if daemon.handed.get(uid, 0) < now - 120:
                                    daemon.handed[uid] = now
                                    return self._json(200, {'ok': True, 'utterance': utt})
                        if time.time() >= deadline:
                            return self._json(200, {'ok': True, 'utterance': None})
                        daemon.utterance_event.wait(0.25)
                return self._json(404, {'ok': False, 'error': 'not found'})

            def do_POST(self):
                u = urllib.parse.urlparse(self.path)
                ln = int(self.headers.get('Content-Length') or 0)
                raw = self.rfile.read(ln) if ln else b''
                try:
                    d = json.loads(raw.decode('utf-8')) if raw else {}
                except ValueError:
                    d = {}
                try:
                    if u.path == '/say':
                        r = daemon.answer(d.get('utterance_id'), d.get('text') or '',
                                          d.get('title') or '回答')
                        return self._json(200, r)
                    if u.path == '/notify':
                        r = daemon.notify(d.get('text') or '', d.get('char_id'),
                                          d.get('title') or 'お知らせ')
                        return self._json(200, r)
                except Exception as e:
                    return self._json(500, {'ok': False, 'error': str(e)})
                return self._json(404, {'ok': False, 'error': 'not found'})

        return H

    def run(self):
        self.load_catalog()
        # 起動直後にエンジンを温めておく（最初の発話で冷間起動を踏ませない）
        self._warm_engine_async()
        # 全キャラの相槌も先に用意しておく（どのキャラでも初回から速い）
        self.prebuild_acks_async()
        server = ThreadingHTTPServer(('127.0.0.1', self.api_port), self.api_handler())
        with open(STATE_PATH, 'w', encoding='utf-8') as f:
            json.dump({'api_port': self.api_port, 'pid': os.getpid(),
                       'room': self.config['room']}, f)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        threading.Thread(target=self.synth.idle_watcher, daemon=True).start()
        log(f'ローカルAPI: 127.0.0.1:{self.api_port}')
        self.ws_loop()


# ================================================================ CLI
def load_config():
    with open(CONFIG_PATH, encoding='utf-8') as f:
        return json.load(f)


def api_base():
    with open(STATE_PATH, encoding='utf-8') as f:
        st = json.load(f)
    return f"http://127.0.0.1:{st['api_port']}"


UA = 'Mozilla/5.0 (avatar-brain)'


def http_json(url, payload=None, timeout=70):
    if payload is None:
        req = urllib.request.Request(url, headers={'User-Agent': UA})
    else:
        req = urllib.request.Request(url, data=json.dumps(payload).encode('utf-8'),
                                     headers={'Content-Type': 'application/json',
                                              'User-Agent': UA})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode('utf-8'))


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest='cmd', required=True)
    p = sub.add_parser('claim')
    p.add_argument('--code', required=True)
    p.add_argument('--relay', required=True)
    p = sub.add_parser('run')
    p.add_argument('--stub-answer', type=float, default=None)
    p.add_argument('--api-port', type=int, default=API_PORT_DEFAULT)
    p = sub.add_parser('listen')
    p.add_argument('--timeout', type=float, default=55)
    p = sub.add_parser('say')
    p.add_argument('--id', required=True)
    p.add_argument('--text', required=True)
    p.add_argument('--title', default='回答')
    p = sub.add_parser('notify')
    p.add_argument('--text', required=True)
    p.add_argument('--char', default=None)
    p.add_argument('--title', default='お知らせ')
    sub.add_parser('status')
    args = ap.parse_args()

    if args.cmd == 'claim':
        url = args.relay.rstrip('/') + '/pair-claim'
        req = urllib.request.Request(url, data=json.dumps({'code': args.code}).encode('utf-8'),
                                     headers={'Content-Type': 'application/json',
                                              'User-Agent': UA})
        res = None
        for attempt in range(4):  # claim は冪等。回線・bot判定の揺れに備えて再試行する
            try:
                with urllib.request.urlopen(req, timeout=30) as r:
                    res = json.loads(r.read().decode('utf-8'))
                break
            except Exception:
                if attempt == 3:
                    raise
                time.sleep(1.5 * (attempt + 1))
        if not res.get('ok'):
            print(json.dumps(res, ensure_ascii=False))
            sys.exit(1)
        os.makedirs(HOME2, exist_ok=True)
        with open(CONFIG_PATH, 'w', encoding='utf-8') as f:
            json.dump(res['config'], f, ensure_ascii=False, indent=1)
        os.chmod(CONFIG_PATH, 0o600)
        print(json.dumps({'ok': True, 'saved': CONFIG_PATH,
                          'room': res['config']['room']}, ensure_ascii=False))
        return
    if args.cmd == 'run':
        config = load_config()
        d = Daemon(config, stub_answer=args.stub_answer)
        d.api_port = args.api_port
        d.run()
        return
    base = api_base()
    if args.cmd == 'listen':
        r = http_json(f'{base}/next?timeout={args.timeout}', timeout=args.timeout + 10)
        print(json.dumps(r.get('utterance'), ensure_ascii=False))
        return
    if args.cmd == 'say':
        r = http_json(f'{base}/say', {'utterance_id': args.id, 'text': args.text,
                                      'title': args.title})
        print(json.dumps(r, ensure_ascii=False))
        return
    if args.cmd == 'notify':
        r = http_json(f'{base}/notify', {'text': args.text, 'char_id': args.char,
                                         'title': args.title})
        print(json.dumps(r, ensure_ascii=False))
        return
    if args.cmd == 'status':
        r = http_json(f'{base}/status', timeout=5)
        print(json.dumps(r, ensure_ascii=False))
        return


if __name__ == '__main__':
    main()

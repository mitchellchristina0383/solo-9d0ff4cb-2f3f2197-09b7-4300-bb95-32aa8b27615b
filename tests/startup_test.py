#!/usr/bin/env python3
"""启动 / 恢复 / 多实例接力测试（仅标准库，需本机可运行 uvicorn）。

覆盖：
1. 两个实例同时指向同一个空卷启动，都能起来并查到同一份空名单、目标 1。
2. 首次启动被 kill -9 打断后，用原卷重启可恢复。
3. 旧卷只剩初始名单（无 state 行）或只剩空表时，可原地恢复。
4. 已有演练时，新实例启动期间监控仍可向原实例上报；
   新实例就绪后沿用名单/目标/轮号/累计/事件，并继续接收该轮上报。

用法: python3 tests/startup_test.py
"""
import json
import os
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FAILURES = []
WORK = tempfile.mkdtemp(prefix="scaler-startup-")


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f"  | {detail}" if detail else ""))
    if not cond:
        FAILURES.append(name)


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def call(port, method, path, body=None, timeout=10):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=data,
                                 method=method, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


def start(db_path, port):
    existing = os.environ.get("PYTHONPATH")
    env = dict(os.environ, SCALER_DB=str(db_path),
               PYTHONPATH=os.pathsep.join(filter(None, [str(ROOT), existing])))
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app",
         "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning"],
        cwd=str(ROOT), env=env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    return proc


def wait_ready(port, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            return call(port, "GET", "/health", timeout=2)
        except (urllib.error.URLError, ConnectionError, TimeoutError, OSError):
            time.sleep(0.05)
    return None


def stop(*procs):
    for p in procs:
        if p.poll() is None:
            p.terminate()
    for p in procs:
        try:
            p.wait(timeout=10)
        except subprocess.TimeoutExpired:
            p.kill()
            p.wait()


def seed_partial_volume(path, mode):
    """模拟首次启动中断留下的卷。"""
    conn = sqlite3.connect(path)
    if mode == "zero":
        pass  # 零字节文件
    elif mode == "tables":
        conn.executescript("""
            CREATE TABLE rosters (version INTEGER PRIMARY KEY, pods TEXT NOT NULL, created_at TEXT NOT NULL);
            CREATE TABLE reports (roster_version INTEGER NOT NULL, round INTEGER NOT NULL, pod TEXT NOT NULL,
                                  pending INTEGER NOT NULL, received_at TEXT NOT NULL,
                                  PRIMARY KEY (roster_version, round, pod));
            CREATE TABLE state (id INTEGER PRIMARY KEY CHECK (id = 1), target INTEGER NOT NULL,
                                roster_version INTEGER NOT NULL, max_round_seen INTEGER,
                                last_complete_round INTEGER, last_suggestion INTEGER,
                                reason TEXT NOT NULL, updated_at TEXT NOT NULL);
            CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT NOT NULL, kind TEXT NOT NULL,
                                 old_target INTEGER NOT NULL, new_target INTEGER NOT NULL,
                                 roster_version INTEGER NOT NULL, round INTEGER, reason TEXT NOT NULL);
        """)
    elif mode == "roster_only":
        # 中断在“写下初始名单后、写 state 前”：旧版本会在这里因重复主键启动失败。
        conn.executescript("""
            CREATE TABLE rosters (version INTEGER PRIMARY KEY, pods TEXT NOT NULL, created_at TEXT NOT NULL);
            INSERT INTO rosters(version, pods, created_at) VALUES (0, '[]', 'seed');
        """)
    conn.commit()
    conn.close()


def scenario_partial_recovery():
    print("场景 B：首次启动中断后的原卷可原地恢复")
    for mode in ("zero", "tables", "roster_only"):
        d = Path(WORK) / f"partial-{mode}"
        d.mkdir(parents=True)
        db = d / "scaler.db"
        seed_partial_volume(str(db), mode)
        port = free_port()
        proc = start(db, port)
        ready = wait_ready(port)
        try:
            check(f"[{mode}] 可启动并就绪", ready is not None)
            if ready:
                roster = call(port, "GET", "/roster")
                target = call(port, "GET", "/target")
                check(f"[{mode}] 名单为空、版号 0", roster == {"roster_version": 0, "pods": []}, str(roster))
                check(f"[{mode}] 目标副本数 1", target["target"] == 1, str(target["target"]))
        finally:
            stop(proc)


def scenario_two_on_empty_volume():
    print("场景 A：两个实例同时用同一个空卷启动")
    d = Path(WORK) / "empty"
    d.mkdir(parents=True)
    db = d / "scaler.db"
    p1, p2 = free_port(), free_port()
    a, b = start(db, p1), start(db, p2)
    try:
        ha, hb = wait_ready(p1), wait_ready(p2)
        check("两个实例都启动成功", ha is not None and hb is not None,
              f"a={ha is not None} b={hb is not None}")
        if ha and hb:
            ra, rb = call(p1, "GET", "/roster"), call(p2, "GET", "/roster")
            ta, tb = call(p1, "GET", "/target"), call(p2, "GET", "/target")
            check("两实例查到同一份空名单",
                  ra == rb == {"roster_version": 0, "pods": []}, f"{ra} / {rb}")
            check("两实例目标副本数均为 1", ta["target"] == tb["target"] == 1,
                  f"{ta['target']} / {tb['target']}")
            check("两实例 /docs 可用",
                  urllib.request.urlopen(f"http://127.0.0.1:{p1}/docs", timeout=5).status == 200
                  and urllib.request.urlopen(f"http://127.0.0.1:{p2}/docs", timeout=5).status == 200)
    finally:
        stop(a, b)


def scenario_failover_during_drill():
    print("场景 C：演练中启动新实例，监控持续向原实例上报，新实例接续")
    d = Path(WORK) / "drill"
    d.mkdir(parents=True)
    db = d / "scaler.db"
    pa, pb = free_port(), free_port()
    a = start(db, pa)
    b = None
    try:
        check("原实例就绪", wait_ready(pa) is not None)
        v = call(pa, "POST", "/roster", {"pods": ["p1", "p2", "p3"]})["roster_version"]
        r = call(pa, "POST", "/reports",
                 {"roster_version": v, "round": 1, "pod": "p1", "pending": 25})
        check("演练已产生扩容（目标 3）", r["target"] == 3 and r["scaling"]["kind"] == "scale_up")
        call(pa, "POST", "/reports",
             {"roster_version": v, "round": 1, "pod": "p2", "pending": 5})
        before_state = call(pa, "GET", "/state")
        before_target = call(pa, "GET", "/target")
        before_events = call(pa, "GET", "/events")["events"]

        # 新实例启动期间持续向原实例上报，直到新实例就绪。
        b = start(db, pb)
        accepted = failed = 0
        while wait_ready(pb, timeout=0.05) is None:
            try:
                resp = call(pa, "POST", "/reports",
                            {"roster_version": v, "round": 1, "pod": "p3", "pending": 0}, timeout=10)
                accepted += resp["outcome"] == "accepted"
            except (urllib.error.URLError, OSError):
                failed += 1
            if b.poll() is not None:
                break
            time.sleep(0.01)
        check("启动期间原实例上报无失败", failed == 0, f"failed={failed}")
        check("启动期间原实例至少受理 1 次上报", accepted >= 1, f"accepted={accepted}")
        # 新实例就绪后，新实例与原实例看到的状态必须一致。
        check("新实例就绪", wait_ready(pb) is not None)
        roster_a = call(pa, "GET", "/roster")
        target_a = call(pa, "GET", "/target")
        state_a = call(pa, "GET", "/state")
        events_a = call(pa, "GET", "/events")["events"]

        roster_b = call(pb, "GET", "/roster")
        target_b = call(pb, "GET", "/target")
        state_b = call(pb, "GET", "/state")
        events_b = call(pb, "GET", "/events")["events"]
        check("新实例沿用名单版号", roster_b == roster_a and roster_b["roster_version"] == v,
              f"{roster_b} / {roster_a}")
        check("新实例沿用目标副本数", target_b["target"] == target_a["target"] == 3,
              f"{target_b['target']} / {target_a['target']}")
        check("新实例沿用当前轮", state_b["max_round_seen"] == state_a["max_round_seen"] == 1,
              f"{state_b['max_round_seen']} / {state_a['max_round_seen']}")
        check("新实例沿用缩容累计", state_b["low_load_streak"] == state_a["low_load_streak"],
              f"{state_b['low_load_streak']} / {state_a['low_load_streak']}")
        check("新实例沿用事件流", events_b == events_a and len(events_b) >= len(before_events),
              f"{len(events_b)} / {len(events_a)} / before={len(before_events)}")
        check("新实例看到本轮已上报的三个 Pod",
              sorted(r["pod"] for r in state_b["active_round_reports"]) == ["p1", "p2", "p3"])

        # 在新实例上继续同一轮（轮 2）上报
        rr = call(pb, "POST", "/reports", {"roster_version": v, "round": 2, "pod": "p1", "pending": 2})
        check("新实例继续接收该轮上报", rr["outcome"] == "accepted", rr.get("reason", ""))
    finally:
        stop(a, b) if b else stop(a)


def scenario_kill_during_first_boot():
    print("场景 D：首次启动被强杀后，原卷重启可恢复")
    d = Path(WORK) / "killed"
    d.mkdir(parents=True)
    db = d / "scaler.db"
    env = dict(os.environ, PYTHONPATH=str(ROOT))
    code = ("import sys; from app.scaler import Scaler; "
            f"Scaler({str(db)!r})")
    killed = 0
    for _ in range(8):
        if db.exists():
            db.unlink()
        for suffix in ("-wal", "-shm"):
            p = Path(str(db) + suffix)
            if p.exists():
                p.unlink()
        proc = subprocess.Popen([sys.executable, "-c", code], env=env, cwd=str(ROOT))
        deadline = time.monotonic() + 5
        while not db.exists() and time.monotonic() < deadline and proc.poll() is None:
            time.sleep(0.001)
        if proc.poll() is None:
            proc.send_signal(signal.SIGKILL)
            killed += 1
        proc.wait()
    check("确实在启动过程中强杀过进程", killed > 0, f"killed={killed}")

    port = free_port()
    proc = start(db, port)
    try:
        check("强杀后原卷可重启", wait_ready(port) is not None)
        roster = call(port, "GET", "/roster")
        target = call(port, "GET", "/target")
        check("恢复后名单为空、版号 0", roster == {"roster_version": 0, "pods": []}, str(roster))
        check("恢复后目标为 1", target["target"] == 1)
        check("数据库完整性检查通过", sqlite3.connect(db).execute("PRAGMA integrity_check").fetchone()[0] == "ok")
    finally:
        stop(proc)


if __name__ == "__main__":
    try:
        scenario_two_on_empty_volume()
        scenario_partial_recovery()
        scenario_failover_during_drill()
        scenario_kill_during_first_boot()
    finally:
        shutil.rmtree(WORK, ignore_errors=True)
    print()
    if FAILURES:
        print(f"失败 {len(FAILURES)} 项: {FAILURES}")
        sys.exit(1)
    print("全部启动/恢复场景通过 ✔")

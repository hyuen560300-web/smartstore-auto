# -*- coding: utf-8 -*-
"""중앙 학습 DB — PostgreSQL (DATABASE_URL) + JSON 파일 폴백.

PostgreSQL 사용: DATABASE_URL 환경변수 설정 시 자동 활성화
폴백 모드:       data/db_fallback/*.jsonl — PostgreSQL 없어도 즉시 동작
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

KST = timezone(timedelta(hours=9))
DATABASE_URL = os.environ.get("DATABASE_URL", "")
_FALLBACK_DIR = Path(__file__).parent / "data" / "db_fallback"

# psycopg2 선택적 임포트
_psycopg2 = None
try:
    import psycopg2
    import psycopg2.extras
    _psycopg2 = psycopg2
except ImportError:
    pass


def _now_kst() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")


def _get_conn():
    if not _psycopg2 or not DATABASE_URL:
        return None
    try:
        return _psycopg2.connect(DATABASE_URL, connect_timeout=5)
    except Exception:
        return None


# ─── 스키마 생성 ──────────────────────────────────────────────────────────────

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS actions (
    id          SERIAL PRIMARY KEY,
    timestamp   TIMESTAMPTZ DEFAULT NOW(),
    agent_name  VARCHAR(100) NOT NULL,
    action_type VARCHAR(100) NOT NULL,
    service     VARCHAR(50)  DEFAULT '',
    input_data  JSONB,
    metadata    JSONB
);
CREATE TABLE IF NOT EXISTS results (
    id          SERIAL PRIMARY KEY,
    action_id   INTEGER REFERENCES actions(id) ON DELETE SET NULL,
    timestamp   TIMESTAMPTZ DEFAULT NOW(),
    metric_type VARCHAR(100) NOT NULL,
    value       FLOAT,
    service     VARCHAR(50)  DEFAULT '',
    metadata    JSONB
);
CREATE TABLE IF NOT EXISTS kpi_history (
    id          SERIAL PRIMARY KEY,
    date        DATE NOT NULL,
    service     VARCHAR(50)  NOT NULL,
    metric_name VARCHAR(100) NOT NULL,
    value       FLOAT,
    UNIQUE (date, service, metric_name)
);
CREATE TABLE IF NOT EXISTS trend_performance (
    id             SERIAL PRIMARY KEY,
    trend_keyword  VARCHAR(200) NOT NULL,
    recommended_at TIMESTAMPTZ  DEFAULT NOW(),
    service        VARCHAR(50)  DEFAULT '',
    views_before   FLOAT        DEFAULT 0,
    views_after    FLOAT,
    sales_before   FLOAT        DEFAULT 0,
    sales_after    FLOAT,
    score          FLOAT,
    evaluated_at   TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS meeting_decisions (
    id               SERIAL PRIMARY KEY,
    meeting_at       TIMESTAMPTZ DEFAULT NOW(),
    decision         TEXT        NOT NULL,
    service          VARCHAR(50) DEFAULT '',
    command          VARCHAR(100) DEFAULT '',
    expected_outcome TEXT,
    actual_outcome   TEXT,
    kpi_before       JSONB,
    kpi_after        JSONB,
    evaluated_at     TIMESTAMPTZ,
    score            FLOAT
);
CREATE INDEX IF NOT EXISTS idx_kpi_ds   ON kpi_history       (date, service);
CREATE INDEX IF NOT EXISTS idx_trend_at ON trend_performance  (recommended_at);
CREATE INDEX IF NOT EXISTS idx_mtg_at   ON meeting_decisions  (meeting_at);
"""


def ensure_schema() -> bool:
    """테이블 생성. 성공(PG) True, 폴백(JSON) False."""
    conn = _get_conn()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute(_SCHEMA_SQL)
            conn.commit()
            conn.close()
            return True
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    # JSON 폴백 초기화
    _FALLBACK_DIR.mkdir(parents=True, exist_ok=True)
    for name in ("actions", "results", "trend_performance", "meeting_decisions"):
        p = _FALLBACK_DIR / f"{name}.jsonl"
        if not p.exists():
            p.write_text("", encoding="utf-8")
    kpi_p = _FALLBACK_DIR / "kpi_history.json"
    if not kpi_p.exists():
        kpi_p.write_text("{}", encoding="utf-8")
    return False


# ─── 행동 기록 ────────────────────────────────────────────────────────────────

def log_action(
    agent_name: str,
    action_type: str,
    service: str = "",
    input_data: dict | None = None,
    metadata: dict | None = None,
) -> int:
    """행동 기록. action_id 반환 (실패 시 -1)."""
    conn = _get_conn()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO actions (agent_name, action_type, service, input_data, metadata)
                       VALUES (%s,%s,%s,%s,%s) RETURNING id""",
                    (agent_name, action_type, service,
                     json.dumps(input_data) if input_data else None,
                     json.dumps(metadata) if metadata else None),
                )
                aid = cur.fetchone()[0]
            conn.commit()
            conn.close()
            return aid
        except Exception:
            try:
                conn.rollback(); conn.close()
            except Exception:
                pass

    try:
        _FALLBACK_DIR.mkdir(parents=True, exist_ok=True)
        aid = int(time.time() * 1000) % 2_147_483_647
        with open(_FALLBACK_DIR / "actions.jsonl", "a", encoding="utf-8") as f:
            f.write(json.dumps({
                "id": aid, "timestamp": _now_kst(), "agent_name": agent_name,
                "action_type": action_type, "service": service,
                "input_data": input_data, "metadata": metadata,
            }, ensure_ascii=False) + "\n")
        return aid
    except Exception:
        return -1


# ─── 결과 기록 ────────────────────────────────────────────────────────────────

def log_result(
    action_id: int,
    metric_type: str,
    value: float | None,
    service: str = "",
    metadata: dict | None = None,
) -> bool:
    conn = _get_conn()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO results (action_id, metric_type, value, service, metadata)
                       VALUES (%s,%s,%s,%s,%s)""",
                    (action_id if action_id > 0 else None, metric_type, value, service,
                     json.dumps(metadata) if metadata else None),
                )
            conn.commit()
            conn.close()
            return True
        except Exception:
            try:
                conn.rollback(); conn.close()
            except Exception:
                pass

    try:
        _FALLBACK_DIR.mkdir(parents=True, exist_ok=True)
        with open(_FALLBACK_DIR / "results.jsonl", "a", encoding="utf-8") as f:
            f.write(json.dumps({
                "id": int(time.time() * 1000) % 2_147_483_647,
                "action_id": action_id, "timestamp": _now_kst(),
                "metric_type": metric_type, "value": value,
                "service": service, "metadata": metadata,
            }, ensure_ascii=False) + "\n")
        return True
    except Exception:
        return False


# ─── KPI 히스토리 ─────────────────────────────────────────────────────────────

def log_kpi(date_str: str, service: str, metric_name: str, value: float | None) -> bool:
    if value is None:
        return False
    v = float(value)
    conn = _get_conn()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO kpi_history (date, service, metric_name, value)
                       VALUES (%s,%s,%s,%s)
                       ON CONFLICT (date, service, metric_name) DO UPDATE SET value=EXCLUDED.value""",
                    (date_str, service, metric_name, v),
                )
            conn.commit()
            conn.close()
            return True
        except Exception:
            try:
                conn.rollback(); conn.close()
            except Exception:
                pass

    try:
        _FALLBACK_DIR.mkdir(parents=True, exist_ok=True)
        kpi_p = _FALLBACK_DIR / "kpi_history.json"
        try:
            data = json.loads(kpi_p.read_text(encoding="utf-8"))
        except Exception:
            data = {}
        data[f"{date_str}|{service}|{metric_name}"] = v
        kpi_p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        return True
    except Exception:
        return False


def get_kpi_history(service: str, metric_name: str, days: int = 7) -> list[tuple[str, float]]:
    """최근 N일 KPI 값 조회. [(date_str, value)] 최신순."""
    conn = _get_conn()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT date::text, value FROM kpi_history
                       WHERE service=%s AND metric_name=%s
                         AND date >= CURRENT_DATE - (%s || ' days')::INTERVAL
                       ORDER BY date DESC""",
                    (service, metric_name, str(days)),
                )
                rows = cur.fetchall()
            conn.close()
            return [(r[0], r[1]) for r in rows if r[1] is not None]
        except Exception:
            try:
                conn.close()
            except Exception:
                pass

    try:
        kpi_p = _FALLBACK_DIR / "kpi_history.json"
        if not kpi_p.exists():
            return []
        data = json.loads(kpi_p.read_text(encoding="utf-8"))
        today = datetime.now(KST).date()
        result = []
        for i in range(days):
            d = (today - timedelta(days=i)).strftime("%Y-%m-%d")
            key = f"{d}|{service}|{metric_name}"
            if key in data:
                result.append((d, data[key]))
        return result
    except Exception:
        return []


def detect_anomaly(service: str, metric_name: str, current_value: float, days: int = 7) -> dict | None:
    """N일 평균 대비 ±30% 이상이면 이상치 dict 반환, 아니면 None."""
    history = get_kpi_history(service, metric_name, days)
    if len(history) < 3:
        return None
    avg = sum(v for _, v in history) / len(history)
    if avg == 0:
        return None
    change_pct = (current_value - avg) / avg * 100
    if abs(change_pct) >= 30.0:
        return {
            "service": service, "metric": metric_name,
            "current": current_value, "avg_7d": round(avg, 2),
            "change_pct": round(change_pct, 1),
            "direction": "▲" if change_pct > 0 else "▼",
        }
    return None


# ─── 트렌드 성과 ──────────────────────────────────────────────────────────────

def log_trend(keyword: str, service: str, views_before: float = 0, sales_before: float = 0) -> int:
    conn = _get_conn()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO trend_performance
                       (trend_keyword, service, views_before, sales_before)
                       VALUES (%s,%s,%s,%s) RETURNING id""",
                    (keyword, service, views_before, sales_before),
                )
                tid = cur.fetchone()[0]
            conn.commit()
            conn.close()
            return tid
        except Exception:
            try:
                conn.rollback(); conn.close()
            except Exception:
                pass

    try:
        _FALLBACK_DIR.mkdir(parents=True, exist_ok=True)
        tid = int(time.time() * 1000) % 2_147_483_647
        with open(_FALLBACK_DIR / "trend_performance.jsonl", "a", encoding="utf-8") as f:
            f.write(json.dumps({
                "id": tid, "trend_keyword": keyword, "recommended_at": _now_kst(),
                "service": service, "views_before": views_before, "sales_before": sales_before,
                "views_after": None, "sales_after": None, "score": None, "evaluated_at": None,
            }, ensure_ascii=False) + "\n")
        return tid
    except Exception:
        return -1


def update_trend_score(trend_id: int, views_after: float, sales_after: float = 0) -> bool:
    conn = _get_conn()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT views_before FROM trend_performance WHERE id=%s", (trend_id,))
                row = cur.fetchone()
                vb = (row[0] or 0) if row else 0
                score = (views_after - vb) / max(vb, 1) if vb > 0 else views_after / 1000.0
                cur.execute(
                    """UPDATE trend_performance
                       SET views_after=%s, sales_after=%s, score=%s, evaluated_at=NOW()
                       WHERE id=%s""",
                    (views_after, sales_after, score, trend_id),
                )
            conn.commit()
            conn.close()
            return True
        except Exception:
            try:
                conn.rollback(); conn.close()
            except Exception:
                pass

    try:
        path = _FALLBACK_DIR / "trend_performance.jsonl"
        if not path.exists():
            return False
        lines = path.read_text(encoding="utf-8").splitlines()
        updated = []
        for line in lines:
            if not line.strip():
                continue
            rec = json.loads(line)
            if rec["id"] == trend_id:
                vb = rec.get("views_before", 0) or 0
                score = (views_after - vb) / max(vb, 1) if vb > 0 else views_after / 1000.0
                rec.update({"views_after": views_after, "sales_after": sales_after,
                             "score": round(score, 3), "evaluated_at": _now_kst()})
            updated.append(json.dumps(rec, ensure_ascii=False))
        path.write_text("\n".join(updated) + "\n", encoding="utf-8")
        return True
    except Exception:
        return False


def get_trend_scores(limit: int = 10) -> list[dict]:
    """평가 완료된 트렌드 성과 상위 조회."""
    conn = _get_conn()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT trend_keyword, service, score, views_before, views_after,
                              recommended_at::text, evaluated_at::text
                       FROM trend_performance
                       WHERE score IS NOT NULL
                       ORDER BY score DESC LIMIT %s""",
                    (limit,),
                )
                cols = [d[0] for d in cur.description]
                rows = [dict(zip(cols, r)) for r in cur.fetchall()]
            conn.close()
            return rows
        except Exception:
            try:
                conn.close()
            except Exception:
                pass

    try:
        path = _FALLBACK_DIR / "trend_performance.jsonl"
        if not path.exists():
            return []
        rows = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            rec = json.loads(line)
            if rec.get("score") is not None:
                rows.append(rec)
        rows.sort(key=lambda x: x.get("score", 0), reverse=True)
        return rows[:limit]
    except Exception:
        return []


def get_unevaluated_trends(days_old: int = 7) -> list[dict]:
    """추천 후 N일 이상 경과했지만 미평가 트렌드 조회 (7일 성과 측정용)."""
    cutoff = (datetime.now(KST) - timedelta(days=days_old)).strftime("%Y-%m-%d")
    conn = _get_conn()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT id, trend_keyword, service, views_before, recommended_at::text
                       FROM trend_performance
                       WHERE score IS NULL
                         AND recommended_at::date <= %s""",
                    (cutoff,),
                )
                cols = [d[0] for d in cur.description]
                rows = [dict(zip(cols, r)) for r in cur.fetchall()]
            conn.close()
            return rows
        except Exception:
            try:
                conn.close()
            except Exception:
                pass

    try:
        path = _FALLBACK_DIR / "trend_performance.jsonl"
        if not path.exists():
            return []
        rows = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            rec = json.loads(line)
            if rec.get("score") is None and rec.get("recommended_at", "") <= cutoff + " 23:59:59":
                rows.append(rec)
        return rows
    except Exception:
        return []


# ─── 회의 결정 추적 ────────────────────────────────────────────────────────────

def log_meeting_decision(
    decision: str,
    service: str = "",
    command: str = "",
    expected_outcome: str = "",
    kpi_before: dict | None = None,
) -> int:
    conn = _get_conn()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO meeting_decisions
                       (decision, service, command, expected_outcome, kpi_before)
                       VALUES (%s,%s,%s,%s,%s) RETURNING id""",
                    (decision, service, command, expected_outcome,
                     json.dumps(kpi_before) if kpi_before else None),
                )
                did = cur.fetchone()[0]
            conn.commit()
            conn.close()
            return did
        except Exception:
            try:
                conn.rollback(); conn.close()
            except Exception:
                pass

    try:
        _FALLBACK_DIR.mkdir(parents=True, exist_ok=True)
        did = int(time.time() * 1000) % 2_147_483_647
        with open(_FALLBACK_DIR / "meeting_decisions.jsonl", "a", encoding="utf-8") as f:
            f.write(json.dumps({
                "id": did, "meeting_at": _now_kst(), "decision": decision,
                "service": service, "command": command,
                "expected_outcome": expected_outcome, "kpi_before": kpi_before,
                "actual_outcome": None, "kpi_after": None,
                "evaluated_at": None, "score": None,
            }, ensure_ascii=False) + "\n")
        return did
    except Exception:
        return -1


def evaluate_meeting_decision(
    decision_id: int,
    actual_outcome: str,
    kpi_after: dict | None = None,
    score: float | None = None,
) -> bool:
    conn = _get_conn()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """UPDATE meeting_decisions
                       SET actual_outcome=%s, kpi_after=%s, score=%s, evaluated_at=NOW()
                       WHERE id=%s""",
                    (actual_outcome, json.dumps(kpi_after) if kpi_after else None,
                     score, decision_id),
                )
            conn.commit()
            conn.close()
            return True
        except Exception:
            try:
                conn.rollback(); conn.close()
            except Exception:
                pass

    try:
        path = _FALLBACK_DIR / "meeting_decisions.jsonl"
        if not path.exists():
            return False
        lines = path.read_text(encoding="utf-8").splitlines()
        updated = []
        for line in lines:
            if not line.strip():
                continue
            rec = json.loads(line)
            if rec["id"] == decision_id:
                rec.update({"actual_outcome": actual_outcome, "kpi_after": kpi_after,
                             "score": score, "evaluated_at": _now_kst()})
            updated.append(json.dumps(rec, ensure_ascii=False))
        path.write_text("\n".join(updated) + "\n", encoding="utf-8")
        return True
    except Exception:
        return False


def get_past_decisions(days: int = 7, service: str | None = None) -> list[dict]:
    """최근 N일 회의 결정 조회. 최신순."""
    conn = _get_conn()
    if conn:
        try:
            with conn.cursor() as cur:
                if service:
                    cur.execute(
                        """SELECT id, meeting_at::text, decision, service, command,
                                  expected_outcome, actual_outcome, score
                           FROM meeting_decisions
                           WHERE meeting_at >= NOW() - (%s || ' days')::INTERVAL
                             AND service = %s
                           ORDER BY meeting_at DESC""",
                        (str(days), service),
                    )
                else:
                    cur.execute(
                        """SELECT id, meeting_at::text, decision, service, command,
                                  expected_outcome, actual_outcome, score
                           FROM meeting_decisions
                           WHERE meeting_at >= NOW() - (%s || ' days')::INTERVAL
                           ORDER BY meeting_at DESC""",
                        (str(days),),
                    )
                cols = [d[0] for d in cur.description]
                rows = [dict(zip(cols, r)) for r in cur.fetchall()]
            conn.close()
            return rows
        except Exception:
            try:
                conn.close()
            except Exception:
                pass

    try:
        path = _FALLBACK_DIR / "meeting_decisions.jsonl"
        if not path.exists():
            return []
        cutoff = (datetime.now(KST) - timedelta(days=days)).strftime("%Y-%m-%d")
        rows = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            rec = json.loads(line)
            ts = rec.get("meeting_at", "")
            if ts[:10] >= cutoff:
                if service is None or rec.get("service") == service:
                    rows.append(rec)
        rows.sort(key=lambda x: x.get("meeting_at", ""), reverse=True)
        return rows
    except Exception:
        return []


# ─── DB 상태 조회 ─────────────────────────────────────────────────────────────

def get_db_status() -> dict:
    conn = _get_conn()
    if conn:
        try:
            counts = {}
            with conn.cursor() as cur:
                for t in ("actions", "results", "kpi_history", "trend_performance", "meeting_decisions"):
                    cur.execute(f"SELECT COUNT(*) FROM {t}")
                    counts[t] = cur.fetchone()[0]
            conn.close()
            return {"mode": "postgresql", "tables": counts}
        except Exception:
            try:
                conn.close()
            except Exception:
                pass

    counts = {}
    for name in ("actions", "results", "trend_performance", "meeting_decisions"):
        p = _FALLBACK_DIR / f"{name}.jsonl"
        counts[name] = sum(1 for l in (p.read_text(encoding="utf-8").splitlines() if p.exists() else []) if l.strip())
    kpi_p = _FALLBACK_DIR / "kpi_history.json"
    counts["kpi_history"] = len(json.loads(kpi_p.read_text(encoding="utf-8"))) if kpi_p.exists() else 0
    return {"mode": "json_fallback", "tables": counts}

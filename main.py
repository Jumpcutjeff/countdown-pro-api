import os
from datetime import date
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import psycopg


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten later to your Next.js domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_URL = os.getenv("SUPABASE_DB_URL")


def _get_conn():
    if not DB_URL:
        raise RuntimeError("Missing SUPABASE_DB_URL env var")
    return psycopg.connect(DB_URL, autocommit=True)


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/charts/weeks")
def list_weeks(
    year: int = Query(..., ge=1955, le=2019),
    limit: int = Query(60, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """
    Returns available weeks for a year, newest first.
    """
    sql = """
      select id, year, week_end_date
      from public.chart_weeks
      where year = %s
      order by week_end_date desc
      limit %s offset %s
    """
    try:
        with _get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql, (year, limit, offset))
            rows = cur.fetchall()
        return [{"id": r[0], "year": r[1], "week_end_date": r[2].isoformat()} for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")


@app.get("/charts/week")
def get_week(
    week_end_date: str = Query(..., description="YYYY-MM-DD"),
    top: int = Query(5, ge=1, le=100),
):
    """
    Returns Top N for a specific week_end_date.
    """
    week_sql = """
      select id, year, week_end_date
      from public.chart_weeks
      where week_end_date = %s::date
      limit 1
    """
    entries_sql = """
      select position, artist, song_title
      from public.chart_entries
      where chart_week_id = %s
      order by position asc
      limit %s
    """
    try:
        with _get_conn() as conn, conn.cursor() as cur:
            cur.execute(week_sql, (week_end_date,))
            week = cur.fetchone()
            if not week:
                raise HTTPException(status_code=404, detail="Week not found")

            week_id, year, wed = week
            cur.execute(entries_sql, (week_id, top))
            entries = cur.fetchall()

        return {
            "week": {"id": week_id, "year": year, "week_end_date": wed.isoformat()},
            "entries": [
                {"position": r[0], "artist": r[1], "song_title": r[2]} for r in entries
            ],
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

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


@app.get("/charts/resolve")
def resolve_chart_week(
    target_date: Optional[str] = Query(None, description="YYYY-MM-DD (e.g., 1985-03-15)"),
    year: Optional[int] = Query(None, ge=1955, le=2019),
    month: Optional[int] = Query(None, ge=1, le=12),
    week_in_month: Optional[int] = Query(None, ge=1, le=6, description="1=first chart week in month"),
    top: int = Query(5, ge=1, le=100),
):
    if not target_date and not (year and month and week_in_month):
        raise HTTPException(
            status_code=400,
            detail="Provide either target_date=YYYY-MM-DD OR year+month+week_in_month",
        )

    if target_date and (year or month or week_in_month):
        raise HTTPException(
            status_code=400,
            detail="Use only one mode: target_date OR year+month+week_in_month",
        )

    resolve_by_date_sql = """
      select id, year, week_end_date
      from public.chart_weeks
      order by abs(public.chart_weeks.week_end_date - %s::date) asc
      limit 1
    """

    resolve_by_week_in_month_sql = """
      select id, year, week_end_date
      from public.chart_weeks
      where year = %s
        and extract(month from week_end_date) = %s
      order by week_end_date asc
      offset %s
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
            if target_date:
                cur.execute(resolve_by_date_sql, (target_date,))
            else:
                cur.execute(resolve_by_week_in_month_sql, (year, month, week_in_month - 1))

            week = cur.fetchone()
            if not week:
                raise HTTPException(status_code=404, detail="No chart week found for that input")

            week_id, y, wed = week

            cur.execute(entries_sql, (week_id, top))
            entries = cur.fetchall()

        wed_str = wed.isoformat()

        if target_date:
            resolution_note = f"Closest chart week to {target_date} (week ending {wed_str})"
        else:
            suffix = "th"
            if week_in_month in (1, 2, 3):
                suffix = {1: "st", 2: "nd", 3: "rd"}[week_in_month]
            resolution_note = f"{week_in_month}{suffix} chart week of {month}/{year} (week ending {wed_str})"

        return {
            "resolved_from": (
                {"target_date": target_date}
                if target_date
                else {"year": year, "month": month, "week_in_month": week_in_month}
            ),
            "resolution_note": resolution_note,
            "week": {"id": week_id, "year": y, "week_end_date": wed_str},
            "entries": [{"position": r[0], "artist": r[1], "song_title": r[2]} for r in entries],
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

from fastapi import Query
from datetime import date
from typing import Optional
import psycopg

# ... keep your existing code above ...

@app.get("/charts/resolve")
def resolve_chart_week(
    # use one of these two modes
    target_date: Optional[str] = Query(None, description="YYYY-MM-DD (e.g., 1985-03-15)"),
    year: Optional[int] = Query(None, ge=1955, le=2019),
    month: Optional[int] = Query(None, ge=1, le=12),
    week_in_month: Optional[int] = Query(None, ge=1, le=6, description="1=first chart week in month"),
    top: int = Query(5, ge=1, le=100),
):
    """
    Mode A: target_date=YYYY-MM-DD  -> choose closest chart week_end_date to that date
    Mode B: year, month, week_in_month -> choose nth chart week_end_date within that month
    """
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
                # offset is zero-based
                cur.execute(resolve_by_week_in_month_sql, (year, month, week_in_month - 1))

            week = cur.fetchone()
            if not week:
                raise HTTPException(status_code=404, detail="No chart week found for that input")

            week_id, y, wed = week

            cur.execute(entries_sql, (week_id, top))
            entries = cur.fetchall()

        return {
            "resolved_from": (
                {"target_date": target_date}
                if target_date
                else {"year": year, "month": month, "week_in_month": week_in_month}
            ),
            "week": {"id": week_id, "year": y, "week_end_date": wed.isoformat()},
            "entries": [{"position": r[0], "artist": r[1], "song_title": r[2]} for r in entries],
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

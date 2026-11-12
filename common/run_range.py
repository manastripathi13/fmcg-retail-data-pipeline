"""Date-range arguments shared by every job: --date D, or --start S --end E (inclusive)."""

from __future__ import annotations

import argparse
from datetime import date, timedelta


def add_range_args(ap: argparse.ArgumentParser) -> None:
    ap.add_argument("--date", type=date.fromisoformat, help="a single business date")
    ap.add_argument("--start", type=date.fromisoformat, help="first business date of a range")
    ap.add_argument("--end", type=date.fromisoformat, help="last business date of a range (inclusive)")


def resolve(a: argparse.Namespace) -> tuple[date, date]:
    if a.date:
        return a.date, a.date
    if a.start and a.end and a.start <= a.end:
        return a.start, a.end
    raise SystemExit("pass --date, or --start and --end with start <= end")


def days(start: date, end: date) -> list[date]:
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def iso_weeks(start: date, end: date) -> list[tuple[int, int, date, date]]:
    """(iso_year, iso_week, week_start, last_day_to_process) for every ISO week touching [start, end].

    last_day_to_process is the week's Sunday, or `end` when the range stops mid-week (week-to-date).
    """
    out, monday = [], start - timedelta(days=start.weekday())
    while monday <= end:
        y, w, _ = monday.isocalendar()
        out.append((y, w, monday, min(monday + timedelta(days=6), end)))
        monday += timedelta(days=7)
    return out


def same_days_last_year(iso_year: int, iso_week: int, n_days: int) -> tuple[date, date]:
    """Same ISO week and weekdays one year earlier (week 53 maps to week 52 when last year had none)."""
    weeks_last_year = date(iso_year - 1, 12, 28).isocalendar()[1]
    start = date.fromisocalendar(iso_year - 1, min(iso_week, weeks_last_year), 1)
    return start, start + timedelta(days=n_days - 1)

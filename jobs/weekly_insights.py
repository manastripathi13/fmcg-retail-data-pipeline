"""PySpark job: turn one week of mart tables into a business insights report.

Reads the mart (Hive) for the ISO week containing --date and writes a Markdown report to
  docs/insights/<year>-W<week>.md            (in the repo)
  gs://<mart>/_reports/<year>-W<week>.md     (next to the data)

    python jobs/weekly_insights.py --date 2026-11-08
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.config import MART_BUCKET, MART_DB, ROOT, gcs_client, get_spark  # noqa: E402


def inr(x) -> str:
    """Indian notation: ₹ for small amounts, lakh (L) and crore (Cr) for large ones."""
    x = float(x or 0)
    if abs(x) < 1e5:
        return f"₹{x:,.0f}"
    return f"₹{x / 1e5:,.2f} L" if abs(x) < 1e7 else f"₹{x / 1e7:,.2f} Cr"


def pct(x) -> str:
    return "n/a" if x is None else f"{x:+.1f}%"


def table(rows: list[list], headers: list[str]) -> list[str]:
    out = ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, type=date.fromisoformat)
    d = ap.parse_args().date
    y, w, _ = d.isocalendar()
    spark = get_spark(f"weekly_insights_{y}_W{w:02d}")
    part = f"year = {y} and week = {w}"
    q = lambda sql: spark.sql(sql).collect()  # noqa: E731

    k = q(f"""select sum(net_sales) net, sum(prev_week_net_sales) prev, sum(baskets) baskets, sum(units) units,
                     sum(discount) disc, sum(returns_value) ret, max(days_in_week) days
              from {MART_DB}.store_weekly_performance where {part}""")[0]
    wow = 100 * (float(k.net) / float(k.prev) - 1) if k.prev else None
    stores = q(f"""select store_name, region, store_format, net_sales, avg_basket_value, sales_per_sqft, wow_growth_pct
                   from {MART_DB}.store_weekly_performance where {part} order by net_sales desc limit 5""")
    regions = q(f"""select region, sum(net_sales) net, sum(prev_week_net_sales) prev from {MART_DB}.store_weekly_performance
                    where {part} group by region order by net desc""")
    cats = q(f"""select category, sum(net_sales) net, sum(prev_week_net_sales) prev from {MART_DB}.category_brand_weekly
                 where {part} group by category order by net desc""")
    brands = q(f"""select brand, sub_category, net_sales, wow_growth_pct from {MART_DB}.category_brand_weekly
                   where {part} and prev_week_net_sales > 5000 order by wow_growth_pct desc limit 5""")
    promos = q(f"""select promo_name, brand, discount_pct, promo_units, unit_uplift_pct, discount_cost, incremental_net_sales, roi
                   from {MART_DB}.promotion_weekly_effectiveness where {part} order by roi desc nulls last""")
    tiers = q(f"""select loyalty_tier, active_customers, net_sales, avg_spend_per_customer, avg_baskets_per_customer, share_of_sales_pct
                  from {MART_DB}.customer_tier_weekly where {part} order by net_sales desc""")
    channel = q(f"""select channel, sum(net_amount) net from {MART_DB}.fact_sales where {part} group by channel order by net desc""")
    peak = q(f"""select sale_date, day_of_week, sum(net_amount) net from {MART_DB}.fact_sales where {part}
                 group by sale_date, day_of_week order by net desc limit 1""")[0]

    md = [f"# FMCG weekly insights: {y}-W{w:02d}", "",
          f"Built from the `{MART_DB}` Hive database (partition `year={y}/week={w}`), data up to {d}. Synthetic data.", "",
          "## Headline", "",
          f"- **Net sales {inr(k.net)}** over {k.days} day{'s' if k.days != 1 else ''}, {pct(wow)} vs the same days last week.",
          f"- {k.baskets:,} baskets, {k.units:,} units, average basket {inr(float(k.net) / k.baskets)}.",
          f"- Promotions cost {inr(k.disc)} in discounts; returns were {inr(k.ret)}.",
          f"- Best day: {peak.day_of_week} {peak.sale_date} with {inr(peak.net)}.",
          "- Channel mix: " + ", ".join(f"{c.channel} {100 * float(c.net) / float(k.net):.0f}%" for c in channel) + ".", "",
          "## Top 5 stores", ""]
    md += table([[s.store_name, s.region, s.store_format, inr(s.net_sales), f"₹{float(s.avg_basket_value):,.0f}",
                  f"₹{float(s.sales_per_sqft):,.1f}", pct(s.wow_growth_pct)] for s in stores],
                ["Store", "Region", "Format", "Net sales", "Avg basket", "Sales / sqft", "WoW"])
    md += ["", "## Regions", ""]
    md += table([[r.region, inr(r.net), pct(100 * (float(r.net) / float(r.prev) - 1) if r.prev else None)] for r in regions],
                ["Region", "Net sales", "WoW"])
    md += ["", "## Categories", ""]
    md += table([[c.category, inr(c.net), f"{100 * float(c.net) / float(k.net):.1f}%",
                  pct(100 * (float(c.net) / float(c.prev) - 1) if c.prev else None)] for c in cats],
                ["Category", "Net sales", "Share", "WoW"])
    md += ["", "## Fastest-growing brands (WoW)", ""]
    md += table([[b.brand, b.sub_category, inr(b.net_sales), pct(b.wow_growth_pct)] for b in brands],
                ["Brand", "Sub-category", "Net sales", "WoW"])
    md += ["", "## Promotion effectiveness", "",
           "Uplift compares units per day on promotion with the same brand's units per day without it. "
           "ROI = incremental net sales ÷ discount cost.", ""]
    md += table([[p.promo_name, p.brand, f"{p.discount_pct}%", p.promo_units, pct(p.unit_uplift_pct), inr(p.discount_cost),
                  inr(p.incremental_net_sales), "n/a" if p.roi is None else f"{p.roi:.2f}x"] for p in promos],
                ["Promotion", "Brand", "Discount", "Units", "Unit uplift", "Discount cost", "Incremental sales", "ROI"])
    md += ["", "## Customers by loyalty tier", ""]
    md += table([[t.loyalty_tier, f"{t.active_customers:,}", inr(t.net_sales),
                  "n/a" if t.avg_spend_per_customer is None else f"₹{float(t.avg_spend_per_customer):,.0f}",
                  t.avg_baskets_per_customer if t.avg_baskets_per_customer is not None else "n/a",
                  f"{t.share_of_sales_pct}%"] for t in tiers],
                ["Tier", "Active customers", "Net sales", "Spend / customer", "Baskets / customer", "Share of sales"])
    text = "\n".join(md) + "\n"

    out = ROOT / "docs" / "insights" / f"{y}-W{w:02d}.md"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(text)
    gcs_client().bucket(MART_BUCKET).blob(f"_reports/{y}-W{w:02d}.md").upload_from_string(text, content_type="text/markdown")
    print(f"[insights] wrote {out.relative_to(ROOT)} and gs://{MART_BUCKET}/_reports/{y}-W{w:02d}.md")
    print(text)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())

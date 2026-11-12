-- Sample queries over the FMCG lakehouse. Run them with:
--   ./scripts/sql_shell.sh -f docs/sample_queries.sql
-- or paste them into the interactive prompt (./scripts/sql_shell.sh).

-- What is in each layer?
show databases;
show tables in datalake;
show tables in mart;
show partitions mart.fact_sales;

-- Where does a table's data live in GCS, and how is it partitioned?
describe formatted mart.store_weekly_performance;

-- Daily line counts in the raw datalake for November 2026 (one partition per drop date)
select year, month, day, filename, count(*) as lines, round(sum(quantity * unit_price), 0) as gross_inr
from datalake.transactions
where year = 2026 and month = 11
group by year, month, day, filename
order by year, month, day;

-- Three years of weekly KPIs with week-on-week and year-on-year change
select year, week, week_start, stores_trading, net_sales, wow_growth_pct, yoy_growth_pct
from mart.company_weekly_kpis
order by year, week;

-- QA: company-level weeks that breached WoW ±20% or YoY ±40%
select check_name, iso_year, iso_week, round(observed, 1) as change_pct
from qa.mart_checks
where level = 'company' and status = 'FAIL'
order by iso_year, iso_week;

-- Week-on-week net sales by region (Diwali week = 2026-W45)
select week, region, round(sum(net_sales)) as net_sales_inr, round(sum(prev_week_net_sales)) as prev_week_inr
from mart.store_weekly_performance
where year = 2026
group by week, region
order by week, net_sales_inr desc;

-- Top 10 stores in the Diwali week with basket size and sales density
select store_name, region, store_format, net_sales, avg_basket_value, sales_per_sqft, wow_growth_pct, rank_in_region
from mart.store_weekly_performance
where year = 2026 and week = 45
order by net_sales desc
limit 10;

-- Category share and growth
select category, round(sum(net_sales)) as net_sales_inr,
       round(100 * sum(net_sales) / sum(sum(net_sales)) over (), 1) as share_pct,
       round(100 * (sum(net_sales) / sum(prev_week_net_sales) - 1), 1) as wow_pct
from mart.category_brand_weekly
where year = 2026 and week = 45
group by category
order by net_sales_inr desc;

-- Did the promotions pay for themselves?
select week, promo_name, brand, discount_pct, promo_units, unit_uplift_pct, discount_cost, incremental_net_sales, roi
from mart.promotion_weekly_effectiveness
where year = 2026
order by week, roi desc;

-- Loyalty members vs walk-ins
select week, loyalty_tier, active_customers, net_sales, avg_spend_per_customer, share_of_sales_pct
from mart.customer_tier_weekly
where year = 2026
order by week, net_sales desc;

-- Sales by hour of day across both weeks (line-level fact table)
select hour(sale_ts) as hour, round(sum(net_amount)) as net_sales_inr, count(distinct transaction_id) as baskets
from mart.fact_sales
group by hour(sale_ts)
order by hour;

-- Payment mix by channel
select channel, payment_mode, round(sum(net_amount)) as net_sales_inr
from mart.fact_sales
group by channel, payment_mode
order by channel, net_sales_inr desc;

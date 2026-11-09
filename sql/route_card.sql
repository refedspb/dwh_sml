-- Эталонный расчёт витрины «Карточка маршрута и коридор рыночных ставок».
-- Grain: месяц (по price_dates.price_date) x город отправления x город назначения x группа кузова x весовая категория.
-- Параметр: as_of (дата снимка; не системная дата).
-- Полная перестройка в одной транзакции: читатели BI видят либо старую, либо новую версию (DELETE, не TRUNCATE).
-- ВАЖНО: в этом файле нельзя использовать символ процента вне плейсхолдеров.

DELETE FROM mart.mart_route_card;

INSERT INTO mart.mart_route_card (
    price_month, loading_city_gcid, unloading_city_gcid, body_group, weight_category,
    loading_city, unloading_city,
    trips_count, carriers_count,
    p25_trip, median_trip, p75_trip, avg_trip,
    p25_km, median_km, p75_km,
    cv_rate, mom_pct, yoy_pct, mom_km_pct, yoy_km_pct,
    last_price_date, reliability_level, as_of)
WITH price_months AS (
    -- Одна цена - один раз в месяц; плановые даты после as_of в историческую статистику не попадают.
    SELECT d.price_id,
           date_trunc('month', d.price_date)::date AS price_month,
           max(d.price_date)                       AS last_price_date
    FROM core.fact_price_date d
    WHERE d.price_date <= %(as_of)s::date
    GROUP BY d.price_id, date_trunc('month', d.price_date)
),
base AS (
    SELECT pm.price_month, pm.last_price_date,
           f.loading_city_gcid, f.unloading_city_gcid,
           g.body_group, f.weight_category,
           f.price_id, f.price_rub, f.distance, f.price_type, f.organization_id
    FROM core.fact_price f
    JOIN price_months pm ON pm.price_id = f.price_id
    JOIN core.fact_price_body_group g ON g.price_id = f.price_id
    WHERE f.is_fact
      AND f.price_type IN (1, 2, 3, 4, 5, 9)
      AND NOT f.complex
      AND NOT f.outlier_mark
      AND f.price_rub > 0
      AND f.distance > 0
),
agg AS (
    SELECT price_month, loading_city_gcid, unloading_city_gcid, body_group, weight_category,
           count(DISTINCT price_id)::int AS trips_count,
           (count(DISTINCT organization_id) FILTER (WHERE price_type IN (3, 9)))::int AS carriers_count,
           percentile_cont(ARRAY[0.25, 0.5, 0.75]::float8[]) WITHIN GROUP (ORDER BY price_rub)             AS q_trip,
           percentile_cont(ARRAY[0.25, 0.5, 0.75]::float8[]) WITHIN GROUP (ORDER BY price_rub / distance)  AS q_km,
           avg(price_rub)         AS avg_trip,
           stddev_samp(price_rub) AS sd_trip,
           max(last_price_date)   AS last_price_date
    FROM base
    GROUP BY price_month, loading_city_gcid, unloading_city_gcid, body_group, weight_category
),
cells AS (
    SELECT price_month, loading_city_gcid, unloading_city_gcid, body_group, weight_category,
           trips_count, carriers_count,
           q_trip[1] AS p25_trip, q_trip[2] AS median_trip, q_trip[3] AS p75_trip, avg_trip,
           q_km[1]   AS p25_km,   q_km[2]   AS median_km,   q_km[3]   AS p75_km,
           -- коэффициент вариации: NULL при менее чем двух наблюдениях или нулевом среднем
           CASE WHEN trips_count >= 2 AND avg_trip <> 0 THEN sd_trip / avg_trip END AS cv_rate,
           last_price_date
    FROM agg
)
SELECT c.price_month, c.loading_city_gcid, c.unloading_city_gcid, c.body_group, c.weight_category,
       lc.city_name, uc.city_name,
       c.trips_count, c.carriers_count,
       c.p25_trip, c.median_trip, c.p75_trip, c.avg_trip,
       c.p25_km, c.median_km, c.p75_km,
       c.cv_rate,
       -- MoM / YoY: только календарно соседняя ячейка; нет ячейки или нулевой знаменатель - NULL
       CASE WHEN pmn.median_trip IS NULL OR pmn.median_trip = 0 THEN NULL
            ELSE (c.median_trip / pmn.median_trip - 1) * 100 END,
       CASE WHEN pyr.median_trip IS NULL OR pyr.median_trip = 0 THEN NULL
            ELSE (c.median_trip / pyr.median_trip - 1) * 100 END,
       CASE WHEN pmn.median_km IS NULL OR pmn.median_km = 0 THEN NULL
            ELSE (c.median_km / pmn.median_km - 1) * 100 END,
       CASE WHEN pyr.median_km IS NULL OR pyr.median_km = 0 THEN NULL
            ELSE (c.median_km / pyr.median_km - 1) * 100 END,
       c.last_price_date,
       CASE WHEN c.trips_count >= 30
                 AND c.carriers_count >= 5
                 AND (%(as_of)s::date - c.last_price_date) <= 30 THEN 'HIGH'
            WHEN c.trips_count >= 10 THEN 'MEDIUM'
            ELSE 'LOW_DATA'
       END,
       %(as_of)s::date
FROM cells c
JOIN core.dim_city lc ON lc.gc_id = c.loading_city_gcid
JOIN core.dim_city uc ON uc.gc_id = c.unloading_city_gcid
LEFT JOIN cells pmn
       ON pmn.loading_city_gcid   = c.loading_city_gcid
      AND pmn.unloading_city_gcid = c.unloading_city_gcid
      AND pmn.body_group          = c.body_group
      AND pmn.weight_category     = c.weight_category
      AND pmn.price_month         = (c.price_month - INTERVAL '1 month')::date
LEFT JOIN cells pyr
       ON pyr.loading_city_gcid   = c.loading_city_gcid
      AND pyr.unloading_city_gcid = c.unloading_city_gcid
      AND pyr.body_group          = c.body_group
      AND pyr.weight_category     = c.weight_category
      AND pyr.price_month         = (c.price_month - INTERVAL '1 year')::date;

ANALYZE mart.mart_route_card;

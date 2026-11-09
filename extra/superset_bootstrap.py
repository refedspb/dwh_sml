"""Extra: дашборд Superset «как код». Выполняется ВНУТРИ контейнера Superset (`python /app/bootstrap.py`).

Создаёт подключение к PostgreSQL, датасеты поверх витрин, графики и дашборд с фильтрами маршрута.
Идемпотентно: подключение и датасеты переиспользуются, графики и дашборд пересоздаются из кода.

Конструкторы параметров (chart_specs, dashboard_layout, route_filters) не импортируют Superset, поэтому
проверяются обычными тестами; всё, что требует Superset, находится внутри main().
"""
from __future__ import annotations

import hashlib
import json
import os
from typing import Any
from urllib.parse import quote

DATABASE_NAME = "Smart Logistics DWH"
DASHBOARD_TITLE = "Тарифный мониторинг: рынок и точность прогноза"
DASHBOARD_SLUG = "market-rates"
PRIMARY_MODEL = "catboost"
DEFAULT_ROUTE = {"loading_city": "Москва", "unloading_city": "Казань", "body_group": "Тент", "weight_category": "FTL"}

# (схема, таблица): физические таблицы и представления витрин, без виртуальных датасетов
DATASETS = [
    ("mart", "v_route_latest"),
    ("mart", "mart_route_card"),
    ("mart", "ml_price_predictions"),
    ("mart", "ml_model_metrics"),
    ("core", "dq_quarantine_prices"),
]


# ----------------------------------------------------------------------------- конструкторы параметров
def sql_metric(expression: str, label: str) -> dict[str, Any]:
    return {"expressionType": "SQL", "sqlExpression": expression, "label": label, "optionName": "metric_" + hashlib.md5(f"{expression}|{label}".encode()).hexdigest()[:10]}


def where(expression: str) -> dict[str, Any]:
    return {"expressionType": "SQL", "clause": "WHERE", "sqlExpression": expression}


def table_chart(columns: list[str], order_by: str, filters: list[dict] | None = None, limit: int = 500) -> dict[str, Any]:
    return {"viz_type": "table", "query_mode": "raw", "all_columns": columns, "adhoc_filters": filters or [],
            "order_by_cols": [json.dumps([order_by, False])], "row_limit": limit, "include_search": True,
            "server_page_length": 20}


def big_number(metric: dict, filters: list[dict], number_format: str = "SMART_NUMBER") -> dict[str, Any]:
    return {"viz_type": "big_number_total", "metric": metric, "adhoc_filters": filters, "y_axis_format": number_format,
            "header_font_size": 0.4, "subheader_font_size": 0.15, "subheader": metric["label"]}


def line_chart(x: str, grain: str, metrics: list[dict], filters: list[dict] | None = None, fmt: str = ",.0f") -> dict[str, Any]:
    return {"viz_type": "echarts_timeseries_line", "x_axis": x, "time_grain_sqla": grain, "metrics": metrics,
            "groupby": [], "adhoc_filters": filters or [], "time_range": "No filter", "row_limit": 10000,
            "show_legend": True, "rich_tooltip": True, "y_axis_format": fmt, "markerEnabled": True}


def chart_specs() -> list[dict[str, Any]]:
    """Графики: ключ, название, датасет, параметры, ширина/высота в сетке дашборда и номер ряда."""
    model = where(f"model_name = '{PRIMARY_MODEL}'")
    return [
        dict(key="kpi_mape", title="Средняя относительная ошибка прогноза (MAPE), %", dataset="ml_model_metrics", row=1, w=3, h=20,
             params=big_number(sql_metric("MAX(mape_pct)", "MAPE на отложенном периоде"), [model], ".2f")),
        dict(key="kpi_r2", title="Какую долю разброса цен объясняет модель (R²)", dataset="ml_model_metrics", row=1, w=3, h=20,
             params=big_number(sql_metric("MAX(r2)", "R² на отложенном периоде"), [model], ".3f")),
        dict(key="kpi_high", title="Ячеек сети с высокой надёжностью данных", dataset="v_route_latest", row=1, w=3, h=20,
             params=big_number(sql_metric("COUNT(*)", "уровень HIGH"), [where("reliability_level = 'HIGH'")])),
        dict(key="kpi_dq", title="Строк источника, не прошедших проверки качества", dataset="dq_quarantine_prices", row=1, w=3, h=20,
             params=big_number(sql_metric("COUNT(DISTINCT price_id)", "отправлено в карантин"), [])),

        dict(key="network", title="Все направления: актуальный срез по каждой ячейке", dataset="v_route_latest", row=2, w=8, h=60,
             params=table_chart(["loading_city", "unloading_city", "body_group", "weight_category", "trips_count",
                                 "carriers_count", "p25_trip", "median_trip", "p75_trip", "median_km", "mom_pct",
                                 "yoy_pct", "reliability_level"], "trips_count")),
        dict(key="reliability", title="Структура сети по надёжности данных", dataset="v_route_latest", row=2, w=4, h=60,
             params={"viz_type": "pie", "groupby": ["reliability_level"], "metric": sql_metric("COUNT(*)", "Ячейки сети"),
                     "adhoc_filters": [], "row_limit": 100, "donut": True, "show_labels": True,
                     "label_type": "key_percent", "show_legend": True}),

        dict(key="corridor_trip", title="Рыночный диапазон цены рейса, ₽", dataset="mart_route_card",
             row=3, w=6, h=50, route=True,
             params=line_chart("price_month", "P1M", [sql_metric("MAX(p25_trip)", "Нижний квартиль (P25)"),
                                                      sql_metric("MAX(median_trip)", "Медиана"),
                                                      sql_metric("MAX(p75_trip)", "Верхний квартиль (P75)")])),
        dict(key="corridor_km", title="Рыночный диапазон цены за километр, ₽/км", dataset="mart_route_card",
             row=3, w=6, h=50, route=True,
             params=line_chart("price_month", "P1M", [sql_metric("MAX(p25_km)", "Нижний квартиль (P25)"),
                                                      sql_metric("MAX(median_km)", "Медиана"),
                                                      sql_metric("MAX(p75_km)", "Верхний квартиль (P75)")], fmt=",.1f")),
        dict(key="route_history", title="Выбранный маршрут по месяцам: цены и надёжность выборки", dataset="mart_route_card",
             row=4, w=12, h=50, route=True,
             params=table_chart(["price_month", "trips_count", "carriers_count", "p25_trip", "median_trip", "p75_trip",
                                 "median_km", "cv_rate", "mom_pct", "yoy_pct", "last_price_date",
                                 "reliability_level"], "price_month", limit=200)),

        dict(key="fact_vs_forecast", title="Фактическая и прогнозная цена рейса по дням погрузки, ₽", dataset="ml_price_predictions",
             row=5, w=8, h=50,
             params=line_chart("loading_date", "P1D", [sql_metric("AVG(actual_price_rub)", "Фактическая цена"),
                                                       sql_metric("AVG(predicted_price_rub)", "Прогноз модели")], [model])),
        dict(key="dq_reasons", title="Почему строки не прошли проверки качества", dataset="dq_quarantine_prices", row=5, w=4, h=50,
             params={"viz_type": "echarts_timeseries_bar", "x_axis": "reason_code", "metrics": [sql_metric("COUNT(*)", "Количество строк")],
                     "groupby": [], "adhoc_filters": [], "time_range": "No filter", "row_limit": 100,
                     "orientation": "vertical", "show_value": True, "show_legend": False}),

        dict(key="overpriced", title="Отложенный период: рейсы, где факт превысил прогноз не менее чем на 30 процентов", dataset="ml_price_predictions",
             row=6, w=12, h=50,
             params=table_chart(["loading_date", "loading_city_gcid", "unloading_city_gcid", "body_group", "actual_price_rub",
                                 "predicted_price_rub", "ape_pct", "is_fact"], "ape_pct",
                                [model, where("split = 'test'"), where("actual_price_rub > predicted_price_rub * 1.3")], 200)),
    ]


def route_filters(dataset_id: int, route_chart_ids: list[int], all_chart_ids: list[int]) -> list[dict[str, Any]]:
    """Нативные фильтры дашборда: город отправления, назначения, кузов, вес; действуют только на карточку маршрута."""
    scope = {"rootPath": ["ROOT_ID"], "excluded": [i for i in all_chart_ids if i not in route_chart_ids]}
    labels = {"loading_city": "Пункт погрузки", "unloading_city": "Пункт выгрузки", "body_group": "Тип кузова", "weight_category": "Весовой класс"}
    out = []
    for column, title in labels.items():
        default = DEFAULT_ROUTE[column]
        out.append({
            "id": f"NATIVE_FILTER-{column}", "name": title, "type": "NATIVE_FILTER", "filterType": "filter_select",
            "description": "", "cascadeParentIds": [], "tabsInScope": [],
            "targets": [{"datasetId": dataset_id, "column": {"name": column}}],
            "controlValues": {"multiSelect": False, "enableEmptyFilter": True, "defaultToFirstItem": False,
                              "searchAllOptions": True, "inverseSelection": False},
            "defaultDataMask": {"extraFormData": {"filters": [{"col": column, "op": "IN", "val": [default]}]},
                                "filterState": {"value": [default], "label": default}, "ownState": {}},
            "scope": scope, "chartsInScope": route_chart_ids,
        })
    return out


INTRO = (
    "## Тарифы на автоперевозки: что происходит на рынке и насколько точен прогноз\n"
    "Все цены в рублях без НДС, по состоянию на дату снимка. **Сверху** показатели модели и качества данных, "
    "ниже обзор всех направлений. **В середине** разбор одного маршрута: слева задайте пункты погрузки и выгрузки, "
    "тип кузова и весовой класс. **Внизу** сравнение фактических цен с прогнозом и список причин, "
    "по которым строки источника были отброшены.\n\n"
    "Как читать диапазон цен: нижний квартиль (P25) означает, что четверть рейсов прошла дешевле, медиана делит рынок пополам, "
    "верхний квартиль (P75) отсекает самую дорогую четверть. Надёжность выборки: `HIGH` при 30 и более рейсах, "
    "5 и более перевозчиках и данных не старше месяца; `MEDIUM` от 10 рейсов; `LOW_DATA` в остальных случаях."
)


def dashboard_layout(chart_ids: dict[str, int], specs: list[dict[str, Any]]) -> dict[str, Any]:
    """Раскладка дашборда: markdown-вступление и ряды графиков по полю row."""
    pos: dict[str, Any] = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
        "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID", "meta": {"text": DASHBOARD_TITLE}},
    }

    def add_row(row_id: str, cells: list[dict[str, Any]]) -> None:
        pos["GRID_ID"]["children"].append(row_id)
        pos[row_id] = {"type": "ROW", "id": row_id, "children": [c["id"] for c in cells],
                       "parents": ["ROOT_ID", "GRID_ID"], "meta": {"background": "BACKGROUND_TRANSPARENT"}}
        for cell in cells:
            cell["parents"] = ["ROOT_ID", "GRID_ID", row_id]
            pos[cell["id"]] = cell

    add_row("ROW-intro", [{"type": "MARKDOWN", "id": "MARKDOWN-intro", "children": [], "meta": {"width": 12, "height": 22, "code": INTRO}}])
    rows: dict[int, list[dict[str, Any]]] = {}
    for s in specs:
        rows.setdefault(s["row"], []).append({
            "type": "CHART", "id": f"CHART-{s['key']}", "children": [],
            "meta": {"width": s["w"], "height": s["h"], "chartId": chart_ids[s["key"]], "sliceName": s["title"]}})
    for n, cells in sorted(rows.items()):
        add_row(f"ROW-{n}", cells)
    return pos


# ----------------------------------------------------------------------------- Superset (только внутри контейнера)
def sqlalchemy_uri() -> str:
    env = os.environ
    return "postgresql+psycopg2://{u}:{p}@{h}:{port}/{d}".format(
        u=quote(env.get("PGUSER", "postgres"), safe=""), p=quote(env.get("PGPASSWORD", "postgres"), safe=""),
        h=env.get("PGHOST", "postgres"), port=env.get("PGPORT", "5432"), d=env.get("PGDATABASE", "deals_bigdata"))


def main() -> None:
    from superset import db, security_manager
    from superset.connectors.sqla.models import SqlaTable
    from superset.models.core import Database
    from superset.models.dashboard import Dashboard
    from superset.models.slice import Slice

    admin = security_manager.find_user(username=os.environ.get("SUPERSET_ADMIN_USER", "admin"))

    database = db.session.query(Database).filter_by(database_name=DATABASE_NAME).one_or_none()
    if database is None:
        database = Database(database_name=DATABASE_NAME)
        db.session.add(database)
    database.set_sqlalchemy_uri(sqlalchemy_uri())
    database.expose_in_sqllab = True
    db.session.flush()

    datasets: dict[str, Any] = {}
    for schema, table in DATASETS:
        ds = db.session.query(SqlaTable).filter_by(database_id=database.id, schema=schema, table_name=table).one_or_none()
        if ds is None:
            ds = SqlaTable(database=database, schema=schema, table_name=table)
            db.session.add(ds)
        ds.owners = [admin]
        db.session.flush()
        ds.fetch_metadata()
        datasets[table] = ds

    specs = chart_specs()
    old = db.session.query(Dashboard).filter_by(slug=DASHBOARD_SLUG).one_or_none()
    if old is not None:
        for stale in list(old.slices):          
            db.session.delete(stale)
        db.session.delete(old)
    for stale in db.session.query(Slice).filter(Slice.slice_name.in_([s["title"] for s in specs])).all():
        db.session.delete(stale)
    db.session.flush()

    slices: dict[str, Any] = {}
    for s in specs:
        ds = datasets[s["dataset"]]
        params = {**s["params"], "datasource": f"{ds.id}__table", "slice_name": s["title"]}
        slices[s["key"]] = Slice(slice_name=s["title"], viz_type=s["params"]["viz_type"], datasource_type="table",
                                 datasource_id=ds.id, params=json.dumps(params, ensure_ascii=False), owners=[admin])
        db.session.add(slices[s["key"]])
    db.session.flush()

    ids = {key: sl.id for key, sl in slices.items()}
    route_ids = [ids[s["key"]] for s in specs if s.get("route")]
    metadata = {
        "native_filter_configuration": route_filters(datasets["mart_route_card"].id, route_ids, list(ids.values())),
        "color_scheme": "supersetColors", "refresh_frequency": 0, "cross_filters_enabled": False,
        "default_filters": "{}", "chart_configuration": {},
    }
    db.session.add(Dashboard(
        dashboard_title=DASHBOARD_TITLE, slug=DASHBOARD_SLUG, published=True, owners=[admin],
        slices=list(slices.values()),
        position_json=json.dumps(dashboard_layout(ids, specs), ensure_ascii=False),
        json_metadata=json.dumps(metadata, ensure_ascii=False)))
    db.session.commit()
    print(f"[superset-bootstrap] dashboard '{DASHBOARD_TITLE}' (/superset/dashboard/{DASHBOARD_SLUG}/): "
          f"{len(slices)} charts, {len(datasets)} datasets")


if __name__ == "__main__":
    from superset.app import create_app

    with create_app().app_context():
        main()

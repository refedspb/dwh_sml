"""Тесты extra-компонентов: перенос в ClickHouse (на поддельном HTTP-сервере), конструкторы дашборда Superset и DAG Airflow.

Сами контейнеры не нужны: проверяется протокол обмена, структура параметров и граф задач.
"""
import ast
import json
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest

from extra.clickhouse_sync import (TABLES, ClickHouseError, ClickHouseHTTP, ClickHouseSettings, read_ddl,
                                   sync_table)
from extra.superset_bootstrap import (DASHBOARD_TITLE, DATASETS, chart_specs, dashboard_layout, route_filters,
                                      sqlalchemy_uri)

# ----------------------------------------------------------------------------- ClickHouse
class FakeClickHouse:
    """Запоминает все запросы; на count() отвечает заданным числом."""

    def __init__(self, count_answer: int, fail_on: str | None = None):
        self.requests: list[tuple[dict, str]] = []
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body = self.rfile.read(int(self.headers.get("Content-Length", 0))).decode("utf-8")
                params = dict(urllib.parse.parse_qsl(urllib.parse.urlparse(self.path).query))
                outer.requests.append((params, body, dict(self.headers)))
                if fail_on and fail_on in (params.get("query", "") + body):
                    self.send_response(500); self.end_headers(); self.wfile.write(b"boom"); return
                answer = f"{count_answer}\n" if "count()" in body else ""
                self.send_response(200); self.end_headers(); self.wfile.write(answer.encode())

            def log_message(self, *a):
                pass

        self.server = HTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    @property
    def url(self):
        return f"http://127.0.0.1:{self.server.server_port}"

    def close(self):
        self.server.shutdown(); self.server.server_close()

    def sql(self):
        return [(p.get("query") or b) for p, b, _ in self.requests]


def _client(fake):
    return ClickHouseHTTP(ClickHouseSettings(fake.url, "analytics", "secret", "analytics"))


def test_sync_swaps_tables_atomically_after_count_check():
    fake = FakeClickHouse(count_answer=3)
    try:
        n = sync_table(_client(fake), "analytics", TABLES[0], b"a,b\n", expected_rows=3)
    finally:
        fake.close()
    assert n == 3
    sql = fake.sql()
    insert = next(i for i, q in enumerate(sql) if q.startswith("INSERT INTO analytics.mart_route_card__new FORMAT CSV"))
    swap = next(i for i, q in enumerate(sql) if q.startswith("EXCHANGE TABLES"))
    assert insert < swap, "подмена только после загрузки"
    assert "analytics.mart_route_card__new AND analytics.mart_route_card" in sql[swap]
    assert sql[-1].startswith("DROP TABLE IF EXISTS analytics.mart_route_card__new")
    assert fake.requests[0][2].get("X-ClickHouse-User") == "analytics"


def test_row_count_mismatch_aborts_swap_and_cleans_up():
    fake = FakeClickHouse(count_answer=2)
    try:
        with pytest.raises(ClickHouseError):
            sync_table(_client(fake), "analytics", TABLES[0], b"x\n", expected_rows=3)
    finally:
        fake.close()
    sql = fake.sql()
    assert not any(q.startswith("EXCHANGE TABLES") for q in sql)
    assert sql[-1].startswith("DROP TABLE IF EXISTS")


def test_http_error_is_raised():
    fake = FakeClickHouse(count_answer=0, fail_on="CREATE DATABASE")
    try:
        with pytest.raises(ClickHouseError):
            _client(fake).command("CREATE DATABASE x")
    finally:
        fake.close()


def test_unreachable_clickhouse_gives_clear_error():
    client = ClickHouseHTTP(ClickHouseSettings("http://127.0.0.1:1", "u", "p", "analytics"))      # порт 1: отказ в соединении
    with pytest.raises(ClickHouseError, match="недоступен"):
        client.command("SELECT 1")


def test_ddl_files_render_and_match_pg_select_width():
    for spec in TABLES:
        ddl = read_ddl(spec, f"analytics.{spec.name}")
        assert f"analytics.{spec.name}" in ddl and "{" not in ddl
        cols_in_ddl = [l.split()[0] for l in ddl.splitlines()
                       if l.startswith("    ") and l.split()[0][0].isalpha()]
        # число колонок SELECT в Postgres = число колонок DDL (порядок критичен для CSV)
        select_list = spec.pg_select.split("FROM")[0].replace("SELECT", "")
        depth, n = 0, 1
        for ch in select_list:
            depth += ch == "("; depth -= ch == ")"
            n += ch == "," and depth == 0
        assert n == len(cols_in_ddl), spec.name


# ----------------------------------------------------------------------------- Superset (конструкторы без самого Superset)
ALLOWED_VIZ = {"table", "big_number_total", "pie", "echarts_timeseries_line", "echarts_timeseries_bar"}


def _ids():
    specs = chart_specs()
    return specs, {s["key"]: i + 100 for i, s in enumerate(specs)}


def test_chart_specs_are_unique_valid_and_serializable():
    specs, _ = _ids()
    assert len({s["key"] for s in specs}) == len(specs) == len({s["title"] for s in specs})
    known_tables = {t for _, t in DATASETS}
    for s in specs:
        assert s["dataset"] in known_tables, s["key"]
        assert s["params"]["viz_type"] in ALLOWED_VIZ, s["key"]
        json.dumps(s["params"], ensure_ascii=False)





def test_layout_places_every_chart_once_with_valid_widths():
    specs, ids = _ids()
    layout = dashboard_layout(ids, specs)
    placed = [v["meta"]["chartId"] for v in layout.values() if isinstance(v, dict) and v.get("type") == "CHART"]
    assert sorted(placed) == sorted(ids.values())
    for rid in layout["GRID_ID"]["children"]:
        width = sum(layout[c]["meta"]["width"] for c in layout[rid]["children"])
        assert width <= 12, rid
        for c in layout[rid]["children"]:
            assert layout[c]["parents"][-1] == rid


def test_route_filters_scope_only_route_charts():
    specs, ids = _ids()
    route_ids = [ids[s["key"]] for s in specs if s.get("route")]
    filters = route_filters(5, route_ids, list(ids.values()))
    assert [f["targets"][0]["column"]["name"] for f in filters] == ["loading_city", "unloading_city", "body_group", "weight_category"]
    for f in filters:
        assert set(f["chartsInScope"]) == set(route_ids)
        assert not set(f["scope"]["excluded"]) & set(route_ids)
        assert set(f["scope"]["excluded"]) | set(route_ids) == set(ids.values())
    assert len(route_ids) == 3


def test_sqlalchemy_uri_quotes_credentials():
    import os

    old = dict(os.environ)
    os.environ.update(PGUSER="u@x", PGPASSWORD="p:/w", PGHOST="h", PGPORT="5432", PGDATABASE="d")
    try:
        assert sqlalchemy_uri() == "postgresql+psycopg2://u%40x:p%3A%2Fw@h:5432/d"
    finally:
        os.environ.clear(); os.environ.update(old)


# ----------------------------------------------------------------------------- Airflow DAG
DAG_FILE = Path(__file__).resolve().parent.parent / "extra" / "airflow_dag.py"
EXPECTED_ORDER = ["init_ddl", "load_core", "refresh_marts", "score_ml", "sync_clickhouse"]


def test_dag_is_valid_python_with_expected_tasks_and_chain():
    src = DAG_FILE.read_text(encoding="utf-8")
    ast.parse(src)
    assert 'dag_id="smart_logistics_pipeline"' in src and "max_active_runs=1" in src
    for t in EXPECTED_ORDER:
        assert f'"{t}"' in src or f"{t} =" in src
    assert " >> ".join(EXPECTED_ORDER) in src
    assert "-m extra.clickhouse_sync" in src


def test_dag_params_are_restricted_to_date_format():
    src = DAG_FILE.read_text(encoding="utf-8")
    assert src.count("pattern=DATE_PATTERN") == 3      # параметры уходят в bash, поэтому валидируются


def test_dag_loads_in_airflow_dagbag():
    pytest.importorskip("airflow")
    from airflow.models import DagBag

    bag = DagBag(dag_folder=str(DAG_FILE.parent), include_examples=False)
    assert not bag.import_errors
    assert [t.task_id for t in bag.dags["smart_logistics_pipeline"].topological_sort()] == EXPECTED_ORDER

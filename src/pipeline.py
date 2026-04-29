"""Version 1 batch pipeline for search attribution reporting."""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable
from urllib.parse import parse_qs, urlparse


SESSION_TIMEOUT_SECONDS = 30 * 60
SEARCH_ENGINES = {
    "google": "q",
    "bing": "q",
    "yahoo": "p",
}


@dataclass(frozen=True)
class Hit:
    hit_time_gmt: int
    date_time: str
    user_agent: str
    ip: str
    event_list: str
    product_list: str
    referrer: str


@dataclass(frozen=True)
class SearchTouch:
    domain: str
    engine: str
    keyword: str


@dataclass
class Session:
    session_id: int
    ip: str
    user_agent: str
    hits: list[Hit]
    attribution: SearchTouch | None = None


def read_hits(input_path: str | Path) -> list[Hit]:
    """Read TSV hit data into normalized Hit objects."""
    with Path(input_path).open(newline="", encoding="utf-8") as input_file:
        reader = csv.DictReader(input_file, delimiter="\t")
        return [parse_hit(row) for row in reader]


def parse_hit(row: dict[str, str]) -> Hit:
    return Hit(
        hit_time_gmt=int(row["hit_time_gmt"]),
        date_time=row.get("date_time", ""),
        user_agent=row.get("user_agent", ""),
        ip=row.get("ip", ""),
        event_list=row.get("event_list", ""),
        product_list=row.get("product_list", ""),
        referrer=row.get("referrer", ""),
    )


def extract_search_touch(referrer: str) -> SearchTouch | None:
    """Return search attribution details for supported external search referrers."""
    if not referrer:
        return None

    parsed = urlparse(referrer)
    host = (parsed.hostname or "").lower()
    for engine, query_param in SEARCH_ENGINES.items():
        if not _is_search_host(host, engine):
            continue

        values = parse_qs(parsed.query).get(query_param, [])
        if not values:
            return None

        keyword = values[0].strip().lower()
        if not keyword:
            return None

        return SearchTouch(domain=_normalize_search_domain(host), engine=engine, keyword=keyword)

    return None


def _is_search_host(host: str, engine: str) -> bool:
    return engine in host.split(".")


def _normalize_search_domain(host: str) -> str:
    if host.startswith("www."):
        return host[4:]
    return host


def is_purchase(event_list: str) -> bool:
    return "1" in {event.strip() for event in event_list.split(",") if event.strip()}


def parse_revenue(product_list: str) -> Decimal:
    """Sum the price field from each product_list item."""
    total = Decimal("0")
    if not product_list:
        return total

    for product in product_list.split(","):
        fields = product.split(";")
        if len(fields) < 4:
            continue

        price = fields[3].strip()
        if not price:
            continue

        try:
            total += Decimal(price)
        except InvalidOperation:
            continue

    return total


def build_sessions(hits: Iterable[Hit]) -> list[Session]:
    """Group hits into sessions by ip + user_agent with a 30-minute timeout."""
    sessions: list[Session] = []
    active_by_visitor: dict[tuple[str, str], Session] = {}
    last_seen_by_visitor: dict[tuple[str, str], int] = {}

    for hit in sorted(hits, key=lambda item: item.hit_time_gmt):
        visitor_key = (hit.ip, hit.user_agent)
        last_seen = last_seen_by_visitor.get(visitor_key)
        session = active_by_visitor.get(visitor_key)

        if session is None or last_seen is None or hit.hit_time_gmt - last_seen > SESSION_TIMEOUT_SECONDS:
            session = Session(
                session_id=len(sessions) + 1,
                ip=hit.ip,
                user_agent=hit.user_agent,
                hits=[],
            )
            sessions.append(session)
            active_by_visitor[visitor_key] = session

        session.hits.append(hit)
        if session.attribution is None:
            session.attribution = extract_search_touch(hit.referrer)

        last_seen_by_visitor[visitor_key] = hit.hit_time_gmt

    return sessions


def summarize_sessions(sessions: Iterable[Session]) -> dict[str, object]:
    engine_summary = defaultdict(_empty_metric)
    keyword_summary = defaultdict(_empty_metric)
    deliverable_summary = defaultdict(Decimal)
    top_keywords = defaultdict(_empty_metric)

    session_count = 0
    attributed_sessions = 0
    purchase_count = 0
    total_revenue = Decimal("0")

    for session in sessions:
        session_count += 1
        purchase_hits = [hit for hit in session.hits if is_purchase(hit.event_list)]
        session_revenue = sum((parse_revenue(hit.product_list) for hit in purchase_hits), Decimal("0"))
        session_purchases = len(purchase_hits)

        purchase_count += session_purchases
        total_revenue += session_revenue

        if session.attribution is None:
            continue

        attributed_sessions += 1
        touch = session.attribution
        _add_metrics(engine_summary[touch.engine], 1, session_purchases, session_revenue)
        _add_metrics(keyword_summary[(touch.engine, touch.keyword)], 1, session_purchases, session_revenue)
        deliverable_summary[(touch.domain, touch.keyword)] += session_revenue
        _add_metrics(top_keywords[touch.keyword], 1, session_purchases, session_revenue)

    return {
        "engine_summary": engine_summary,
        "keyword_summary": keyword_summary,
        "deliverable_summary": deliverable_summary,
        "top_keywords": top_keywords,
        "run_summary": {
            "sessions": session_count,
            "attributed_sessions": attributed_sessions,
            "purchases": purchase_count,
            "revenue": _format_decimal(total_revenue),
        },
    }


def run_pipeline(input_path: str | Path = "data.txt", output_dir: str | Path = "outputs") -> dict[str, object]:
    hits = read_hits(input_path)
    sessions = build_sessions(hits)
    summaries = summarize_sessions(sessions)
    write_outputs(summaries, output_dir)
    return summaries


def write_outputs(summaries: dict[str, object], output_dir: str | Path) -> None:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    _write_engine_summary(summaries["engine_summary"], output_path / "search_engine_summary.csv")
    _write_keyword_summary(summaries["keyword_summary"], output_path / "search_keyword_summary.csv")
    _write_search_keyword_performance(summaries["deliverable_summary"], output_path / _deliverable_filename())
    _write_top_keywords(summaries["top_keywords"], output_path / "top_keywords.csv")

    with (output_path / "run_summary.json").open("w", encoding="utf-8") as summary_file:
        json.dump(summaries["run_summary"], summary_file, indent=2)
        summary_file.write("\n")


def _empty_metric() -> dict[str, object]:
    return {"visits": 0, "purchases": 0, "revenue": Decimal("0")}


def _add_metrics(metric: dict[str, object], visits: int, purchases: int, revenue: Decimal) -> None:
    metric["visits"] += visits
    metric["purchases"] += purchases
    metric["revenue"] += revenue


def _write_engine_summary(summary: dict[str, dict[str, object]], output_file: Path) -> None:
    rows = [
        {
            "search_engine": engine,
            "visits": metrics["visits"],
            "purchases": metrics["purchases"],
            "revenue": _format_decimal(metrics["revenue"]),
        }
        for engine, metrics in sorted(summary.items())
    ]
    _write_csv(output_file, ["search_engine", "visits", "purchases", "revenue"], rows)


def _write_keyword_summary(summary: dict[tuple[str, str], dict[str, object]], output_file: Path) -> None:
    rows = [
        {
            "search_engine": engine,
            "keyword": keyword,
            "visits": metrics["visits"],
            "purchases": metrics["purchases"],
            "revenue": _format_decimal(metrics["revenue"]),
        }
        for (engine, keyword), metrics in sorted(summary.items())
    ]
    _write_csv(output_file, ["search_engine", "keyword", "visits", "purchases", "revenue"], rows)


def _write_search_keyword_performance(summary: dict[tuple[str, str], Decimal], output_file: Path) -> None:
    rows = [
        {
            "Search Engine Domain": domain,
            "Search Keyword": keyword,
            "Revenue": _format_decimal(revenue),
        }
        for (domain, keyword), revenue in sorted(
            summary.items(),
            key=lambda item: (-item[1], item[0][0], item[0][1]),
        )
    ]
    _write_delimited(
        output_file,
        ["Search Engine Domain", "Search Keyword", "Revenue"],
        rows,
        delimiter="\t",
    )


def _write_top_keywords(summary: dict[str, dict[str, object]], output_file: Path) -> None:
    rows = [
        {
            "keyword": keyword,
            "visits": metrics["visits"],
            "purchases": metrics["purchases"],
            "revenue": _format_decimal(metrics["revenue"]),
        }
        for keyword, metrics in sorted(
            summary.items(),
            key=lambda item: (-item[1]["revenue"], -item[1]["purchases"], item[0]),
        )
    ]
    _write_csv(output_file, ["keyword", "visits", "purchases", "revenue"], rows)


def _write_csv(output_file: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    _write_delimited(output_file, fieldnames, rows, delimiter=",")


def _write_delimited(
    output_file: Path,
    fieldnames: list[str],
    rows: list[dict[str, object]],
    delimiter: str,
) -> None:
    with output_file.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames, delimiter=delimiter)
        writer.writeheader()
        writer.writerows(rows)


def _deliverable_filename() -> str:
    return f"{datetime.now(timezone.utc):%Y-%m-%d}_SearchKeywordPerformance.tab"


def _format_decimal(value: Decimal) -> str:
    return f"{value.quantize(Decimal('0.01'))}"


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the Adobe assessment batch pipeline.")
    parser.add_argument("--input", default="data.txt", help="Path to the input TSV file.")
    parser.add_argument("--output-dir", default="outputs", help="Directory for generated reports.")
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    summaries = run_pipeline(args.input, args.output_dir)
    print(json.dumps(summaries["run_summary"], indent=2))

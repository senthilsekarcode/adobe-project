import csv
import json
import shutil
import unittest
from pathlib import Path

from src.pipeline import (
    Hit,
    build_sessions,
    extract_search_touch,
    is_purchase,
    parse_revenue,
    run_pipeline,
)


class PipelineTest(unittest.TestCase):
    def test_extract_search_touch_for_supported_engines(self):
        self.assertEqual(
            extract_search_touch("http://www.google.com/search?q=Ipod&client=firefox"),
            ("google", "ipod"),
        )
        self.assertEqual(
            extract_search_touch("http://www.bing.com/search?q=Zune&form=QBLH"),
            ("bing", "zune"),
        )
        self.assertEqual(
            extract_search_touch("http://search.yahoo.com/search?p=cd+player&ei=UTF-8"),
            ("yahoo", "cd player"),
        )
        self.assertIsNone(extract_search_touch("http://notgoogle.com/search?q=Ipod"))
        self.assertIsNone(extract_search_touch("http://www.esshopzilla.com/search/?k=Ipod"))

    def test_purchase_and_revenue_parsing(self):
        self.assertTrue(is_purchase("2,1"))
        self.assertFalse(is_purchase("10,2"))
        self.assertEqual(parse_revenue("Electronics;Ipod;1;190;,Electronics;Case;1;20;"), 210)

    def test_sessions_use_ip_user_agent_timeout_and_first_search_touch(self):
        hits = [
            _hit(1000, "1.1.1.1", "ua", "", "http://www.google.com/search?q=first"),
            _hit(1100, "1.1.1.1", "ua", "", "http://www.bing.com/search?q=second"),
            _hit(3001, "1.1.1.1", "ua", "", "http://search.yahoo.com/search?p=third"),
        ]

        sessions = build_sessions(hits)

        self.assertEqual(len(sessions), 2)
        self.assertEqual(sessions[0].attribution, ("google", "first"))
        self.assertEqual(sessions[1].attribution, ("yahoo", "third"))

    def test_run_pipeline_writes_expected_outputs(self):
        temp_path = Path(__file__).resolve().parent / ".tmp" / "pipeline"
        shutil.rmtree(temp_path, ignore_errors=True)
        temp_path.mkdir(parents=True)
        self.addCleanup(shutil.rmtree, temp_path.parent, True)

        input_file = temp_path / "data.txt"
        output_dir = temp_path / "outputs"
        _write_sample_input(input_file)

        run_pipeline(input_file, output_dir)

        engine_rows = _read_csv(output_dir / "search_engine_summary.csv")
        keyword_rows = _read_csv(output_dir / "search_keyword_summary.csv")
        top_rows = _read_csv(output_dir / "top_keywords.csv")
        run_summary = json.loads((output_dir / "run_summary.json").read_text(encoding="utf-8"))

        self.assertEqual(
            engine_rows,
            [
                {"search_engine": "bing", "visits": "1", "purchases": "1", "revenue": "250.00"},
                {"search_engine": "google", "visits": "1", "purchases": "1", "revenue": "290.00"},
            ],
        )
        self.assertEqual(
            keyword_rows,
            [
                {
                    "search_engine": "bing",
                    "keyword": "zune",
                    "visits": "1",
                    "purchases": "1",
                    "revenue": "250.00",
                },
                {
                    "search_engine": "google",
                    "keyword": "ipod",
                    "visits": "1",
                    "purchases": "1",
                    "revenue": "290.00",
                },
            ],
        )
        self.assertEqual(top_rows[0]["keyword"], "ipod")
        self.assertEqual(run_summary, {"sessions": 2, "attributed_sessions": 2, "purchases": 2, "revenue": "540.00"})


def _hit(time, ip, user_agent, event_list, referrer, product_list=""):
    return Hit(
        hit_time_gmt=time,
        date_time="",
        user_agent=user_agent,
        ip=ip,
        event_list=event_list,
        product_list=product_list,
        referrer=referrer,
    )


def _write_sample_input(path):
    rows = [
        {
            "hit_time_gmt": "1000",
            "date_time": "2009-09-27 00:16:40",
            "user_agent": "ua1",
            "ip": "1.1.1.1",
            "event_list": "",
            "geo_city": "",
            "geo_region": "",
            "geo_country": "",
            "pagename": "Home",
            "page_url": "http://example.test",
            "product_list": "",
            "referrer": "http://www.google.com/search?q=Ipod",
        },
        {
            "hit_time_gmt": "1100",
            "date_time": "2009-09-27 00:18:20",
            "user_agent": "ua1",
            "ip": "1.1.1.1",
            "event_list": "1",
            "geo_city": "",
            "geo_region": "",
            "geo_country": "",
            "pagename": "Order Complete",
            "page_url": "http://example.test/complete",
            "product_list": "Electronics;Ipod;1;290;",
            "referrer": "http://example.test/checkout",
        },
        {
            "hit_time_gmt": "2000",
            "date_time": "2009-09-27 00:33:20",
            "user_agent": "ua2",
            "ip": "2.2.2.2",
            "event_list": "",
            "geo_city": "",
            "geo_region": "",
            "geo_country": "",
            "pagename": "Home",
            "page_url": "http://example.test",
            "product_list": "",
            "referrer": "http://www.bing.com/search?q=Zune",
        },
        {
            "hit_time_gmt": "2100",
            "date_time": "2009-09-27 00:35:00",
            "user_agent": "ua2",
            "ip": "2.2.2.2",
            "event_list": "2,1",
            "geo_city": "",
            "geo_region": "",
            "geo_country": "",
            "pagename": "Order Complete",
            "page_url": "http://example.test/complete",
            "product_list": "Electronics;Zune;1;250;",
            "referrer": "http://example.test/checkout",
        },
    ]
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0]), delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)


def _read_csv(path):
    with path.open(newline="", encoding="utf-8") as file:
        return list(csv.DictReader(file))


if __name__ == "__main__":
    unittest.main()

"""
Автономные модульные тесты для чистых (не требующих сети) функций app.py.

Покрывают требования из мастер-промпта: разбор URL, вычисление периода,
парсинг дат отзывов, обработку Unicode/спецсимволов в CSV, отсутствие
удаления дубликатов, сортировку DESC, обработку пагинации/дыр в фиде,
частичный отказ регионов и rate-limit/retry.

Сетевые вызовы (requests.get) везде замоканы — тесты не обращаются
к реальному Apple API.
"""

from __future__ import annotations

import io
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pandas as pd

import app


class TestAppIdParsing(unittest.TestCase):
    def test_plain_numeric_id(self):
        self.assertTrue(app.validate_app_id("123456789"))
        self.assertEqual(app.get_app_id("123456789"), "123456789")

    def test_id_with_prefix_and_spaces(self):
        self.assertEqual(app.get_app_id("  id123456789 "), "123456789")
        self.assertEqual(app.get_app_id("ID123456789"), "123456789")

    def test_invalid_values(self):
        for bad in ["hello", "", "   ", None, "12ab34", "123", "-123456789",
                    "https://google.com", "12345678901234567890"]:
            self.assertFalse(app.validate_app_id(bad), bad)

    def test_url_is_no_longer_accepted(self):
        self.assertFalse(app.validate_app_id("https://apps.apple.com/us/app/example/id123456789"))

    def test_get_app_id_raises_on_invalid(self):
        with self.assertRaises(ValueError):
            app.get_app_id("hello")


class TestDateHandling(unittest.TestCase):
    def test_subtract_two_months_regular(self):
        dt = datetime(2026, 9, 24, tzinfo=timezone.utc)
        result = app.subtract_months(dt, 2)
        self.assertEqual(result, datetime(2026, 7, 24, tzinfo=timezone.utc))

    def test_subtract_months_year_rollover(self):
        dt = datetime(2026, 1, 15, tzinfo=timezone.utc)
        result = app.subtract_months(dt, 2)
        self.assertEqual(result, datetime(2025, 11, 15, tzinfo=timezone.utc))

    def test_subtract_months_day_clamping(self):
        # 31 марта - 2 месяца = должно попасть в январь (31 день, ок)
        dt = datetime(2026, 3, 31, tzinfo=timezone.utc)
        result = app.subtract_months(dt, 2)
        self.assertEqual(result, datetime(2026, 1, 31, tzinfo=timezone.utc))

    def test_parse_review_date_with_offset(self):
        parsed = app.parse_review_date("2026-08-15T13:45:12-07:00")
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.tzinfo, timezone.utc)

    def test_parse_review_date_with_z(self):
        parsed = app.parse_review_date("2026-08-15T13:45:12Z")
        self.assertIsNotNone(parsed)

    def test_parse_review_date_invalid(self):
        self.assertIsNone(app.parse_review_date("not-a-date"))
        self.assertIsNone(app.parse_review_date(""))
        self.assertIsNone(app.parse_review_date(None))

    def test_period_is_one_month(self):
        self.assertEqual(app.PERIOD_MONTHS, 1)

    def test_filter_reviews_by_date(self):
        # Test 14: отзывы старше периода отфильтровываются
        now = datetime(2026, 9, 24, tzinfo=timezone.utc)
        cutoff = app.subtract_months(now, app.PERIOD_MONTHS)
        recent = {"publication_date": datetime(2026, 9, 10, tzinfo=timezone.utc)}
        old = {"publication_date": datetime(2026, 1, 1, tzinfo=timezone.utc)}
        future = {"publication_date": datetime(2026, 10, 1, tzinfo=timezone.utc)}
        result = app.filter_reviews_by_date([recent, old, future], cutoff, now)
        self.assertEqual(result, [recent])


class TestReviewParsing(unittest.TestCase):
    def _entry(self, rating="5", title="Great", content="Very good app!", updated="2026-08-01T10:00:00Z"):
        return {
            "id": {"label": "111"},
            "title": {"label": title},
            "content": {"label": content},
            "im:rating": {"label": rating},
            "updated": {"label": updated},
        }

    def test_parse_review_basic(self):
        review = app.parse_review(self._entry(), "us")
        self.assertIsNotNone(review)
        self.assertEqual(review["rating"], 5)
        self.assertEqual(review["country"], "us")
        self.assertEqual(review["title"], "Great")

    def test_parse_review_rejects_bad_rating(self):
        self.assertIsNone(app.parse_review(self._entry(rating="7"), "us"))
        self.assertIsNone(app.parse_review(self._entry(rating="abc"), "us"))

    def test_parse_review_rejects_bad_date(self):
        self.assertIsNone(app.parse_review(self._entry(updated="garbage"), "us"))

    def test_extract_review_entries_skips_app_info(self):
        # первая запись фида - метаинформация без im:rating - должна отсеиваться
        data = {
            "feed": {
                "entry": [
                    {"im:name": {"label": "My App"}},  # app info, без im:rating
                    self._entry(),
                ]
            }
        }
        entries = app._extract_review_entries(data)
        self.assertEqual(len(entries), 1)

    def test_extract_review_entries_single_entry_as_object(self):
        # когда отзыв всего один, feed.entry - объект, а не список
        data = {"feed": {"entry": self._entry()}}
        entries = app._extract_review_entries(data)
        self.assertEqual(len(entries), 1)

    def test_extract_review_entries_empty_feed(self):
        # Test 6: регион без отзывов
        data = {"feed": {}}
        self.assertEqual(app._extract_review_entries(data), [])
        self.assertEqual(app._extract_review_entries({}), [])

    def test_unicode_text_preserved(self):
        # Test 9: Unicode (русский, японский, эмодзи)
        review = app.parse_review(self._entry(content="Очень хорошее приложение! 👍 とても良い"), "jp")
        self.assertIn("Очень хорошее", review["review_text"])
        self.assertIn("とても良い", review["review_text"])
        self.assertIn("👍", review["review_text"])


class TestDuplicatesAndSorting(unittest.TestCase):
    def test_duplicates_not_removed(self):
        # Дубликаты - намеренное требование: не удалять
        reviews = [
            {"review_text": "Same text", "rating": 5, "publication_date": datetime(2026, 8, 1, tzinfo=timezone.utc), "title": "T", "country": "us", "language": "en"},
            {"review_text": "Same text", "rating": 5, "publication_date": datetime(2026, 8, 1, tzinfo=timezone.utc), "title": "T", "country": "us", "language": "en"},
        ]
        df = app.build_dataframe(reviews)
        self.assertEqual(len(df), 2)

    def test_sort_desc(self):
        # Test: сортировка DESC (новые сверху)
        reviews = [
            {"review_text": "old", "rating": 3, "publication_date": datetime(2026, 1, 1, tzinfo=timezone.utc), "title": "", "country": "us", "language": "en"},
            {"review_text": "new", "rating": 4, "publication_date": datetime(2026, 8, 1, tzinfo=timezone.utc), "title": "", "country": "us", "language": "en"},
            {"review_text": "mid", "rating": 2, "publication_date": datetime(2026, 5, 1, tzinfo=timezone.utc), "title": "", "country": "us", "language": "en"},
        ]
        df = app.sort_reviews(app.build_dataframe(reviews))
        self.assertEqual(list(df["review_text"]), ["new", "mid", "old"])

    def test_empty_dataframe_has_required_columns(self):
        df = app.build_dataframe([])
        for col in app.REQUIRED_COLUMNS:
            self.assertIn(col, df.columns)


class TestCsvExport(unittest.TestCase):
    def test_csv_handles_commas_quotes_newlines_unicode(self):
        # Test 10, 11, 12: запятая, кавычки, перенос строки; плюс юникод
        reviews = [
            {
                "review_text": 'Text, with comma and "quotes"\nand newline',
                "rating": 5,
                "publication_date": datetime(2026, 8, 1, tzinfo=timezone.utc),
                "title": "Заголовок, с запятой",
                "country": "ru",
                "language": "ru",
            },
            {
                "review_text": "とても良いアプリです。素晴らしい！ 😊",
                "rating": 4,
                "publication_date": datetime(2026, 8, 2, tzinfo=timezone.utc),
                "title": "良い",
                "country": "jp",
                "language": "ja",
            },
        ]
        df = app.sort_reviews(app.build_dataframe(reviews))
        filename, csv_bytes = app.export_csv(df, "123456789")

        self.assertEqual(filename, "appstore_reviews_123456789.csv")
        # UTF-8 BOM для корректного открытия в Excel
        self.assertTrue(csv_bytes.startswith(b"\xef\xbb\xbf"))

        decoded = csv_bytes.decode("utf-8-sig")
        parsed_back = pd.read_csv(io.StringIO(decoded))
        self.assertEqual(len(parsed_back), 2)
        all_text = " ".join(parsed_back["review_text"].tolist())
        self.assertIn("とても良いアプリです", all_text)
        self.assertIn("素晴らしい", all_text)
        # текст с запятой/кавычками/переносом строки должен восстановиться как есть
        self.assertIn("with comma and", all_text)
        self.assertIn("quotes", all_text)

    def test_validate_dataframe_flags_bad_rating(self):
        now = datetime(2026, 9, 24, tzinfo=timezone.utc)
        cutoff = app.subtract_months(now, 2)
        df = pd.DataFrame([{
            "review_text": "x", "rating": 9, "publication_date": now,
            "title": "t", "country": "us", "language": "en",
        }])
        problems = app.validate_dataframe(df, cutoff, now)
        self.assertIn("rating_out_of_range", problems)


class TestNetworkResilience(unittest.TestCase):
    def _make_response(self, status_code=200, json_data=None, headers=None):
        resp = MagicMock()
        resp.status_code = status_code
        resp.headers = headers or {}
        if json_data is not None:
            resp.json.return_value = json_data
        else:
            resp.json.side_effect = ValueError("no json")
        return resp

    def test_timeout_then_retry_then_success(self):
        # Test 7: временная HTTP-ошибка с последующим успехом (retry)
        import requests as requests_module
        session = MagicMock()
        good_entry = {
            "id": {"label": "1"}, "title": {"label": "T"},
            "content": {"label": "C"}, "im:rating": {"label": "5"},
            "updated": {"label": "2026-08-01T00:00:00Z"},
        }
        session.get.side_effect = [
            requests_module.exceptions.Timeout(),
            self._make_response(200, {"feed": {"entry": [good_entry]}}),
        ]
        with patch("time.sleep", return_value=None):
            entries, error = app._fetch_page_with_retry("http://fake", session)
        self.assertIsNone(error)
        self.assertEqual(len(entries), 1)

    def test_exhausted_retries_returns_error(self):
        import requests as requests_module
        session = MagicMock()
        session.get.side_effect = requests_module.exceptions.ConnectionError()
        with patch("time.sleep", return_value=None):
            entries, error = app._fetch_page_with_retry("http://fake", session)
        self.assertIsNone(entries)
        self.assertEqual(error, "connection_error")

    def test_rate_limit_respects_retry_after(self):
        # Test 8: 429 rate limit
        session = MagicMock()
        good_entry = {
            "id": {"label": "1"}, "title": {"label": "T"},
            "content": {"label": "C"}, "im:rating": {"label": "5"},
            "updated": {"label": "2026-08-01T00:00:00Z"},
        }
        session.get.side_effect = [
            self._make_response(429, headers={"Retry-After": "2"}),
            self._make_response(200, {"feed": {"entry": [good_entry]}}),
        ]
        sleep_calls = []
        with patch("time.sleep", side_effect=lambda s: sleep_calls.append(s)):
            entries, error = app._fetch_page_with_retry("http://fake", session)
        self.assertIsNone(error)
        self.assertEqual(len(entries), 1)
        self.assertIn(2.0, sleep_calls)

    def test_multiple_pages_pagination(self):
        # Test 13: несколько страниц отзывов
        def make_entry(i):
            return {
                "id": {"label": str(i)}, "title": {"label": f"T{i}"},
                "content": {"label": f"C{i}"}, "im:rating": {"label": "5"},
                "updated": {"label": "2026-08-01T00:00:00Z"},
            }

        page1 = self._make_response(200, {"feed": {"entry": [make_entry(i) for i in range(50)]}})
        page2 = self._make_response(200, {"feed": {"entry": [make_entry(i) for i in range(50, 60)]}})
        empty_pages = [self._make_response(200, {"feed": {}}) for _ in range(8)]

        session = MagicMock()
        session.get.side_effect = [page1, page2] + empty_pages
        with patch("time.sleep", return_value=None):
            raw_entries, error = app.fetch_reviews_for_storefront("123", "us", session)
        self.assertIsNone(error)
        self.assertEqual(len(raw_entries), 60)

    def test_holes_in_pagination_are_scanned_through(self):
        # известная особенность фида: пустая страница может быть за
        # непустой - не должны останавливаться на первой пустой странице
        def make_entry(i):
            return {
                "id": {"label": str(i)}, "title": {"label": f"T{i}"},
                "content": {"label": f"C{i}"}, "im:rating": {"label": "5"},
                "updated": {"label": "2026-08-01T00:00:00Z"},
            }

        empty_resp = self._make_response(200, {"feed": {}})
        full_resp = self._make_response(200, {"feed": {"entry": [make_entry(i) for i in range(5)]}})

        session = MagicMock()
        # страница 1 пустая, страница 2 полная, остальные пустые
        session.get.side_effect = [empty_resp, full_resp] + [empty_resp] * 8
        with patch("time.sleep", return_value=None):
            raw_entries, error = app.fetch_reviews_for_storefront("123", "us", session)
        self.assertIsNone(error)
        self.assertEqual(len(raw_entries), 5)

    def test_partial_region_failure_does_not_lose_others(self):
        # Test 15: частичная ошибка регионов - остальные данные не теряются
        import requests as requests_module

        def fake_fetch(app_id, country, session, stop_event=None):
            if country == "jp":
                return [], "http_500"
            recent = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()
            entry = {
                "id": {"label": country}, "title": {"label": "T"},
                "content": {"label": f"Review from {country}"}, "im:rating": {"label": "5"},
                "updated": {"label": recent},
            }
            return [entry], None

        with patch.object(app, "fetch_reviews_for_storefront", side_effect=fake_fetch), \
             patch.object(app, "get_available_storefronts", return_value=["us", "jp", "de"]):
            reviews, failed = app.fetch_all_reviews("123456789")

        countries_with_reviews = {r["country"] for r in reviews}
        self.assertEqual(countries_with_reviews, {"us", "de"})
        self.assertEqual(len(failed), 1)
        self.assertEqual(failed[0]["region"], "jp")

    def test_app_not_found(self):
        # Test 5: несуществующий App ID
        session_resp = self._make_response(200, {"resultCount": 0, "results": []})
        with patch("app.requests.get", return_value=session_resp):
            self.assertFalse(app.check_app_exists("000000000"))

    def test_app_found(self):
        session_resp = self._make_response(200, {"resultCount": 1, "results": [{}]})
        with patch("app.requests.get", return_value=session_resp):
            self.assertTrue(app.check_app_exists("123456789"))

    def test_lookup_network_failure_fails_open(self):
        with patch("app.requests.get", side_effect=Exception("network down")):
            self.assertTrue(app.check_app_exists("123456789"))


class TestCountrySelection(unittest.TestCase):
    def test_empty_selection_means_all(self):
        all_codes = app.get_available_storefronts()
        self.assertEqual(app.normalize_countries(None), all_codes)
        self.assertEqual(app.normalize_countries([]), all_codes)

    def test_selection_is_normalized(self):
        self.assertEqual(app.normalize_countries(["US", " de ", "us", "xx"]), ["us", "de"])

    def test_only_selected_countries_are_requested(self):
        requested = []

        def fake_fetch(app_id, country, session, stop_event=None):
            requested.append(country)
            return [], None

        with patch.object(app, "fetch_reviews_for_storefront", side_effect=fake_fetch):
            reviews, failed = app.fetch_all_reviews("123456789", countries=["us", "ru"])

        self.assertEqual(sorted(requested), ["ru", "us"])
        self.assertEqual(reviews, [])
        self.assertEqual(failed, [])

    def test_progress_total_matches_selection(self):
        totals = []

        def fake_fetch(app_id, country, session, stop_event=None):
            return [], None

        def on_progress(processed, total, country, reviews_so_far, failed_so_far):
            totals.append(total)

        with patch.object(app, "fetch_reviews_for_storefront", side_effect=fake_fetch):
            app.fetch_all_reviews("123456789", countries=["us", "de", "jp"], progress_callback=on_progress)
        self.assertEqual(set(totals), {3})


class TestStopButton(unittest.TestCase):
    def _recent_entry(self, country):
        recent = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
        return {
            "id": {"label": country}, "title": {"label": "T"},
            "content": {"label": f"Review from {country}"}, "im:rating": {"label": "5"},
            "updated": {"label": recent},
        }

    def test_page_fetch_returns_stopped_when_event_set(self):
        import threading
        ev = threading.Event()
        ev.set()
        session = MagicMock()
        entries, error = app._fetch_page_with_retry("http://fake", session, ev)
        self.assertIsNone(entries)
        self.assertEqual(error, app.STOPPED)
        session.get.assert_not_called()

    def test_storefront_keeps_pages_loaded_before_stop(self):
        import threading
        ev = threading.Event()
        calls = {"n": 0}

        def fake_page(url, session, stop_event=None):
            calls["n"] += 1
            if calls["n"] == 2:
                ev.set()  # пользователь нажал "Стоп" после первой страницы
            if stop_event is not None and stop_event.is_set():
                return None, app.STOPPED
            return [self._recent_entry("us")], None

        with patch.object(app, "_fetch_page_with_retry", side_effect=fake_page):
            entries, error = app.fetch_reviews_for_storefront("123456789", "us", MagicMock(), ev)
        self.assertIsNone(error)
        self.assertEqual(len(entries), 1)  # первая страница сохранена

    def test_stop_before_start_fetches_nothing(self):
        import threading
        ev = threading.Event()
        ev.set()
        fake = MagicMock(return_value=([], None))
        with patch.object(app, "fetch_reviews_for_storefront", fake):
            reviews, failed = app.fetch_all_reviews("123456789", countries=["us", "de"], stop_event=ev)
        self.assertEqual((reviews, failed), ([], []))
        fake.assert_not_called()

    def test_stop_keeps_already_loaded_regions(self):
        import threading
        ev = threading.Event()
        progress = []

        def fake_fetch(app_id, country, session, stop_event=None):
            ev.set()  # "Стоп" нажат во время обработки первого региона
            return [self._recent_entry(country)], None

        with patch.object(app, "MAX_WORKERS", 1), \
             patch.object(app, "fetch_reviews_for_storefront", side_effect=fake_fetch):
            reviews, failed = app.fetch_all_reviews(
                "123456789", countries=["us", "de", "jp"], stop_event=ev,
                progress_callback=lambda *a: progress.append(a),
            )
        self.assertEqual({r["country"] for r in reviews}, {"us"})
        self.assertEqual(failed, [])
        self.assertEqual(len(progress), 1)  # остальные регионы не запускались

    def test_job_stop_returns_partial_results(self):
        import threading
        first_started = threading.Event()

        def fake_fetch(app_id, country, session, stop_event=None):
            first_started.set()
            stop_event.wait(5)  # висим, пока не нажмут "Стоп"
            return [self._recent_entry(country)], None

        with patch.object(app, "MAX_WORKERS", 1), \
             patch.object(app, "fetch_reviews_for_storefront", side_effect=fake_fetch):
            job = app.FetchJob("123456789", ["us", "de", "jp"])
            job.start()
            self.assertTrue(first_started.wait(5))  # первый регион уже в работе
            job.request_stop()
            self.assertTrue(job.done_event.wait(5))
        self.assertTrue(job.was_stopped)
        self.assertFalse(job.crashed)
        self.assertEqual(len(job.reviews), 1)

    def test_job_completes_without_stop(self):
        def fake_fetch(app_id, country, session, stop_event=None):
            return [self._recent_entry(country)], None

        with patch.object(app, "fetch_reviews_for_storefront", side_effect=fake_fetch):
            job = app.FetchJob("123456789", ["us", "de"])
            job.start()
            self.assertTrue(job.done_event.wait(5))
        self.assertFalse(job.was_stopped)
        self.assertEqual(len(job.reviews), 2)
        self.assertEqual(job.snapshot()["processed"], 2)

    def test_job_crash_is_reported_not_raised(self):
        with patch.object(app, "fetch_all_reviews", side_effect=RuntimeError("boom")):
            job = app.FetchJob("123456789", ["us"])
            job.start()
            self.assertTrue(job.done_event.wait(5))
        self.assertTrue(job.crashed)


class TestUiSmoke(unittest.TestCase):
    def test_main_renders_without_errors(self):
        app.main()


class TestLanguageDetectionFallback(unittest.TestCase):
    def test_short_text_is_unknown(self):
        self.assertEqual(app.detect_language(""), "unknown")
        self.assertEqual(app.detect_language("ok"), "unknown")

    def test_no_library_available_is_unknown(self):
        with patch.object(app, "LANGDETECT_AVAILABLE", False):
            self.assertEqual(app.detect_language("This is a perfectly normal English sentence."), "unknown")


if __name__ == "__main__":
    unittest.main(verbosity=2)

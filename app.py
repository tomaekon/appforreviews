"""
App Store Reviews Downloader
=============================

Внутренний инструмент для быстрой выгрузки отзывов пользователей из Apple
App Store по всем доступным регионам за последний месяц или 2 месяца
(на выбор; отсчёт от текущего момента).
На вход подаётся только числовой ID приложения.

Источник данных: публичный JSON RSS-фид Apple "customer reviews"
(https://itunes.apple.com/{country}/rss/customerreviews/page={page}/id={app_id}/sortby=mostrecent/json).
Это официальный публичный endpoint Apple, не требующий авторизации,
API-ключей или аккаунта App Store Connect. Он используется самим сайтом
apps.apple.com и множеством open-source инструментов для чтения отзывов.

ВАЖНОЕ ОГРАНИЧЕНИЕ ИСТОЧНИКА ДАННЫХ (см. README.md, раздел "Ограничения Apple"):
Apple отдаёт не более ~10 страниц по 50 отзывов (то есть не более примерно
500 самых новых отзывов) на каждую пару "приложение + регион". Если за
выбранный период в конкретном регионе было оставлено больше отзывов, чем
покрывает это ограничение, самые старые из них в это окно физически не
попадут — это ограничение самого источника данных, а не этого кода.
Приложение честно сообщает об этом и никогда не утверждает, что получены
"все существующие отзывы" — только "все доступные через этот источник".
"""

from __future__ import annotations

import calendar
import io
import logging
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
import streamlit as st

try:
    from langdetect import DetectorFactory, LangDetectException, detect

    DetectorFactory.seed = 0  # детерминированные результаты
    LANGDETECT_AVAILABLE = True
except ImportError:  # pragma: no cover
    LANGDETECT_AVAILABLE = False


# ---------------------------------------------------------------------------
# Логирование
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("appstore_reviews")


# ---------------------------------------------------------------------------
# Константы
# ---------------------------------------------------------------------------

MAX_RETRIES = 3
BACKOFF_BASE_SECONDS = 1.0
REQUEST_TIMEOUT_SECONDS = 15
MAX_PAGES_PER_STOREFRONT = 10  # техническое ограничение Apple
MAX_WORKERS = 8
PERIOD_MONTHS = 1  # период по умолчанию (в месяцах)
PERIOD_OPTIONS = (1, 2)  # доступные в интерфейсе периоды (в месяцах)

# Контрольная проверка источника: у очень популярного приложения всегда есть
# свежие отзывы. Используется ТОЛЬКО для диагностики, когда по вашему
# приложению найдено 0 отзывов (чтобы отличить "отзывов нет" от
# "фид Apple не работает"). На результат загрузки не влияет.
HEALTHCHECK_APP_ID = "310633997"  # WhatsApp Messenger
HEALTHCHECK_COUNTRIES = ("us", "gb")

REQUIRED_COLUMNS = [
    "review_text",
    "rating",
    "publication_date",
    "title",
    "country",
    "language",
]

EXPORT_COLUMNS = [
    "title",
    "review_text",
    "rating",
    "publication_date",
    "country",
    "language",
]

APP_ID_PATTERN = re.compile(r"^(?:id)?(\d{5,12})$", re.IGNORECASE)


# Известный список storefront-ов (регионов) Apple App Store.
# Источник: официальный публичный список стран/регионов App Store Connect
# и apps.apple.com. Список статичен и меняется редко, поэтому хранится
# в коде, а не запрашивается динамически (отдельного публичного
# no-auth API для получения этого списка именно под задачу отзывов Apple
# не предоставляет). Если Apple добавит/уберёт регион, список нужно
# обновить вручную — см. README.md.
APP_STORE_COUNTRIES: Dict[str, str] = {
    "al": "Albania", "dz": "Algeria", "ao": "Angola", "ai": "Anguilla",
    "ag": "Antigua and Barbuda", "ar": "Argentina", "am": "Armenia",
    "au": "Australia", "at": "Austria", "az": "Azerbaijan",
    "bs": "Bahamas", "bh": "Bahrain", "bd": "Bangladesh", "bb": "Barbados",
    "by": "Belarus", "be": "Belgium", "bz": "Belize", "bj": "Benin",
    "bm": "Bermuda", "bt": "Bhutan", "bo": "Bolivia",
    "ba": "Bosnia and Herzegovina", "bw": "Botswana", "br": "Brazil",
    "vg": "British Virgin Islands", "bn": "Brunei", "bg": "Bulgaria",
    "bf": "Burkina Faso", "kh": "Cambodia", "cm": "Cameroon", "ca": "Canada",
    "cv": "Cape Verde", "ky": "Cayman Islands", "td": "Chad", "cl": "Chile",
    "cn": "China", "co": "Colombia", "cr": "Costa Rica",
    "ci": "Cote d'Ivoire", "hr": "Croatia", "cy": "Cyprus", "cz": "Czech Republic",
    "dk": "Denmark", "dm": "Dominica", "do": "Dominican Republic",
    "ec": "Ecuador", "eg": "Egypt", "sv": "El Salvador", "ee": "Estonia",
    "sz": "Eswatini", "fj": "Fiji", "fi": "Finland", "fr": "France",
    "ga": "Gabon", "gm": "Gambia", "ge": "Georgia", "de": "Germany",
    "gh": "Ghana", "gr": "Greece", "gd": "Grenada", "gt": "Guatemala",
    "gw": "Guinea-Bissau", "gy": "Guyana", "hn": "Honduras",
    "hk": "Hong Kong", "hu": "Hungary", "is": "Iceland", "in": "India",
    "id": "Indonesia", "iq": "Iraq", "ie": "Ireland", "il": "Israel",
    "it": "Italy", "jm": "Jamaica", "jp": "Japan", "jo": "Jordan",
    "kz": "Kazakhstan", "ke": "Kenya", "kr": "South Korea",
    "xk": "Kosovo", "kw": "Kuwait", "kg": "Kyrgyzstan", "la": "Laos",
    "lv": "Latvia", "lb": "Lebanon", "lr": "Liberia", "ly": "Libya",
    "lt": "Lithuania", "lu": "Luxembourg", "mo": "Macau",
    "mg": "Madagascar", "mw": "Malawi", "my": "Malaysia",
    "mv": "Maldives", "ml": "Mali", "mt": "Malta", "mr": "Mauritania",
    "mu": "Mauritius", "mx": "Mexico", "fm": "Micronesia",
    "md": "Moldova", "mn": "Mongolia", "me": "Montenegro",
    "ms": "Montserrat", "ma": "Morocco", "mz": "Mozambique",
    "mm": "Myanmar", "na": "Namibia", "np": "Nepal",
    "nl": "Netherlands", "nz": "New Zealand", "ni": "Nicaragua",
    "ne": "Niger", "ng": "Nigeria", "mk": "North Macedonia", "no": "Norway",
    "om": "Oman", "pk": "Pakistan", "pw": "Palau", "pa": "Panama",
    "pg": "Papua New Guinea", "py": "Paraguay", "pe": "Peru",
    "ph": "Philippines", "pl": "Poland", "pt": "Portugal", "qa": "Qatar",
    "cg": "Republic of the Congo", "ro": "Romania", "ru": "Russia", "rw": "Rwanda",
    "kn": "Saint Kitts and Nevis", "lc": "Saint Lucia",
    "vc": "Saint Vincent and the Grenadines", "sa": "Saudi Arabia",
    "sn": "Senegal", "rs": "Serbia", "sc": "Seychelles",
    "sl": "Sierra Leone", "sg": "Singapore", "sk": "Slovakia",
    "si": "Slovenia", "sb": "Solomon Islands", "za": "South Africa",
    "es": "Spain", "lk": "Sri Lanka", "sr": "Suriname", "se": "Sweden",
    "ch": "Switzerland", "tw": "Taiwan", "tj": "Tajikistan",
    "tz": "Tanzania", "th": "Thailand", "tt": "Trinidad and Tobago",
    "tn": "Tunisia", "tm": "Turkmenistan", "tc": "Turks and Caicos",
    "tr": "Turkey", "tv": "Tuvalu", "ug": "Uganda", "ua": "Ukraine",
    "ae": "United Arab Emirates", "gb": "United Kingdom",
    "us": "United States", "uy": "Uruguay", "uz": "Uzbekistan",
    "vu": "Vanuatu", "ve": "Venezuela", "vn": "Vietnam", "ye": "Yemen",
    "zm": "Zambia", "zw": "Zimbabwe",
}


# ---------------------------------------------------------------------------
# Разбор и валидация App ID
# ---------------------------------------------------------------------------

def validate_app_id(value: str) -> bool:
    """Проверяет, что строка — числовой App ID (допускается префикс "id")."""
    if not value or not isinstance(value, str):
        return False
    return APP_ID_PATTERN.match(value.strip()) is not None


def get_app_id(value: str) -> str:
    """Возвращает нормализованный App ID (только цифры) или бросает ValueError."""
    if not validate_app_id(value):
        raise ValueError("invalid_app_id")
    match = APP_ID_PATTERN.match(value.strip())
    return match.group(1)


def check_app_exists(app_id: str) -> bool:
    """
    Проверяет существование приложения через публичный iTunes Lookup API
    (https://itunes.apple.com/lookup?id=...) — тоже официальный, публичный,
    без авторизации.

    При сетевых сбоях самой проверки решаем "в пользу пользователя"
    (fail-open) — не блокируем работу из-за флакующего запроса, а не
    из-за того, что приложение действительно не найдено.
    """
    try:
        resp = requests.get(
            "https://itunes.apple.com/lookup",
            params={"id": app_id},
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        if resp.status_code != 200:
            logger.warning("Lookup API вернул статус %s, пропускаем проверку", resp.status_code)
            return True
        data = resp.json()
        return int(data.get("resultCount", 0)) > 0
    except Exception as exc:  # noqa: BLE001
        logger.warning("Не удалось проверить существование приложения: %s", exc)
        return True


# ---------------------------------------------------------------------------
# Работа с датами
# ---------------------------------------------------------------------------

def period_label(months: int) -> str:
    """Название периода для переключателя: "1 месяц", "2 месяца"."""
    return "1 месяц" if months == 1 else f"{months} месяца"


def period_phrase(months: int) -> str:
    """Фраза для сообщений: "за последний месяц", "за последние 2 месяца"."""
    return "за последний месяц" if months == 1 else f"за последние {months} месяца"


def subtract_months(dt: datetime, months: int) -> datetime:
    """Календарно корректно вычитает N месяцев из даты (с учётом длины месяца)."""
    month = dt.month - months
    year = dt.year
    while month <= 0:
        month += 12
        year -= 1
    last_day_of_target_month = calendar.monthrange(year, month)[1]
    day = min(dt.day, last_day_of_target_month)
    return dt.replace(year=year, month=month, day=day)


def parse_review_date(raw: str) -> Optional[datetime]:
    """Парсит дату отзыва (ISO 8601) в timezone-aware datetime (UTC)."""
    if not raw:
        return None
    text = raw.strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def filter_reviews_by_date(
    reviews: List[dict], cutoff: datetime, now: datetime
) -> List[dict]:
    """Оставляет только отзывы, дата которых попадает в [cutoff, now]."""
    result = []
    for review in reviews:
        pub_date = review.get("publication_date")
        if pub_date is not None and cutoff <= pub_date <= now:
            result.append(review)
    return result


# ---------------------------------------------------------------------------
# Определение языка
# ---------------------------------------------------------------------------

def detect_language(text: str) -> str:
    """
    Определяет язык текста локально (без отправки во внешние сервисы).
    Используется, если сам API не сообщает язык явно (для этого источника
    данных язык в ответе не приходит).
    """
    if not text or len(text.strip()) < 3:
        return "unknown"
    if not LANGDETECT_AVAILABLE:
        return "unknown"
    try:
        return detect(text)
    except LangDetectException:
        return "unknown"
    except Exception:  # noqa: BLE001
        return "unknown"


# ---------------------------------------------------------------------------
# Получение данных из Apple
# ---------------------------------------------------------------------------

def _build_feed_url(app_id: str, country: str, page: int) -> str:
    return (
        f"https://itunes.apple.com/{country}/rss/customerreviews/"
        f"page={page}/id={app_id}/sortby=mostrecent/json"
    )


STOPPED = "stopped"  # служебный код: загрузка остановлена пользователем


def _is_stopped(stop_event: Optional[threading.Event]) -> bool:
    return stop_event is not None and stop_event.is_set()


def _sleep(seconds: float, stop_event: Optional[threading.Event] = None) -> None:
    """Пауза, которую можно прервать сигналом остановки."""
    if stop_event is not None:
        stop_event.wait(seconds)
    else:
        time.sleep(seconds)


def _fetch_page_with_retry(
    url: str,
    session: requests.Session,
    stop_event: Optional[threading.Event] = None,
) -> Tuple[Optional[List[dict]], Optional[str]]:
    """
    Запрашивает одну страницу фида с retry + exponential backoff.
    Возвращает (список_сырых_записей, None) при успехе (пустой список —
    валидный результат, значит на странице нет отзывов), либо
    (None, код_ошибки) если все попытки исчерпаны. Если загрузка
    остановлена пользователем — (None, STOPPED).
    """
    delay = BACKOFF_BASE_SECONDS
    last_error: Optional[str] = None

    for attempt in range(1, MAX_RETRIES + 1):
        if _is_stopped(stop_event):
            return None, STOPPED
        try:
            resp = session.get(
                url,
                timeout=REQUEST_TIMEOUT_SECONDS,
                headers={"Accept": "application/json", "User-Agent": "Mozilla/5.0"},
            )
        except requests.exceptions.Timeout:
            last_error = "timeout"
        except requests.exceptions.ConnectionError:
            last_error = "connection_error"
        except requests.exceptions.RequestException as exc:  # noqa: BLE001
            last_error = f"request_error: {exc}"
        else:
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                wait_seconds = delay
                if retry_after and retry_after.strip().isdigit():
                    wait_seconds = float(retry_after)
                last_error = "rate_limited"
                logger.warning("429 Too Many Requests, ждём %.1f сек: %s", wait_seconds, url)
                _sleep(wait_seconds, stop_event)
                delay *= 2
                continue
            if resp.status_code == 404:
                # Для этого фида 404 обычно означает "на этой странице
                # для этого региона отзывов нет" — не ошибка.
                return [], None
            if resp.status_code != 200:
                last_error = f"http_{resp.status_code}"
            else:
                try:
                    data = resp.json()
                except ValueError:
                    last_error = "invalid_json"
                else:
                    return _extract_review_entries(data), None

        if attempt < MAX_RETRIES:
            _sleep(delay, stop_event)
            delay *= 2

    return None, last_error or "unknown_error"


def _extract_review_entries(data: dict) -> List[dict]:
    """
    Достаёт список "сырых" записей-отзывов из JSON фида.
    Первая запись в фиде — метаинформация о приложении, а не отзыв,
    поэтому отбрасываем записи без поля im:rating (у отзывов оно всегда
    есть, у служебной записи — нет).
    """
    if not isinstance(data, dict):
        return []
    feed = data.get("feed", {})
    if not isinstance(feed, dict):
        return []
    raw_entries = feed.get("entry", [])
    if isinstance(raw_entries, dict):
        raw_entries = [raw_entries]
    if not isinstance(raw_entries, list):
        return []
    return [e for e in raw_entries if isinstance(e, dict) and "im:rating" in e]


def parse_review(entry: dict, country: str) -> Optional[dict]:
    """Преобразует одну сырую запись фида в нормализованный словарь отзыва."""
    try:
        title = (entry.get("title", {}) or {}).get("label", "") or ""
        content = (entry.get("content", {}) or {}).get("label", "") or ""
        rating_raw = (entry.get("im:rating", {}) or {}).get("label", "")
        updated_raw = (entry.get("updated", {}) or {}).get("label", "")
        review_id = (entry.get("id", {}) or {}).get("label", "")

        try:
            rating = int(rating_raw)
        except (TypeError, ValueError):
            return None
        if rating < 1 or rating > 5:
            return None

        pub_date = parse_review_date(updated_raw)
        if pub_date is None:
            return None

        language = detect_language(content or title)

        return {
            "review_id": review_id,
            "review_text": content,
            "rating": rating,
            "publication_date": pub_date,
            "title": title,
            "country": country,
            "language": language,
        }
    except Exception as exc:  # noqa: BLE001
        logger.warning("Не удалось разобрать отзыв: %s", exc)
        return None


def fetch_reviews_for_storefront(
    app_id: str,
    country: str,
    session: requests.Session,
    stop_event: Optional[threading.Event] = None,
) -> Tuple[List[dict], Optional[str]]:
    """
    Забирает все доступные страницы (до MAX_PAGES_PER_STOREFRONT) отзывов
    для одного региона. Не останавливается на первой пустой странице —
    известно, что у этого фида бывают "дыры" (пустая страница, за которой
    следуют непустые), поэтому просматриваем весь диапазон страниц.

    Если загрузка остановлена (stop_event), возвращает то, что успело
    загрузиться для этого региона, без ошибки.

    Возвращает (список_сырых_записей, код_ошибки_если_регион_целиком_недоступен).
    """
    collected: List[dict] = []
    last_error: Optional[str] = None
    any_page_succeeded = False

    for page in range(1, MAX_PAGES_PER_STOREFRONT + 1):
        if _is_stopped(stop_event):
            break
        url = _build_feed_url(app_id, country, page)
        entries, error = _fetch_page_with_retry(url, session, stop_event)
        if entries is None:
            if error == STOPPED:
                break
            last_error = error
            continue
        any_page_succeeded = True
        collected.extend(entries)

    if not any_page_succeeded:
        if _is_stopped(stop_event):
            return [], None
        return [], last_error or "unknown_error"
    return collected, None


def get_available_storefronts() -> List[str]:
    return list(APP_STORE_COUNTRIES.keys())


def normalize_countries(selected: Optional[List[str]]) -> List[str]:
    """
    Приводит выбор пользователя к списку кодов стран.
    Пустой/None выбор означает "все страны". Неизвестные коды отбрасываются,
    повторы удаляются (порядок сохраняется).
    """
    available = get_available_storefronts()
    if not selected:
        return available
    known = set(available)
    result: List[str] = []
    for code in selected:
        normalized = str(code).strip().lower()
        if normalized in known and normalized not in result:
            result.append(normalized)
    return result


def fetch_all_reviews(
    app_id: str,
    countries: Optional[List[str]] = None,
    progress_callback=None,
    stop_event: Optional[threading.Event] = None,
    now: Optional[datetime] = None,
    period_months: int = PERIOD_MONTHS,
) -> Tuple[List[dict], List[dict]]:
    """
    Проходит по выбранным регионам (умеренно параллельно; если countries
    не задан — по всем), собирает и фильтрует отзывы за последние
    period_months месяцев (от момента now, по умолчанию — от текущего).

    Ошибка одного региона не останавливает остальные (partial success).
    Если выставлен stop_event, новые регионы не запускаются, а уже
    загруженные отзывы возвращаются как есть.
    Возвращает (список_отзывов, список_регионов_с_ошибками).
    """
    countries = normalize_countries(countries)
    total = len(countries)

    now = now or datetime.now(timezone.utc)
    cutoff = subtract_months(now, period_months)

    all_reviews: List[dict] = []
    failed_regions: List[dict] = []
    processed = 0
    lock = threading.Lock()
    session = requests.Session()

    def worker(country: str):
        if _is_stopped(stop_event):
            return None  # регион не запускался — остановка
        raw_entries, error = fetch_reviews_for_storefront(
            app_id, country, session, stop_event
        )
        parsed = [
            parsed_review
            for raw in raw_entries
            if (parsed_review := parse_review(raw, country)) is not None
        ]
        return country, parsed, error

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(worker, country): country for country in countries}
        for future in as_completed(futures):
            country = futures[future]
            try:
                result = future.result()
                if result is None:
                    continue  # регион пропущен из-за остановки
                _, parsed, error = result
            except Exception as exc:  # noqa: BLE001
                logger.error("Непредвиденная ошибка для региона %s: %s", country, exc)
                parsed, error = [], f"unexpected_error: {exc}"

            with lock:
                if error and not parsed:
                    failed_regions.append({"region": country, "error": error})
                filtered = filter_reviews_by_date(parsed, cutoff, now)
                all_reviews.extend(filtered)
                processed += 1
                if progress_callback:
                    progress_callback(
                        processed, total, country, len(all_reviews), list(failed_regions)
                    )

    return all_reviews, failed_regions


def check_review_source_health(session: Optional[requests.Session] = None) -> Optional[bool]:
    """
    Контрольная проверка источника: отдаёт ли фид Apple отзывы для заведомо
    популярного приложения.

    True  — отзывы получены, источник работает;
    False — фид отвечает (HTTP 200/404), но отзывов нет ни на одной странице
            ни в одной контрольной стране: вероятно, Apple перестал отдавать
            отзывы через этот фид;
    None  — проверить не удалось (сетевая/HTTP-ошибка): вывода делать нельзя.

    Просматриваются все страницы, а не только первая: у фида бывают
    "дыры" (пустая страница, за которой идут непустые).
    """
    session = session or requests.Session()
    for country in HEALTHCHECK_COUNTRIES:
        for page in range(1, MAX_PAGES_PER_STOREFRONT + 1):
            url = _build_feed_url(HEALTHCHECK_APP_ID, country, page)
            entries, error = _fetch_page_with_retry(url, session)
            if entries is None:
                logger.warning("Контрольная проверка источника не удалась: %s", error)
                return None
            if entries:
                return True
    return False


# ---------------------------------------------------------------------------
# Фоновая задача загрузки (позволяет показывать прогресс и нажимать "Стоп")
# ---------------------------------------------------------------------------

class FetchJob:
    """
    Загрузка отзывов в фоновом потоке. Интерфейс только читает состояние
    (snapshot) и может запросить остановку (request_stop) — сам сбор данных
    от интерфейса не зависит.
    """

    def __init__(
        self, app_id: str, countries: List[str], period_months: int = PERIOD_MONTHS
    ) -> None:
        self.app_id = app_id
        self.period_months = period_months
        self.countries = list(countries)
        self.total = len(self.countries)
        self.started_at = datetime.now(timezone.utc)
        self.stop_event = threading.Event()
        self.done_event = threading.Event()
        self._lock = threading.Lock()
        self._processed = 0
        self._current_country = ""
        self._reviews_count = 0
        self.reviews: List[dict] = []
        self.failed_regions: List[dict] = []
        self.crashed = False
        self._thread = threading.Thread(target=self._run, name="fetch-job", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def request_stop(self) -> None:
        self.stop_event.set()

    @property
    def stop_requested(self) -> bool:
        return self.stop_event.is_set()

    @property
    def is_done(self) -> bool:
        return self.done_event.is_set()

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "processed": self._processed,
                "total": self.total,
                "current_country": self._current_country,
                "reviews_count": self._reviews_count,
            }

    @property
    def was_stopped(self) -> bool:
        """True, если загрузка прервана до обработки всех регионов."""
        with self._lock:
            return self.stop_event.is_set() and self._processed < self.total

    def _on_progress(self, processed, total, country, reviews_so_far, failed_so_far) -> None:
        with self._lock:
            self._processed = processed
            self._current_country = country
            self._reviews_count = reviews_so_far

    def _run(self) -> None:
        try:
            reviews, failed = fetch_all_reviews(
                self.app_id,
                countries=self.countries,
                progress_callback=self._on_progress,
                stop_event=self.stop_event,
                now=self.started_at,
                period_months=self.period_months,
            )
            with self._lock:
                self.reviews = reviews
                self.failed_regions = failed
        except Exception:  # noqa: BLE001
            logger.exception("Фоновая загрузка завершилась с непредвиденной ошибкой")
            self.crashed = True
        finally:
            self.done_event.set()


# ---------------------------------------------------------------------------
# DataFrame / CSV
# ---------------------------------------------------------------------------

def build_dataframe(reviews: List[dict]) -> pd.DataFrame:
    """Строит DataFrame из списка отзывов. Дубликаты намеренно не удаляются."""
    if not reviews:
        return pd.DataFrame(columns=REQUIRED_COLUMNS)
    df = pd.DataFrame(reviews)
    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            df[col] = None
    return df


def sort_reviews(df: pd.DataFrame) -> pd.DataFrame:
    """Сортирует отзывы от новых к старым (publication_date DESC)."""
    if df.empty or "publication_date" not in df.columns:
        return df
    return df.sort_values(
        by="publication_date", ascending=False, kind="stable"
    ).reset_index(drop=True)


def validate_dataframe(df: pd.DataFrame, cutoff: datetime, now: datetime) -> List[str]:
    """Проверяет DataFrame перед экспортом. Возвращает список найденных проблем."""
    problems: List[str] = []
    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            problems.append(f"missing_column:{col}")
    if df.empty:
        return problems
    if "rating" in df.columns and not df["rating"].between(1, 5).all():
        problems.append("rating_out_of_range")
    if "publication_date" in df.columns:
        if ((df["publication_date"] < cutoff) | (df["publication_date"] > now)).any():
            problems.append("date_out_of_period")
        if not df["publication_date"].is_monotonic_decreasing:
            problems.append("not_sorted_desc")
    return problems


def export_csv(df: pd.DataFrame, app_id: str) -> Tuple[str, bytes]:
    """Готовит CSV (UTF-8 with BOM, для корректного открытия в Excel)."""
    export_df = df.copy()
    if "publication_date" in export_df.columns:
        export_df["publication_date"] = export_df["publication_date"].apply(
            lambda d: d.isoformat() if pd.notnull(d) else ""
        )
    columns = [c for c in EXPORT_COLUMNS if c in export_df.columns]
    buffer = io.StringIO()
    export_df.to_csv(buffer, columns=columns, index=False)
    csv_bytes = buffer.getvalue().encode("utf-8-sig")
    filename = f"appstore_reviews_{app_id}.csv"
    return filename, csv_bytes


# ---------------------------------------------------------------------------
# Streamlit UI
# ---------------------------------------------------------------------------

def _format_display_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    display_df = df.copy()
    if "publication_date" in display_df.columns:
        display_df["publication_date"] = display_df["publication_date"].apply(
            lambda d: d.strftime("%Y-%m-%d %H:%M UTC") if pd.notnull(d) else ""
        )
    ordered = [c for c in EXPORT_COLUMNS if c in display_df.columns]
    return display_df[ordered]


def _show_job_progress(job: FetchJob) -> None:
    """Показывает кнопку "Стоп" и прогресс, пока загрузка не завершится."""
    if st.button("Стоп", key="stop_btn"):
        job.request_stop()

    status_placeholder = st.empty()
    progress_bar = st.progress(0)
    counters_placeholder = st.empty()

    while True:
        snap = job.snapshot()
        if job.stop_requested:
            status_placeholder.text("Останавливаем загрузку...")
        elif snap["current_country"]:
            region_name = APP_STORE_COUNTRIES.get(snap["current_country"], snap["current_country"])
            status_placeholder.text(f"Обрабатывается регион: {region_name}")
        else:
            status_placeholder.text("Получение данных...")
        progress_bar.progress(min(snap["processed"] / max(snap["total"], 1), 1.0))
        counters_placeholder.text(
            f"Обработано регионов: {snap['processed']} / {snap['total']}    "
            f"Получено отзывов: {snap['reviews_count']}"
        )
        if job.done_event.wait(timeout=0.5):
            break


def _finalize_job(job: FetchJob) -> None:
    """Собирает итоговую таблицу из того, что загружено (в т.ч. после "Стоп")."""
    reviews = list(job.reviews)
    failed_regions = list(job.failed_regions)
    logger.info(
        "Сбор завершён: получено %d отзывов, ошибок регионов: %d, остановлено: %s",
        len(reviews), len(failed_regions), job.was_stopped,
    )

    df = sort_reviews(build_dataframe(reviews))
    cutoff = subtract_months(job.started_at, job.period_months)
    problems = validate_dataframe(df, cutoff, datetime.now(timezone.utc))
    if problems:
        logger.error("Проблемы валидации данных перед экспортом: %s", problems)

    # Если отзывов 0 — выясняем, это "отзывов нет" или "источник не работает".
    source_status = None
    if df.empty and not job.was_stopped and not job.crashed:
        with st.spinner("Проверяем доступность источника данных..."):
            health = check_review_source_health()
        source_status = {True: "ok", False: "dead"}.get(health, "unknown")
        logger.info("Контрольная проверка источника: %s", source_status)

    snap = job.snapshot()
    st.session_state["source_status"] = source_status
    st.session_state["result_df"] = df
    st.session_state["failed_regions"] = failed_regions
    st.session_state["app_id"] = job.app_id
    st.session_state["period_months"] = job.period_months
    st.session_state["total_countries"] = job.total
    st.session_state["processed_regions"] = snap["processed"]
    st.session_state["was_stopped"] = job.was_stopped
    st.session_state["crashed"] = job.crashed
    st.session_state["job"] = None


def _show_results() -> None:
    df: pd.DataFrame = st.session_state["result_df"]
    failed_regions: List[dict] = st.session_state.get("failed_regions", [])
    app_id: str = st.session_state.get("app_id", "app")
    total_countries: int = st.session_state.get("total_countries", 0)
    processed_regions: int = st.session_state.get("processed_regions", total_countries)
    was_stopped: bool = st.session_state.get("was_stopped", False)
    crashed: bool = st.session_state.get("crashed", False)
    successful_regions = max(processed_regions - len(failed_regions), 0)

    summary = (
        f"Получено отзывов: {len(df)}\n"
        f"Успешно обработано регионов: {successful_regions} / {total_countries}\n"
        f"Регионов с ошибками: {len(failed_regions)}"
    )
    if crashed:
        st.error(
            "Сбор данных завершился с ошибкой. Показано то, что успело "
            "загрузиться. Попробуйте повторить операцию позже."
        )
    elif was_stopped:
        st.warning(
            "Загрузка остановлена. Показаны отзывы, которые успели загрузиться "
            "(последние регионы могли загрузиться не полностью).\n\n" + summary
        )
    else:
        st.success("Сбор завершён.\n\n" + summary)

    if df.empty:
        source_status = st.session_state.get("source_status")
        if source_status == "dead":
            st.error(
                "Публичный фид отзывов Apple отвечает, но не отдаёт ни одного "
                "отзыва даже для контрольного популярного приложения. Скорее "
                "всего, Apple перестал отдавать отзывы через этот источник, "
                "а не у вашего приложения их нет. Приложение само это "
                "исправить не может: нужен другой источник данных "
                "(см. README, раздел «Ограничения Apple»)."
            )
        else:
            period_months = st.session_state.get("period_months", PERIOD_MONTHS)
            st.info(f"{period_phrase(period_months).capitalize()} отзывы не найдены.")
            if source_status == "unknown":
                st.caption(
                    "Не удалось выполнить контрольную проверку источника "
                    "данных, поэтому исключить сбой на стороне Apple нельзя."
                )
    else:
        st.dataframe(_format_display_dataframe(df), use_container_width=True)
        filename, csv_bytes = export_csv(df, app_id)
        st.download_button(
            label="Скачать CSV",
            data=csv_bytes,
            file_name=filename,
            mime="text/csv",
        )

    if failed_regions:
        with st.expander(f"Регионы с ошибками ({len(failed_regions)})"):
            for item in failed_regions:
                region_name = APP_STORE_COUNTRIES.get(item["region"], item["region"])
                st.text(f"{region_name} ({item['region']}): {item['error']}")

    st.caption(
        "Источник: публичный RSS-фид отзывов Apple. Он отдаёт не более "
        "~500 самых новых отзывов на регион, поэтому если в каком-то "
        "регионе за выбранный период отзывов оставлено больше — часть самых "
        "старых из них физически недоступна через этот источник."
    )


def main() -> None:
    st.set_page_config(page_title="App Store Reviews Downloader", page_icon="⭐", layout="centered")
    st.title("App Store Reviews Downloader")
    st.caption(
        "Введите ID приложения из Apple App Store и получите CSV со всеми "
        "доступными отзывами за последний месяц или 2 месяца. Страны можно "
        "ограничить (по умолчанию — все)."
    )

    job: Optional[FetchJob] = st.session_state.get("job")
    running = job is not None

    raw_app_id = st.text_input(
        "ID приложения в App Store",
        placeholder="123456789",
        help="Число после «id» в ссылке на приложение: apps.apple.com/.../id123456789",
        disabled=running,
    )
    selected_countries = st.multiselect(
        "Страны (необязательно)",
        options=sorted(APP_STORE_COUNTRIES.keys(), key=lambda c: APP_STORE_COUNTRIES[c]),
        format_func=lambda c: f"{APP_STORE_COUNTRIES[c]} ({c.upper()})",
        placeholder="Все страны",
        help="Если ничего не выбрано — отзывы собираются по всем странам. "
             "Выбор нескольких стран заметно ускоряет сбор.",
        disabled=running,
    )
    period_months = st.radio(
        "Период (от сегодняшнего дня)",
        options=list(PERIOD_OPTIONS),
        format_func=period_label,
        index=PERIOD_OPTIONS.index(PERIOD_MONTHS),
        horizontal=True,
        help="За какой период собирать отзывы. Чем длиннее период, тем больше "
             "отзывов может не поместиться в лимит Apple (~500 на страну).",
        disabled=running,
    )
    start_clicked = st.button("Получить отзывы", type="primary", disabled=running)

    if start_clicked and not running:
        for key in ("result_df", "failed_regions", "app_id", "processed_regions",
                    "total_countries", "was_stopped", "crashed", "source_status",
                    "period_months"):
            st.session_state.pop(key, None)

        if not raw_app_id or not raw_app_id.strip():
            st.error("Введите ID приложения App Store.")
            return

        try:
            app_id = get_app_id(raw_app_id)
        except ValueError:
            st.error(
                "Некорректный ID. Введите числовой ID приложения App Store "
                "(например, 123456789)."
            )
            return

        with st.spinner("Проверяем приложение..."):
            exists = check_app_exists(app_id)
        if not exists:
            st.error(
                "Не удалось найти приложение по указанному ID. Проверьте ID "
                "или убедитесь, что приложение доступно в App Store."
            )
            return

        countries_to_fetch = normalize_countries(selected_countries)
        logger.info(
            "Начало сбора отзывов для app_id=%s, стран: %d, период: %d мес.",
            app_id, len(countries_to_fetch), period_months,
        )
        job = FetchJob(app_id, countries_to_fetch, period_months)
        st.session_state["job"] = job
        job.start()

    if job is not None:
        _show_job_progress(job)
        _finalize_job(job)
        st.rerun()

    if "result_df" in st.session_state:
        _show_results()


if __name__ == "__main__":
    main()

"""
App Store Reviews Downloader
=============================

Внутренний инструмент для быстрой выгрузки отзывов пользователей из Apple
App Store по всем доступным регионам за последние 2 месяца.

Источник данных: публичный JSON RSS-фид Apple "customer reviews"
(https://itunes.apple.com/{country}/rss/customerreviews/page={page}/id={app_id}/sortby=mostrecent/json).
Это официальный публичный endpoint Apple, не требующий авторизации,
API-ключей или аккаунта App Store Connect. Он используется самим сайтом
apps.apple.com и множеством open-source инструментов для чтения отзывов.

ВАЖНОЕ ОГРАНИЧЕНИЕ ИСТОЧНИКА ДАННЫХ (см. README.md, раздел "Ограничения Apple"):
Apple отдаёт не более ~10 страниц по 50 отзывов (то есть не более примерно
500 самых новых отзывов) на каждую пару "приложение + регион". Если за
последние 2 месяца в конкретном регионе было оставлено больше отзывов, чем
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
from urllib.parse import urlparse

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
PERIOD_MONTHS = 2

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

APP_STORE_URL_ID_PATTERN = re.compile(r"id(\d{5,12})", re.IGNORECASE)
VALID_APP_STORE_HOSTS = {"apps.apple.com", "itunes.apple.com"}


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
    "cg": "Republic of the Congo", "ro": "Romania", "rw": "Rwanda",
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
# Разбор и валидация URL
# ---------------------------------------------------------------------------

def validate_app_store_url(url: str) -> bool:
    """Проверяет, что строка похожа на ссылку на приложение в App Store."""
    if not url or not isinstance(url, str):
        return False
    candidate = url.strip()
    if not candidate:
        return False
    if not re.match(r"^https?://", candidate, re.IGNORECASE):
        candidate = "https://" + candidate
    try:
        parsed = urlparse(candidate)
    except Exception:
        return False
    host = (parsed.netloc or "").lower()
    if host.startswith("www."):
        host = host[4:]
    if ":" in host:
        host = host.split(":", 1)[0]
    if host not in VALID_APP_STORE_HOSTS:
        return False
    if not APP_STORE_URL_ID_PATTERN.search(parsed.path + "?" + parsed.query):
        return False
    return True


def parse_app_store_url(url: str) -> Optional[str]:
    """Извлекает числовой App ID из ссылки App Store, либо None."""
    if not url or not isinstance(url, str):
        return None
    candidate = url.strip()
    if not candidate:
        return None
    match = APP_STORE_URL_ID_PATTERN.search(candidate)
    if not match:
        return None
    return match.group(1)


def get_app_id(url: str) -> str:
    """Возвращает App ID или бросает ValueError, если ссылка некорректна."""
    if not validate_app_store_url(url):
        raise ValueError("invalid_url")
    app_id = parse_app_store_url(url)
    if not app_id:
        raise ValueError("invalid_url")
    return app_id


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


def _fetch_page_with_retry(
    url: str, session: requests.Session
) -> Tuple[Optional[List[dict]], Optional[str]]:
    """
    Запрашивает одну страницу фида с retry + exponential backoff.
    Возвращает (список_сырых_записей, None) при успехе (пустой список —
    валидный результат, значит на странице нет отзывов), либо
    (None, код_ошибки) если все попытки исчерпаны.
    """
    delay = BACKOFF_BASE_SECONDS
    last_error: Optional[str] = None

    for attempt in range(1, MAX_RETRIES + 1):
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
                time.sleep(wait_seconds)
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
            time.sleep(delay)
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
    app_id: str, country: str, session: requests.Session
) -> Tuple[List[dict], Optional[str]]:
    """
    Забирает все доступные страницы (до MAX_PAGES_PER_STOREFRONT) отзывов
    для одного региона. Не останавливается на первой пустой странице —
    известно, что у этого фида бывают "дыры" (пустая страница, за которой
    следуют непустые), поэтому просматриваем весь диапазон страниц.

    Возвращает (список_сырых_записей, код_ошибки_если_регион_целиком_недоступен).
    """
    collected: List[dict] = []
    last_error: Optional[str] = None
    any_page_succeeded = False

    for page in range(1, MAX_PAGES_PER_STOREFRONT + 1):
        url = _build_feed_url(app_id, country, page)
        entries, error = _fetch_page_with_retry(url, session)
        if entries is None:
            last_error = error
            continue
        any_page_succeeded = True
        collected.extend(entries)

    if not any_page_succeeded:
        return [], last_error or "unknown_error"
    return collected, None


def get_available_storefronts() -> List[str]:
    return list(APP_STORE_COUNTRIES.keys())


def fetch_all_reviews(
    app_id: str, progress_callback=None
) -> Tuple[List[dict], List[dict]]:
    """
    Проходит по всем регионам (умеренно параллельно), собирает и
    фильтрует отзывы за последние PERIOD_MONTHS месяцев.

    Ошибка одного региона не останавливает остальные (partial success).
    Возвращает (список_отзывов, список_регионов_с_ошибками).
    """
    countries = get_available_storefronts()
    total = len(countries)

    now = datetime.now(timezone.utc)
    cutoff = subtract_months(now, PERIOD_MONTHS)

    all_reviews: List[dict] = []
    failed_regions: List[dict] = []
    processed = 0
    lock = threading.Lock()
    session = requests.Session()

    def worker(country: str):
        raw_entries, error = fetch_reviews_for_storefront(app_id, country, session)
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
                _, parsed, error = future.result()
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


def main() -> None:
    st.set_page_config(page_title="App Store Reviews Downloader", page_icon="⭐", layout="centered")
    st.title("App Store Reviews Downloader")
    st.caption(
        "Вставьте ссылку на приложение в Apple App Store и получите CSV со всеми "
        "доступными отзывами за последние 2 месяца из всех доступных регионов."
    )

    url = st.text_input(
        "Ссылка на приложение в App Store",
        placeholder="https://apps.apple.com/us/app/example/id123456789",
    )
    start_clicked = st.button("Получить отзывы", type="primary")

    if start_clicked:
        st.session_state.pop("result_df", None)
        st.session_state.pop("failed_regions", None)
        st.session_state.pop("app_id", None)

        if not url or not url.strip():
            st.error("Введите ссылку на приложение App Store.")
            return

        if not validate_app_store_url(url):
            st.error("Некорректная ссылка. Введите ссылку на приложение Apple App Store.")
            return

        try:
            app_id = get_app_id(url)
        except ValueError:
            st.error("Не удалось определить приложение по этой ссылке. Проверьте URL App Store.")
            return

        with st.spinner("Проверяем приложение..."):
            exists = check_app_exists(app_id)
        if not exists:
            st.error(
                "Не удалось найти приложение по указанной ссылке. Проверьте URL "
                "или убедитесь, что приложение доступно в App Store."
            )
            return

        status_placeholder = st.empty()
        progress_bar = st.progress(0)
        counters_placeholder = st.empty()
        total_countries = len(get_available_storefronts())

        def on_progress(processed, total, country, reviews_so_far, failed_so_far):
            region_name = APP_STORE_COUNTRIES.get(country, country)
            status_placeholder.text(f"Обрабатывается регион: {region_name}")
            progress_bar.progress(min(processed / total, 1.0))
            counters_placeholder.text(
                f"Обработано регионов: {processed} / {total}    "
                f"Получено отзывов: {reviews_so_far}"
            )

        logger.info("Начало сбора отзывов для app_id=%s", app_id)
        reviews, failed_regions = fetch_all_reviews(app_id, progress_callback=on_progress)
        logger.info(
            "Сбор завершён: получено %d отзывов, ошибок регионов: %d",
            len(reviews), len(failed_regions),
        )

        progress_bar.progress(1.0)
        status_placeholder.text("Готово.")

        now = datetime.now(timezone.utc)
        cutoff = subtract_months(now, PERIOD_MONTHS)

        df = build_dataframe(reviews)
        df = sort_reviews(df)

        problems = validate_dataframe(df, cutoff, now)
        if problems:
            logger.error("Проблемы валидации данных перед экспортом: %s", problems)

        st.session_state["result_df"] = df
        st.session_state["failed_regions"] = failed_regions
        st.session_state["app_id"] = app_id
        st.session_state["total_countries"] = total_countries

    if "result_df" in st.session_state:
        df: pd.DataFrame = st.session_state["result_df"]
        failed_regions: List[dict] = st.session_state.get("failed_regions", [])
        app_id: str = st.session_state.get("app_id", "app")
        total_countries: int = st.session_state.get(
            "total_countries", len(get_available_storefronts())
        )
        successful_regions = total_countries - len(failed_regions)

        st.success(
            f"Сбор завершён.\n\n"
            f"Получено отзывов: {len(df)}\n"
            f"Успешно обработано регионов: {successful_regions} / {total_countries}\n"
            f"Регионов с ошибками: {len(failed_regions)}"
        )

        if df.empty:
            st.info("За последние 2 месяца отзывы не найдены.")
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
            "регионе за 2 месяца отзывов оставлено больше — часть самых "
            "старых из них физически недоступна через этот источник."
        )


if __name__ == "__main__":
    main()

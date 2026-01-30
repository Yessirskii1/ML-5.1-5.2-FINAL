import csv
import random
import sqlite3
import time
import uuid             #gen uid
from datetime import datetime, timezone
from threading import Lock

import requests
import urllib3          #отключить предупреждения ssl
from bs4 import BeautifulSoup



DB_FILE = "news.db"
CSV_FILE = "news.csv"


TARGET_PER_SOURCE = 10000                           # Сколько статей нужно собрать с каждого источника
MAX_PAGES_PER_SOURCE = 200                          # будем ходить только по 200 страницам
SOURCES = ["habr", "scientific_russia"]             # Список источников, обрабатываем в main() по очереди


HABR_ROOT = "https://habr.com"
HABR_NEWS = "https://habr.com/ru/news/"

SCIRUSSIA_ROOT = "https://scientificrussia.ru"
SCIRUSSIA_NEWS = "https://scientificrussia.ru/news"


REQUEST_TIMEOUT = 10     #ожидание ответа
REQUEST_DELAY = 1        #пауза между странцами

# колонки CSV
CSV_COLUMNS = [
    "guid",
    "title",
    "description",
    "url",
    "published_at",
    "comments_count",
    "created_at_utc",
    "rating",
]

WRITE_LOCK = Lock()

BROWSER_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0",
]


def prepare_storage():              #бд

    with sqlite3.connect(DB_FILE) as conn:          #подключаемся к базе, если  нет - создается автоматически
        cursor = conn.cursor()                      # задаем курсор для выполнения sql команд
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS news (
                guid TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                description TEXT NOT NULL,
                url TEXT UNIQUE NOT NULL,
                published_at TEXT,
                comments_count INTEGER,
                created_at_utc TEXT NOT NULL,
                rating REAL
            );
            """
        )
        # Индексы ускоряют поиск по URL и дате
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_url ON news(url)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_published_at ON news(published_at)")
        conn.commit()


def get_total_count():                  #счетчик статей в бд
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM news")
            return cursor.fetchone()[0]                 #забираем результат
    except Exception as err:
        # если БД недоступна — вернем 0, чтобы парсер не падал
        print(f"[Ошибка БД] Не удалось получить количество: {err}")
        return 0


def article_exists(url):        #проверяем есть ли такая статья в бд по url(2
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            # ищем запись по уникальному URL
            cursor.execute("SELECT 1 FROM news WHERE url = ?", (url,))
            return cursor.fetchone() is not None
    except Exception as err:
        # луучше пропустить, чем упасть на ошибке
        print(f"[Ошибка БД] Не удалось проверить URL: {err}")
        return False


def store_record(item_data):            # принимает статью в виде словаря и записывает в бд
    with WRITE_LOCK:
        try:
            with sqlite3.connect(DB_FILE) as conn:
                cursor = conn.cursor()
                # INSERT OR IGNORE: если URL уже есть — запись игнорируется
                cursor.execute(
                    """
                    INSERT OR IGNORE INTO news (
                        guid, title, description, url, published_at,
                        comments_count, created_at_utc, rating
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(uuid.uuid4()),
                        item_data.get("title"),
                        item_data.get("description"),
                        item_data.get("url"),
                        item_data.get("published_at"),
                        item_data.get("comments_count", 0),
                        item_data.get("created_at_utc"),
                        item_data.get("rating"),
                    ),
                )
                conn.commit()
                return True
        except Exception as err:
            print(f"[Ошибка БД] {err}")
            return False


def fetch_html_safe(target_link, max_tries=5):                  #ф-я для скачивания страницы по url
    for try_num in range(max_tries):
        try:
            # случайный User-Agent
            headers = {"User-Agent": random.choice(BROWSER_LIST)}
            # ауза между запросами
            time.sleep(random.uniform(0.1, 0.3))

            # скачиваем страницу
            resp = requests.get(
                target_link,
                headers=headers,
                timeout=REQUEST_TIMEOUT,
                verify=False,           #ssl
            )
            #логика что делать с ответом
            if resp.status_code == 200:
                resp.encoding = "utf-8"
                return BeautifulSoup(resp.text, "html.parser")  #возвращает разобраную страницу
            if resp.status_code == 404:
                return None

            time.sleep(2)
        except Exception:
            # если ошибка другая делаем еще попытку
            time.sleep(2 * (try_num + 1))

    return None


def normalize_article(data):            #приводим текст в стандартный вид
    if not data:
        return None
    #время добавления
    data["created_at_utc"] = datetime.now(timezone.utc).isoformat()
    #заполняет 0 com_count если нет
    if "comments_count" not in data or data["comments_count"] is None:
        data["comments_count"] = 0
    # если нет рейтинга — оставляем None
    if "rating" not in data:
        data["rating"] = None
    return data




def habr_list_url(page_num):
    """URL списка новостей Habr."""
    # первая страница отличается от остальных
    if page_num == 1:
        return HABR_NEWS
    return f"{HABR_ROOT}/ru/news/page{page_num}/"


def parse_habr_list(page_soup):
    """Ищет ссылки на статьи Habr на странице списка."""
    urls = []
    # на Habr ссылки на новости находятся по селектору .tm-title__link
    for link in page_soup.select("a.tm-title__link"):
        href = link.get("href")
        if href and "/ru/news/" in href:
            urls.append(HABR_ROOT + href)
    # убираем дубликаты, оставляем до 20 ссылок
    return list(dict.fromkeys(urls))[:20]


def parse_habr_article(url, page_soup):
    """Парсит статью Habr и возвращает словарь данных."""
    # заголовок — обычный <h1>
    title = "Без заголовка"
    title_elem = page_soup.select_one("h1")
    if title_elem:
        title_text = title_elem.get_text(strip=True)
        if title_text:
            title = title_text

    # дата  <time datetime="...">
    published_at = None
    time_elem = page_soup.select_one("time[datetime]")
    if time_elem:
        published_at = time_elem.get("datetime")

    # контент статьи
    description = ""
    content_elem = (
        page_soup.select_one(".article-formatted-body")
        or page_soup.select_one(".tm-article-body")
    )
    if content_elem:
        # Убираем код/скрипты — это не часть текста статьи
        for tag in content_elem.select("script, style, iframe, pre, code"):
            tag.decompose()
        description = content_elem.get_text(separator="\n", strip=True)

    # если текста нет — пропускаем статью
    if not description or len(description) < 50:
        return None

    # комментарии: пытаемся вытащить число
    comments_count = 0
    comments_elem = page_soup.select_one(".tm-article-comments-counter-link")
    if comments_elem:
        text = comments_elem.get_text()
        digits = "".join([c for c in text if c.isdigit()])
        if digits:
            comments_count = int(digits)

    # рейтинг: если есть, пробуем превратить в число
    rating = None
    rating_elem = page_soup.select_one(".tm-votes__value")
    if rating_elem:
        text = rating_elem.get_text(strip=True)
        try:
            rating = float(text.replace("+", ""))
        except Exception:
            rating = None

    # возвращаем готовый словарь (добавляем обязательные поля)
    return normalize_article(
        {
            "title": title,
            "description": description,
            "url": url,
            "published_at": published_at,
            "comments_count": comments_count,
            "rating": rating,
        }
    )



# ИСТОЧНИК 2: Scientific Russia
def scirussia_list_url(page_num):
    """URL списка новостей Scientific Russia."""
    # первая страница отличается от остальных
    if page_num == 1:
        return SCIRUSSIA_NEWS
    return f"{SCIRUSSIA_NEWS}/{page_num}"


def parse_scirussia_list(page_soup):
    """Ищет ссылки на статьи Scientific Russia."""
    urls = []
    # на Scientific russia статьи находятся по путям /articles/
    for link in page_soup.find_all("a", href=True):
        href = link.get("href", "")
        if "/articles/" in href:
            full = href if href.startswith("http") else SCIRUSSIA_ROOT + href
            # Отсекаем теги, рубрики, якоря и параметры
            if not any(x in full for x in ["/tags/", "/rubric/", "?", "#"]):
                urls.append(full)
    # Убираем дубликаты, ограничиваем число ссылок
    return list(dict.fromkeys(urls))[:20]


def parse_scirussia_article(url, page_soup):
    """Парсит статью Scientific Russia и возвращает словарь данных."""

    title = ""
    title_elem = (
        page_soup.select_one('h1[itemprop="headline"]')
        or page_soup.select_one("article h1")
        or page_soup.select_one("h1")
    )
    if title_elem:
        title = title_elem.get_text(strip=True)
    if not title:
        # без заголовка статью ропускаем
        return None

    # дата публикации: сначала мета-тег, потом time
    published_at = None
    meta_date = page_soup.find("meta", {"property": "article:published_time"})
    if meta_date and meta_date.get("content"):
        published_at = meta_date["content"]
    else:
        time_elem = page_soup.find("time", {"datetime": True})
        if time_elem:
            published_at = time_elem.get("datetime")

    # основной текст статьи
    description = ""
    content_elem = (
        page_soup.select_one('div[itemprop="articleBody"]')
        or page_soup.select_one(".article-text")
        or page_soup.select_one("article")
    )
    if content_elem:
        # чистим от мусорных элементов
        for tag in content_elem.select("script, style, iframe"):
            tag.decompose()
        # берем только содержательные абзацы
        paragraphs = content_elem.find_all("p")
        parts = [p.get_text(strip=True) for p in paragraphs if len(p.get_text()) > 30]
        description = "\n".join(parts)[:5000]

    # если текста мало — считаем статью неполной
    if not description or len(description) < 50:
        return None


    return normalize_article(
        {
            "title": title,
            "description": description,
            "url": url,
            "published_at": published_at,
            "comments_count": 0,
            "rating": None,
        }
    )

# Один проход while = одна страница списка
# Внутри него for = статьи на этой странице
def parse_source(source_key, list_url_func, list_parser, article_parser):

    saved_count = 0     #сколько новых статей реально сохранили для этого источника
    page_num = 1        #страница списка
    empty_pages = 0     #счетчик пустых страниц

    while saved_count < TARGET_PER_SOURCE and page_num <= MAX_PAGES_PER_SOURCE:
        page_url = list_url_func(page_num)                          # получаем URL страницы списка новостей
        print(f"[{source_key}] страница {page_num}: {page_url}")

        page_soup = fetch_html_safe(page_url)       #скачиваем HTML страницы списка
        if not page_soup:
            empty_pages += 1
            if empty_pages > 3:
                print(f"[{source_key}] слишком много пустых страниц, остановка")
                break
            page_num += 1  #переход к другой страницы
            continue


        article_urls = list_parser(page_soup)                #получаем ссылки на статьи страницы
        if not article_urls:
            empty_pages += 1
            if empty_pages > 2:
                print(f"[{source_key}] нет статей, остановка")
                break
            page_num += 1
            continue

        # идем по каждой статье
        for article_url in article_urls:
            if saved_count >= TARGET_PER_SOURCE:
                break
            # дубликаты пропускаем
            if article_exists(article_url):
                continue

            article_soup = fetch_html_safe(article_url)
            if not article_soup:
                continue

            # парсим статью в словарь
            article_data = article_parser(article_url, article_soup)
            if not article_data:
                continue

            # сохраняем в БД
            if store_record(article_data):
                saved_count += 1
                if saved_count % 50 == 0:
                    print(f"[{source_key}] сохранено: {saved_count}")

        time.sleep(REQUEST_DELAY)
        page_num += 1

    print(f"[{source_key}] завершено, сохранено {saved_count}")
    return saved_count


def validate_csv_columns():
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            # PRAGMA table_info возвращает список колонок таблицы
            cursor.execute("PRAGMA table_info(news)")
            table_columns = [row[1] for row in cursor.fetchall()]
    except Exception as err:
        print(f"[Ошибка БД] Не удалось проверить колонки: {err}")
        return False

    if table_columns != CSV_COLUMNS:
        print("[Ошибка] Колонки БД не совпадают с CSV_COLUMNS")
        print(f"Ожидание: {CSV_COLUMNS}")
        print(f"Факт: {table_columns}")
        return False

    return True


def export_csv():
    # сначала убеждаемся, что колонки совпадают
    if not validate_csv_columns():
        return

    # берем все строки из БД
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute(f"SELECT {', '.join(CSV_COLUMNS)} FROM news")
        rows = cursor.fetchall()

    # записываем CSV в строгом порядке колонок
    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(CSV_COLUMNS)
        writer.writerows(rows)

    print(f"[CSV] Готово: {CSV_FILE} ({len(rows)} строк)")


def main():
    prepare_storage()

    print("=" * 60)
    print(f"Цель: по {TARGET_PER_SOURCE} статей с источника")
    print(f"Источники: {', '.join(SOURCES)}")
    print("=" * 60)

    for source in SOURCES:
        if source == "habr":
            parse_source(source, habr_list_url, parse_habr_list, parse_habr_article)
        elif source == "scientific_russia":
            parse_source(source, scirussia_list_url, parse_scirussia_list, parse_scirussia_article)


    export_csv()


if __name__ == "__main__":
    main()

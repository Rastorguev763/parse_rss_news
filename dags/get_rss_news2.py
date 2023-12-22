from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import psycopg2
import requests

try:
    from bs4 import BeautifulSoup
except ImportError:
    import subprocess
    subprocess.run(["pip", "install", "bs4"])
    from bs4 import BeautifulSoup

try:
    import feedparser
except ImportError:
    import subprocess
    subprocess.run(["pip", "install", "feedparser"])
    import feedparser



# Параметры подключения к базе данных PostgreSQL
DB_CONNECTION = {
    'host': 'db',
    'port': '5432',
    'database': 'rssnews',
    'user': 'postgres',
    'password': 'postgres'
}

# Список URL-адресов RSS-каналов
RSS_FEEDS = [
    'https://www.vedomosti.ru/rss/news',
    'https://lenta.ru/rss/',
    'https://tass.ru/rss/v2.xml'
]

# Создание таблицы rss_data
create_table_query = """
CREATE TABLE IF NOT EXISTS rss_data (
    id_rss_data SERIAL PRIMARY KEY,
    title VARCHAR(255),
    link VARCHAR(255),
    description TEXT,
    published TIMESTAMP,
    category VARCHAR(255),
    author VARCHAR(255),
    site_name VARCHAR(255),
    source VARCHAR(255)
);
"""

# Подключение к базе данных и выполнение запроса
conn = psycopg2.connect(**DB_CONNECTION)
cursor = conn.cursor()
cursor.execute(create_table_query)
conn.commit()
conn.close()

def get_news_text_vedomosti(url):

    # Отправляем запрос на получение HTML-кода страницы
    response = requests.get(url)

    # Проверяем успешность запроса
    if response.status_code == 200:
        # Используем BeautifulSoup для парсинга HTML-кода
        soup = BeautifulSoup(response.text, 'html.parser')
        # Находим все элементы с классом 'box-paragraph__text' (параграфы с текстом новости)
        paragraphs = soup.find_all('p', class_='box-paragraph__text')
        news_text = ' '.join(paragraph.get_text() for paragraph in paragraphs)
        return news_text
    else:
        print("Ошибка при получении страницы. Код состояния:", response.status_code)

def check_titles_postgres(title_to_check, site_name):
    try:
        # Устанавливаем соединение с базой данных PostgreSQL
        conn = psycopg2.connect(**DB_CONNECTION)
        
        # Создаем курсор для выполнения SQL-запросов
        cursor = conn.cursor()
        
        # Выполнение запроса
        cursor.execute("SELECT COUNT(*) FROM rss_data WHERE title = %s AND site_name = %s", (title_to_check, site_name))
        result = cursor.fetchone()
        if result[0] > 0:
            # Новость с таким заголовком уже существует в базе
            return False
        else:
            return True

    except (Exception, psycopg2.Error) as e:
        print("Ошибка при проверке Title:", e)
        
    finally:
        # Закрываем курсор и соединение с базой данных
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


def parse_rss(feed_url):
    """Функция для парсинга RSS и сохранения данных в PostgreSQL."""
    conn = psycopg2.connect(**DB_CONNECTION)
    cursor = conn.cursor()

    # Парсим RSS
    feed = feedparser.parse(feed_url)

    # Итерируемся по элементам RSS и сохраняем данные в базу данных
    for entry in feed.entries:
        title = entry.title if hasattr(entry, 'title') else None
        link = entry.link if hasattr(entry, 'link') else None
        description = entry.description if hasattr(entry, 'description') else None
        published = entry.published if hasattr(entry, 'published') else None
        category = entry.category if hasattr(entry, 'category') else None
        author = entry.author if hasattr(entry, 'author') and len(entry.author) != 0 else 'Автор не указан'
        site_name = feed.feed.title
        source = feed_url
        
        if check_titles_postgres(title, site_name):
            # if 'vedomosti' in feed_url:
                # description = get_news_text_vedomosti(entry.link)

            # Выполняем SQL-запрос для вставки данных в таблицу
            insert_query = """
            INSERT INTO rss_data (title, link, description, published, category, author, site_name, source)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """
            cursor.execute(insert_query, (title, link, description, published, category, author, site_name, source))

    # Завершаем транзакцию и закрываем соединение
    conn.commit()
    conn.close()

# Определение DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime.utcnow(),
    'retries': 1,
}

dag = DAG(
    'rss_to_postgres_2',
    default_args=default_args,
    schedule_interval='*/10 * * * *',
)

# Определение оператора PythonOperator для каждого RSS-канала
operators = []
for i, feed_url in enumerate(RSS_FEEDS):
    task_id = f'parse_rss_{i}'
    operator = PythonOperator(
        task_id=task_id,
        python_callable=parse_rss,
        op_kwargs={'feed_url': feed_url},
        dag=dag,
    )
    operators.append(operator)

# Определение порядка выполнения задач
for i in range(len(operators) - 1):
    operators[i] >> operators[i+1]


if __name__ == "__main__":
    dag.cli()

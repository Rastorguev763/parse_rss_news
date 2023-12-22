from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import requests

try:
    from bs4 import BeautifulSoup
except ImportError:
    import subprocess
    subprocess.run(["pip", "install", "bs4"])
    from bs4 import BeautifulSoup

default_args = {
    'owner': 'airflow',
    'start_date': datetime.utcnow(),
    'retries': 1,
}

dag = DAG(
    'extract_and_fill_news_dag',
    default_args=default_args,
    schedule_interval='@daily',
)

DB_CONNECTION = {
    'host': 'db',
    'port': '5432',
    'database': 'rssnews',
    'user': 'postgres',
    'password': 'postgres'
}

# Группируюем категории к единому виду
tag = {'Общество': ['Северо-Запад','Происшествия','Общество', 'Биографии и справки', 'Москва', 'Культура', 'Медиа',
                    'Бывший СССР', 'Среда обитания', 'Ценности', 'Забота о себе', 'Россия', 'Культура', 'Путешествия',
                    'Из жизни', 'Интернет и СМИ','Моя страна', 'Новости Урала', '69-я параллель', 'Московская область','В стране'],
       'Силовик': ['Армия и ОПК', 'Политика / Армия и спецслужбы', 'Силовые структуры'],
       'Политика': ['Политика'],
       'Экономика': ['Экономика и бизнес','Бизнес','Инвестиции / Рынки', 'Финансы', 'Экономика', 'Инвестиции',
                     'Инвестиции / Эмитенты', 'Недвижимость', 'Промышленность', 'Бизнес / ТЭК', 'Инвестиции / Инструменты',
                     'Инвестиции / Регулирование' ],
       'Спорт': ['Спорт'],
       'Наука и техника': ['Космос', 'Авто', 'Технологии', 'Наука и техника', 'Наука'],
       'Мир': ['Международная панорама', 'Политика / Международные новости', 'Мир'],
}

# Создание таблиц для нормализации базы

query = '''
CREATE TABLE IF NOT EXISTS category_groupe (
  id_category_groupe SERIAL PRIMARY KEY,
  category_groupe VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS subcategory (
  id_category_groupe INT,
  subcategory VARCHAR(50),
  FOREIGN KEY (id_category_groupe) REFERENCES category_groupe(id_category_groupe)
);

CREATE TABLE IF NOT EXISTS category (
  id_category SERIAL PRIMARY KEY,
  id_category_groupe INT,
  category VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS site (
  id_site SERIAL PRIMARY KEY,
  site_name VARCHAR(255),
  source VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS author (
  id_author SERIAL PRIMARY KEY,
  author VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS news (
  id_news_data SERIAL PRIMARY KEY,
  title VARCHAR(255),
  link VARCHAR(255),
  description TEXT,
  published TIMESTAMP
);

CREATE TABLE IF NOT EXISTS data_news (
  id SERIAL PRIMARY KEY,
  id_news_data INT,
  id_category INT,
  id_site INT,
  id_author INT,
  FOREIGN KEY (id_news_data) REFERENCES news(id_news_data),
  FOREIGN KEY (id_category) REFERENCES category(id_category),
  FOREIGN KEY (id_site) REFERENCES site(id_site),
  FOREIGN KEY (id_author) REFERENCES author(id_author)
);
'''

# Подключение к базе данных и выполнение запроса
conn = psycopg2.connect(**DB_CONNECTION)
cursor = conn.cursor()
cursor.execute(query)
conn.commit()
conn.close()

def get_category_groupe():
    conn = psycopg2.connect(**DB_CONNECTION)
    cursor = conn.cursor()

    for key, values in tag.items():

        cursor.execute("SELECT COUNT(*) FROM category_groupe WHERE category_groupe = %s", [key])
        result = cursor.fetchone()
        if result[0] == 0:

            insert_query = """
            INSERT INTO category_groupe (category_groupe)
            VALUES (%s);
            """
            cursor.execute(insert_query, (key,))

        cursor.execute("SELECT id_category_groupe FROM category_groupe WHERE category_groupe = %s", [key])
        id_category_groupe = cursor.fetchone()

        for value in values:
            cursor.execute("SELECT COUNT(*) FROM subcategory WHERE subcategory = %s", [value])
            result = cursor.fetchone()
            if result[0] == 0:
                insert_query_2 = """
                INSERT INTO subcategory (id_category_groupe, subcategory)
                VALUES (%s, %s);
                """
                cursor.execute(insert_query_2, (id_category_groupe, value,))

    #  Закрываем соединение с базой данных
    conn.commit()
    conn.close()

def get_news_text_tass(url):
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        try:
            paragraphs = soup.find_all('p', class_='Paragraph_paragraph__nYCys')
            title_lower = soup.find_all('div', class_='NewsHeader_lead__6Z9If')[0].get_text()
            news_text = ' '.join(paragraph.get_text() for paragraph in paragraphs)
            return title_lower + '\n' + news_text
        except Exception as e:
            print(f"Ошибка при получении текста новсти сайта ТАСС. Ошибка: {e}")
            return 'Нет текста.'
    else:
        print("Ошибка при получении страницы. Код состояния:", response.status_code)

def get_news_text_vedomosti(url):
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        paragraphs = soup.find_all('p', class_='box-paragraph__text')
        news_text = ' '.join(paragraph.get_text() for paragraph in paragraphs)
        return news_text.encode('utf-8').decode('utf-8')

    else:
        print("Ошибка при получении страницы. Код состояния:", response.status_code)

def get_site_name():
    conn = psycopg2.connect(**DB_CONNECTION)
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT site_name FROM rss_data")
    unique_site_name = cursor.fetchall()
    for site_name in unique_site_name:
        site_name = site_name[0]
        cursor.execute("SELECT source FROM rss_data WHERE site_name = %s", [site_name])
        result_source = cursor.fetchone()
        cursor.execute("SELECT COUNT(*) FROM site WHERE site_name = %s", [site_name])
        result = cursor.fetchone()
        if result[0] == 0:
            insert_query = "INSERT INTO site (site_name, source) VALUES (%s, %s);"
            cursor.execute(insert_query, (site_name, result_source[0]))
    conn.commit()
    conn.close()

def get_authors():
    conn = psycopg2.connect(**DB_CONNECTION)
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT author FROM rss_data")
    unique_authors = cursor.fetchall()
    for author in unique_authors:
        author = author[0]
        cursor.execute("SELECT COUNT(*) FROM author WHERE author = %s", [author])
        result = cursor.fetchone()
        if result[0] == 0:
            insert_query = "INSERT INTO author (author) VALUES (%s);"
            cursor.execute(insert_query, (author,))
    conn.commit()
    conn.close()

def get_category():
    conn = psycopg2.connect(**DB_CONNECTION)
    cursor = conn.cursor()

    # Выполняем SQL-запрос для выборки уникальных значений столбца "category"
    cursor.execute("SELECT DISTINCT category FROM rss_data")

    # Получаем все уникальные значения столбца "category"
    unique_category = cursor.fetchall()

    # Уникальные значения
    for category in unique_category:
        category = category[0]
        cursor.execute("SELECT COUNT(*) FROM category WHERE category = %s", [category])
        result = cursor.fetchone()
        if result[0] == 0:
            cursor.execute("SELECT id_category_groupe FROM subcategory WHERE subcategory = %s", [category])
            id_category_groupe = cursor.fetchone()

            insert_query = """
            INSERT INTO category (id_category_groupe, category)
            VALUES (%s, %s);
            """
            cursor.execute(insert_query, (id_category_groupe, category,))
    # проверяем и заполняем категории с NULL значениями id_category_groupe
    cursor.execute("SELECT * FROM category WHERE id_category_groupe IS NULL")
    nulls_category = cursor.fetchall()
    for row in nulls_category:
        id_category, id_category_groupe, null_category = row
        cursor.execute("SELECT id_category_groupe FROM subcategory WHERE subcategory = %s", [null_category])
        id_null_category_groupe = cursor.fetchone()
        insert_query_2 = """
        UPDATE category SET id_category_groupe = %s WHERE id_category = %s;
        """
        cursor.execute(insert_query_2, (id_null_category_groupe, id_category,))

    # Закрываем соединение с базой данных
    conn.commit()
    conn.close()

def extract_and_fill_news():
    conn = psycopg2.connect(**DB_CONNECTION)
    cursor = conn.cursor()
    cursor.execute("SELECT title, link, description, published FROM rss_data")
    rows = cursor.fetchall()
    for row in rows:
        title, link, description, published = row
        cursor.execute("SELECT COUNT(*) FROM news WHERE link = %s", (link,))
        count = cursor.fetchone()[0]
        if count == 0:
            if description is None:
                if 'vedomosti' in link:
                    description = get_news_text_vedomosti(link)
                elif 'tass' in link:
                    description = get_news_text_tass(link)
            print(description)
            insert_query = "INSERT INTO news (title, link, description, published) VALUES (%s, %s, %s, %s)"
            cursor.execute(insert_query, (title, link, description, published))
    conn.commit()
    conn.close()

def fill_data_news():
    conn = psycopg2.connect(**DB_CONNECTION)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT n.title, n.link, n.description, n.id_news_data, c.id_category, s.id_site, a.id_author
        FROM news n
        LEFT JOIN rss_data rd ON rd.link = n.link
        LEFT JOIN category c ON rd.category = c.category
        LEFT JOIN site s ON rd.site_name = s.site_name AND rd.source = s.source
        LEFT JOIN author a ON rd.author = a.author;
    """)
    rows = cursor.fetchall()
    for row in rows:
        title, link, description, id_news_data, category_id, site_id, author_id = row
        insert_query = "INSERT INTO data_news (id_news_data, id_category, id_site, id_author) VALUES (%s, %s, %s, %s)"
        cursor.execute(insert_query, (id_news_data, category_id, site_id, author_id))
    conn.commit()
    conn.close()

get_category_groupe_task = PythonOperator(
    task_id='get_category_groupe',
    python_callable=get_category_groupe,
    dag=dag,
)
get_site_name_task = PythonOperator(
    task_id='get_site_name',
    python_callable=get_site_name,
    dag=dag,
)

get_authors_task = PythonOperator(
    task_id='get_authors',
    python_callable=get_authors,
    dag=dag,
)

get_category_task = PythonOperator(
    task_id='get_category',
    python_callable=get_category,
    dag=dag,
)

extract_and_fill_news_task = PythonOperator(
    task_id='extract_and_fill_news',
    python_callable=extract_and_fill_news,
    dag=dag,
)

fill_data_news_task = PythonOperator(
    task_id='fill_data_news',
    python_callable=fill_data_news,
    dag=dag,
)

get_category_groupe_task >> get_site_name_task >> get_authors_task >> get_category_task >> extract_and_fill_news_task >> fill_data_news_task

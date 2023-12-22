from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime, timedelta

# Параметры подключения к базе данных PostgreSQL
DB_CONNECTION = {
    'host': 'db',
    'port': '5432',
    'database': 'rssnews',
    'user': 'postgres',
    'password': 'postgres'
}

default_args = {
    'owner': 'airflow',
    'start_date': datetime.utcnow(),
    'retries': 1,
}

dag = DAG(
    'example_sql_dag',
    default_args=default_args,
    schedule_interval='@daily',
)

# SQL-запрос, который вы хотите выполнить
sql_query_rss_news_analisis = """
CREATE OR REPLACE VIEW rss_news_analisis AS
WITH NewsCounts AS (
  SELECT
    cg.id_category_groupe,
    n.published::date AS publication_date,
    COUNT(DISTINCT dn.id_news_data) AS publications_count
  FROM
    category_groupe cg
  JOIN
    category c ON cg.id_category_groupe = c.id_category_groupe
  LEFT JOIN
    data_news dn ON c.id_category = dn.id_category
  LEFT JOIN
    news n ON dn.id_news_data = n.id_news_data
  GROUP BY
    cg.id_category_groupe, n.published::date
),
RankedDates AS (
  SELECT
    nc.id_category_groupe,
    nc.publication_date,
    RANK() OVER (PARTITION BY nc.id_category_groupe ORDER BY nc.publications_count DESC) AS rnk
  FROM
    NewsCounts nc
)
SELECT
  cg.id_category_groupe AS surrogate_key,
  cg.category_groupe AS category_name,
  s.site_name,
  nc.publication_date AS max_publications_date,
  SUM(COUNT(dn.id_news_data)) OVER (PARTITION BY cg.id_category_groupe) AS total_news_count,
  COUNT(dn.id_site) AS news_count_per_site,
  SUM(COUNT(DISTINCT CASE WHEN n.published >= NOW() - INTERVAL '1 day' THEN dn.id_news_data END)) OVER (PARTITION BY cg.id_category_groupe) AS total_news_count_last_day,
  COUNT(DISTINCT CASE WHEN n.published >= NOW() - INTERVAL '1 day' THEN dn.id_news_data END) AS news_count_last_day_per_site,
  ROUND(AVG(COUNT(DISTINCT dn.id_news_data)) OVER (PARTITION BY cg.id_category_groupe)) AS avg_publications_per_day
FROM
  category_groupe cg
JOIN
  category c ON cg.id_category_groupe = c.id_category_groupe
LEFT JOIN
  data_news dn ON c.id_category = dn.id_category
LEFT JOIN
  site s ON dn.id_site = s.id_site
LEFT JOIN
  news n ON dn.id_news_data = n.id_news_data
LEFT JOIN
  RankedDates nc ON cg.id_category_groupe = nc.id_category_groupe
WHERE
  nc.rnk = 1
GROUP BY
  cg.id_category_groupe, cg.category_groupe, s.site_name, nc.publication_date
ORDER BY
  surrogate_key, site_name;
"""

sql_query_day_of_week = """
CREATE OR REPLACE VIEW day_of_week AS
SELECT
  cg.id_category_groupe,
  cg.category_groupe,
  s.site_name,
  CASE EXTRACT(DOW FROM n.published)
    WHEN 0 THEN 'Воскресенье'
    WHEN 1 THEN 'Понедельник'
    WHEN 2 THEN 'Вторник'
    WHEN 3 THEN 'Среда'
    WHEN 4 THEN 'Четверг'
    WHEN 5 THEN 'Пятница'
    WHEN 6 THEN 'Суббота'
    ELSE 'Неизвестно'
  END AS day_of_week,
  COUNT(*) AS publication_count
FROM
  news n
JOIN
  data_news dn ON n.id_news_data = dn.id_news_data
JOIN
  category c ON dn.id_category = c.id_category
JOIN
  category_groupe cg ON c.id_category_groupe = cg.id_category_groupe
JOIN
  site s ON dn.id_site = s.id_site
GROUP BY
  cg.id_category_groupe, cg.category_groupe, s.site_name, day_of_week
ORDER BY
  cg.id_category_groupe, s.site_name, MIN(EXTRACT(DOW FROM n.published));
"""

# Оператор PostgresOperator для выполнения SQL-запроса
execute_sql = PostgresOperator(
    task_id='execute_sql_rss_news_analisis',
    sql=sql_query_rss_news_analisis,
    postgres_conn_id='sql_query',  # Идентификатор подключения к PostgreSQL в Airflow
    autocommit=True,  # Автозавершение транзакции
    dag=dag,
)
execute_sql = PostgresOperator(
    task_id='execute_sql_day_of_week',
    sql=sql_query_day_of_week,
    postgres_conn_id='sql_query',  # Идентификатор подключения к PostgreSQL в Airflow
    autocommit=True,  # Автозавершение транзакции
    dag=dag,
)

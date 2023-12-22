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
  SUM(COUNT(DISTINCT CASE WHEN n.published >= NOW() - INTERVAL '1 day' THEN dn.id_news_data END)) OVER (PARTITION BY cg.id_category_groupe) AS news_count_last_day,
  COUNT(DISTINCT CASE WHEN n.published >= NOW() - INTERVAL '1 day' THEN dn.id_news_data END) AS news_count_last_day,
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
  surrogate_key, site_name
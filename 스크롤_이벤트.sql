SELECT
    event_date,
    traffic_source.source as source,
    count(case when event_name="scroll" then 1 end) as scroll
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  where _TABLE_SUFFIX BETWEEN '20210117' AND '20210123'
  group by source
  having scroll > 0

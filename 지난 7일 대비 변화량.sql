with this_week as (
  SELECT
    traffic_source.source as source,
    count(case when event_name="scroll" then 1 end) as scroll
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  where _TABLE_SUFFIX BETWEEN '20210117' AND '20210123'
  group by source
  having scroll > 0
), last_week as (
  SELECT
    traffic_source.source as source,
    count(case when event_name="scroll" then 1 end) as scroll
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  where _TABLE_SUFFIX BETWEEN '20210110' AND '20210116'
  group by source
  having scroll > 0 
)

select
  last_week.source as last_week_source,
  this_week.source as this_week_source,
  last_week.scroll as last_week_scroll,
  this_week.scroll as this_week_scroll,
  CONCAT(CAST(round(((this_week.scroll - last_week.scroll)/last_week.scroll) * 100, 1) as string), "%") as Scroll_ROC
from last_week inner join this_week on last_week.source=this_week.source

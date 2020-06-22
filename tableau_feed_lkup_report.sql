DROP TABLE IF EXISTS saket.tableau_feed_insights_lu_tmp;
CREATE TABLE saket.tableau_feed_insights_lu_tmp
AS
SELECT DISTINCT
  CONCAT(CAST(a.miq_advertiser_id as string), '-', a.miq_advertiser_name) AS miq_advertiser_name,
  b.name as jarvis_campaign_name,
  a.geo_country,
  a.insertion_order_id,
  a.campaign_id,
  a.campaign_group_id,
  a.pixel_id,
  a.dayserial_numeric,
  a.miq_advertiser_id,
  a.jarvis_campaign_id
FROM saket.report_tableau_feed_cross_dsp a
  left join jarvis.campaign b on a.jarvis_campaign_id = b.id
where a.miq_advertiser_id is not NULL;

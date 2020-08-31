SET mapreduce.map.memory.mb = 3584;
SET mapreduce.reduce.memory.mb = 3584;
SET mapreduce.map.java.opts = -Xmx2867m;
SET mapreduce.reduce.java.opts = -Xmx2867m;
SET mapreduce.map.cpu.vcores = 2;
SET mapreduce.reduce.cpu.vcores = 2;



DROP TABLE if exists saket.tableau_feed_insights_tmp;
CREATE TABLE saket.tableau_feed_insights_tmp
AS
SELECT
  a.dt,
  a.timezone,
  case when a.weekday = 'Sun' then 'Sunday'
       when a.weekday = 'Mon' then 'Monday'
       when a.weekday = 'Tue' then 'Tuesday'
       when a.weekday = 'Wed' then 'Wednesday'
       when a.weekday = 'Thu' then 'Thursday'
       when a.weekday = 'Fri' then 'Friday'
       when a.weekday = 'Sat' then 'Saturday'
       else a.weekday
  end as weekday,
  a.day_hour,
  a.time_of_day,
  a.creative_area,
  a.geo_country,
  a.os_name,
  a.os_family,
  a.browser_name,
  a.advertiser_id,
  a.insertion_order_id,
  a.campaign_group_id,
  a.campaign_id,
  a.creative_id,
  a.pixel_id,
  a.device_model_name,
  a.device_make_name,
  a.device_type,
  a.supply_type,
  a.dayserial_numeric,
  a.homebiz_type,
  a.isp_name,
  a.imp,
  a.click,
  a.conversion,
  a.media_cost_dollars_cpm,
  b.miq_advertiser_id,
  b.miq_advertiser_name,
  b.agency_id,
  b.agency_name,
  b.campaign_id AS jarvis_campaign_id,
  b.dsp
FROM saket.tableau_feed_insights a
LEFT JOIN saket.combined_dsp_lookup b ON a.insertion_order_id = b.insertion_order_id and a.campaign_group_id = b.lineitem_id and a.dt between b.start_date and b.end_date;


DROP TABLE if exists saket.report_tableau_feed_cross_dsp;
CREATE TABLE saket.report_tableau_feed_cross_dsp
AS
SELECT
  advertiser_id AS dsp_advertiser_id,
  browser_name,
  campaign_group_id,
  campaign_id,
  creative_area,
  creative_id,
  day_hour,
  device_make_name,
  device_model_name,
  device_type,
  to_date(dt) AS dt,
  CAST(NULL AS STRING) AS dt_days,
  geo_country,
  homebiz_type,
  insertion_order_id,
  isp_name,
  CAST(NULL AS STRING) AS label,
  os_family,
  os_name,
  pixel_id,
  supply_type,
  time_of_day,
  timezone,
  weekday,
  SUM(imp) AS impressions,
  SUM(click) AS clicks,
  SUM(conversion) AS total_conversions,
  dayserial_numeric,
  SUM(media_cost_dollars_cpm) AS media_cost,
  COALESCE(miq_advertiser_id, -1) AS miq_advertiser_id,
  COALESCE(miq_advertiser_name, 'Unknown') AS miq_advertiser_name,
  agency_id,
  COALESCE(agency_name, 'Unknown') AS agency_name,
  COALESCE(jarvis_campaign_id, -1) AS jarvis_campaign_id,
  COALESCE(dsp, 'Unknown') AS dsp
FROM saket.tableau_feed_insights_tmp
GROUP BY
  advertiser_id,
  browser_name,
  campaign_group_id,
  campaign_id,
  creative_area,
  creative_id,
  day_hour,
  device_make_name,
  device_model_name,
  device_type,
  to_date(dt),
  geo_country,
  homebiz_type,
  insertion_order_id,
  isp_name,
  os_family,
  os_name,
  pixel_id,
  supply_type,
  time_of_day,
  timezone,
  weekday,
  dayserial_numeric,
  miq_advertiser_id,
  miq_advertiser_name,
  agency_id,
  agency_name,
  jarvis_campaign_id,
  dsp;

DROP TABLE if exists saket.dbm_feed_insights_tmp;

CREATE TABLE saket.dbm_feed_insights_tmp
AS
SELECT
  a.utc_dt,
  a.dt,
  a.country,
  a.country_name,
  a.timezone,
  a.adv_tz_offset,
  a.dayserial_numeric,
  a.advertiser_id,
  a.insertion_order_id,
  a.line_item_id,
  a.creative_id,
  a.creative_area,
  a.floodlight_id AS pixel_id,
  a.xchange,
  a.ad_position,
  a.geo_region_id,
  a.region_name,
  a.city_id,
  a.city_name,
  a.os_id,
  a.os_name,
  a.browser_id,
  a.browser_name,
  a.isp_id,
  a.isp_name,
  a.device_type,
  a.device_type_name,
  a.mobile_make_id,
  a.mobile_make_name,
  a.mobile_model_id,
  a.model_name,
  a.weekday,
  a.dayhour,
  a.time_of_day,
  a.impressions,
  a.clicks,
  a.conversions,
  a.buyer_spend,
  b.miq_advertiser_id,
  b.miq_advertiser_name,
  b.agency_id,
  b.agency_name,
  b.campaign_id AS jarvis_campaign_id,
  b.dsp
FROM saket.googledbm_insights a
LEFT JOIN saket.combined_dsp_lookup b ON a.insertion_order_id = b.insertion_order_id and a.line_item_id = b.placement_id and a.utc_dt between b.start_date and b.end_date;

INSERT INTO saket.report_tableau_feed_cross_dsp
SELECT
  advertiser_id AS dsp_advertiser_id,
  browser_name,
  line_item_id,
  NULL,
  creative_area,
  creative_id,
  dayhour,
  mobile_make_name,
  model_name,
  device_type,
  to_date(dt) AS dt,
  NULL,
  country,
  NULL,
  insertion_order_id,
  isp_name,
  NULL,
  NULL,
  os_name,
  pixel_id,
  NULL,
  time_of_day,
  timezone,
  weekday,
  SUM(impressions) AS impressions,
  SUM(clicks) AS clicks,
  SUM(conversions) AS total_conversions,
  dayserial_numeric,
  SUM(buyer_spend) AS media_cost,
  COALESCE(miq_advertiser_id, -1) AS miq_advertiser_id,
  COALESCE(miq_advertiser_name, 'Unknown') AS miq_advertiser_name,
  agency_id,
  COALESCE(agency_name, 'Unknown') AS agency_name,
  COALESCE(jarvis_campaign_id, -1) AS jarvis_campaign_id,
  COALESCE(dsp, 'Unknown') AS dsp
FROM saket.dbm_feed_insights_tmp
GROUP BY
  advertiser_id,
  browser_name,
  creative_area,
  creative_id,
  dayhour,
  mobile_make_name,
  model_name,
  device_type,
  to_date(dt),
  country,
  insertion_order_id,
  isp_name,
  os_name,
  pixel_id,
  time_of_day,
  timezone,
  weekday,
  dayserial_numeric,
  miq_advertiser_id,
  miq_advertiser_name,
  agency_id,
  line_item_id,
  agency_name,
  jarvis_campaign_id,
  dsp;

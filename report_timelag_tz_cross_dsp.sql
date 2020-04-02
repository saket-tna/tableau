Drop table if exists saket.report_timelag_tz;
CREATE EXTERNAL TABLE saket.report_timelag_tz(
  timezone                   string,
  utc_dt                     timestamp,
  dt                         timestamp,
  adv_TZ_OFFSET              double,
  tag_id                     int,
  width                      int,
  height                     int,
  geo_country                string,
  seller_member_id           int,
  sellername                 string,
  buyer_member_id            int,
  creative_id                int,
  creative_name              string,
  advertiser_id              int,
  advertiser_name            string,
  campaign_group_id          int,
  lineitemname               string,
  campaign_id                int,
  campaign_name              string,
  pcconversions              int,
  pvconversions              int,
  utc_imp_dt                 timestamp,
  imp_dt                     timestamp,
  pixel_id                   int,
  pixel_name                 string,
  imp_type                   int,
  publisher_id               int,
  insertion_order_id         int,
  insertion_order_name       string,
  operating_system           int,
  os_name                    string,
  os_family                  string,
  browser                    int,
  browser_name               string,
  LANGUAGE                   string,
  media_cost_dollars_cpm     string,
  site_domain                string,
  dayserial_numeric          int,
  time_lag_bucket            string,
  category                   string,
  subcategory                string,
  leaf_name                  string
)
PARTITIONED BY (day_numeric bigint) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE LOCATION 's3://dwh-reports-data/srs_reports/tableau_reports/report_timelag_tz/';

ALTER TABLE saket.report_timelag_tz ADD PARTITION (day_numeric = $DAYSERIAL_NUMERIC$);


DROP TABLE IF EXISTS saket.report_timelag_tz_tmp;
CREATE TABLE saket.report_timelag_tz_tmp
AS
SELECT
  a.timezone,
  a.utc_dt,
  a.dt,
  a.adv_TZ_OFFSET,
  a.tag_id,
  a.width,
  a.height,
  a.geo_country,
  a.seller_member_id,
  a.sellername,
  a.buyer_member_id,
  a.creative_id,
  a.creative_name,
  a.advertiser_id,
  a.advertiser_name,
  a.campaign_group_id,
  a.lineitemname,
  a.campaign_id,
  a.campaign_name,
  a.pcconversions,
  a.pvconversions,
  a.utc_imp_dt,
  a.imp_dt,
  a.pixel_id,
  a.pixel_name,
  a.imp_type,
  a.publisher_id,
  a.insertion_order_id,
  a.insertion_order_name,
  a.operating_system,
  a.os_name,
  a.os_family,
  a.browser,
  a.browser_name,
  a.LANGUAGE,
  CAST(a.media_cost_dollars_cpm as DOUBLE) AS media_cost_dollars_cpm,
  a.site_domain,
  a.dayserial_numeric,
  a.time_lag_bucket,
  a.category,
  a.subcategory,
  a.leaf_name,
  b.miq_advertiser_id,
  b.miq_advertiser_name,
  b.agency_id,
  b.agency_name,
  b.campaign_id AS jarvis_campaign_id,
  b.dsp
FROM saket.report_timelag_tz a
  LEFT JOIN saket.combined_dsp_lookup b ON a.insertion_order_id = b.insertion_order_id;


DROP TABLE IF EXISTS saket.report_timelag_tz_cross_dsp;
CREATE TABLE saket.report_timelag_tz_cross_dsp
AS
SELECT
  advertiser_id AS dsp_advertiser_id,
  advertiser_name,
  browser_name,
  buyer_member_id,
  campaign_group_id,
  campaign_id,
  campaign_name,
  category,
  creative_id,
  creative_name,
  TO_DATE(dt) AS dt,
  geo_country,
  TO_DATE(imp_dt) AS imp_dt,
  insertion_order_id,
  insertion_order_name,
  LANGUAGE,
  leaf_name,
  lineitemname,
  os_family,
  os_name,
  pixel_id,
  pixel_name,
  publisher_id,
  seller_member_id,
  sellername,
  site_domain,
  subcategory,
  tag_id,
  CAST(NULL AS STRING) AS time_lag,
  time_lag_bucket,
  timezone,
  TO_DATE(utc_dt) AS utc_dt,
  TO_DATE(utc_imp_dt) AS utc_imp_dt,
  adv_TZ_OFFSET,
  browser,
  SUM(pcconversions+pvconversions) AS conversions,
  dayserial_numeric,
  height,
  width,
  imp_type,
  SUM(media_cost_dollars_cpm) AS media_cost_dollars_cpm,
  operating_system,
  SUM(pcconversions) AS pcconversions,
  SUM(pvconversions) AS pvconversions,
  miq_advertiser_id,
  COALESCE(miq_advertiser_name, 'Unknown') AS miq_advertiser_name,
  agency_id,
  COALESCE(agency_name, 'Unknown') AS agency_name,
  COALESCE(jarvis_campaign_id, -1) AS jarvis_campaign_id,
  COALESCE(dsp, 'Unknown') AS dsp
FROM saket.report_timelag_tz_tmp
GROUP BY
  timezone,
  TO_DATE(utc_dt),
  TO_DATE(dt),
  adv_TZ_OFFSET,
  tag_id,
  width,
  height,
  geo_country,
  seller_member_id,
  sellername,
  buyer_member_id,
  creative_id,
  creative_name,
  advertiser_id,
  advertiser_name,
  campaign_group_id,
  lineitemname,
  campaign_id,
  campaign_name,
  TO_DATE(utc_imp_dt),
  TO_DATE(imp_dt),
  pixel_id,
  pixel_name,
  imp_type,
  publisher_id,
  insertion_order_id,
  insertion_order_name,
  operating_system,
  os_name,
  os_family,
  browser,
  browser_name,
  LANGUAGE,
  site_domain,
  dayserial_numeric,
  time_lag_bucket,
  category,
  subcategory,
  leaf_name,
  miq_advertiser_id,
  miq_advertiser_name,
  agency_id,
  agency_name,
  jarvis_campaign_id,
  dsp;

DROP TABLE IF EXISTS saket.report_timelag_dbm;
CREATE EXTERNAL TABLE saket.report_timelag_dbm(
  timezone              string,
  utc_dt                date,
  dt                    date,
  adv_TZ_OFFSET         int,
  country               string,
  country_name          string,
  geo_region_id         int,
  region_name           string,
  city_id               int,
  city_name             string,
  os_id                 int,
  os_name               string,
  browser_id            int,
  browser_name          string,
  language              string,
  xchange               int,
  xchange_name          string,
  site_domain           string,
  advertiser_id         int,
  advertiser_name       string,
  insertion_order_id    int,
  insertion_order_name  string,
  line_item_id          int,
  line_item_name        string,
  creative_id           int,
  creative_area         string,
  floodlight_id         int,
  floodlight_name       string,
  post_click_conv       int,
  post_view_conv        int,
  imp_utc_dt            date,
  imp_date              date,
  buyer_spend           double,
  time_lag_bucket       string,
  dayserial_numeric     int,
  category              string,
  subcategory           string
)
PARTITIONED BY (day_numeric bigint) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE LOCATION 's3://dwh-reports-data/srs_reports/dbm_reports/report_timelag_dbm/';

Alter table saket.report_timelag_dbm add partition (day_numeric = $DAYSERIAL_NUMERIC$);

DROP TABLE IF EXISTS saket.report_timelag_dbm_tmp;
CREATE TABLE saket.report_timelag_dbm_tmp
AS
SELECT
  a.timezone,
  a.utc_dt,
  a.dt,
  a.adv_TZ_OFFSET,
  a.country,
  a.country_name,
  a.geo_region_id,
  a.region_name,
  a.city_id,
  a.city_name,
  a.os_id,
  a.os_name,
  a.browser_id,
  a.browser_name,
  a.language,
  a.xchange,
  a.xchange_name,
  a.site_domain,
  a.advertiser_id,
  a.advertiser_name,
  a.insertion_order_id,
  a.insertion_order_name,
  a.line_item_id,
  a.line_item_name,
  a.creative_id,
  a.creative_area,
  a.floodlight_id as pixel_id,
  a.floodlight_name as pixel_name,
  a.post_click_conv,
  a.post_view_conv,
  a.imp_utc_dt,
  a.imp_date,
  a.buyer_spend,
  a.time_lag_bucket,
  a.dayserial_numeric,
  a.category,
  a.subcategory,
  b.miq_advertiser_id,
  b.miq_advertiser_name,
  b.agency_id,
  b.agency_name,
  b.campaign_id AS jarvis_campaign_id,
  b.dsp
FROM saket.report_timelag_dbm a
  LEFT JOIN saket.combined_dsp_lookup b ON a.insertion_order_id = b.insertion_order_id;

INSERT INTO saket.report_timelag_tz_cross_dsp
SELECT
  advertiser_id,
  advertiser_name,
  browser_name,
  NULL,
  NULL,
  NULL,
  NULL,
  category,
  creative_id,
  NULL,
  dt,
  country,
  imp_date,
  insertion_order_id,
  insertion_order_name,
  language,
  NULL,
  line_item_id,
  NULL,
  os_name,
  pixel_id,
  pixel_name,
  NULL,
  NULL,
  NULL,
  site_domain,
  subcategory,
  NULL,
  NULL,
  time_lag_bucket,
  timezone,
  utc_dt,
  imp_utc_dt,
  adv_TZ_OFFSET,
  browser_id,
  SUM(post_click_conv+post_view_conv) AS conversions,
  dayserial_numeric,
  NULL,
  NULL,
  NULL,
  SUM(buyer_spend) AS media_cost_dollars_cpm,
  NULL,
  SUM(post_click_conv) AS pcconversions,
  SUM(post_view_conv) AS pvconversions,
  miq_advertiser_id,
  COALESCE(miq_advertiser_name, 'Unknown') AS miq_advertiser_name,
  agency_id,
  COALESCE(agency_name, 'Unknown') AS agency_name,
  COALESCE(jarvis_campaign_id, -1) AS jarvis_campaign_id,
  COALESCE(dsp, 'Unknown') AS dsp
FROM saket.report_timelag_dbm_tmp
GROUP BY
  timezone,
  utc_dt,
  dt,
  adv_TZ_OFFSET,
  country,
  creative_id,
  advertiser_id,
  advertiser_name,
  line_item_id,
  imp_utc_dt,
  imp_date,
  insertion_order_id,
  insertion_order_name,
  os_name,
  pixel_id,
  pixel_name,
  browser_id,
  browser_name,
  language,
  site_domain,
  dayserial_numeric,
  time_lag_bucket,
  category,
  subcategory,
  miq_advertiser_id,
  miq_advertiser_name,
  agency_id,
  agency_name,
  jarvis_campaign_id,
  dsp;

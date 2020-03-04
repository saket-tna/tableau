Drop table if exists saket.tableau_geo_report;
Create EXTERNAL table saket.tableau_geo_report(
  UTC_dt string,
  geo_country string,
  de_country_name string,
  advertiser_id bigint,
  advertiser string,
  category string,
  subcategory string,
  agency string,
  insertion_order_id bigint,
  insertion_order string,
  campaign_group_id bigint,
  lineitem string,
  campaign_id bigint,
  campaign string,
  pixel_id bigint,
  pixel string,
  dayserial_numeric bigint,
  geo_region string,
  geo_region_name string,
  city_id string,
  city_name string,
  city_region string,
  GEO_DMA string,
  POSTAL_CODE string,
  lat string,
  lon string,
  imp int,
  click int,
  pc_conv int,
  pv_conv int,
  media_cost_dollars_cpm string,
  buyer_spend string
)
PARTITIONED BY (day_numeric bigint)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION 's3://dwh-reports-data/srs_reports/tableau_reports/tableau_geo_report/';

ALTER TABLE saket.tableau_geo_report ADD PARTITION (day_numeric= $DAYSERIAL_NUMERIC$);


DROP TABLE IF EXISTS saket.tableau_geo_report_tmp;
CREATE TABLE saket.tableau_geo_report_tmp
AS
SELECT
  a.UTC_dt,
  a.geo_country,
  a.de_country_name,
  a.advertiser_id,
  a.advertiser,
  a.category,
  a.subcategory,
  a.agency,
  a.insertion_order_id,
  a.insertion_order,
  a.campaign_group_id,
  a.lineitem,
  a.campaign_id,
  a.campaign,
  a.pixel_id,
  a.pixel,
  a.dayserial_numeric,
  a.geo_region,
  a.geo_region_name,
  a.city_id,
  a.city_name,
  a.city_region,
  a.GEO_DMA,
  a.POSTAL_CODE,
  a.lat,
  a.lon,
  a.imp,
  a.click,
  a.pc_conv,
  a.pv_conv,
  CAST(a.media_cost_dollars_cpm as DOUBLE) AS media_cost_dollars_cpm,
  CAST(a.buyer_spend as DOUBLE) AS buyer_spend,
  b.miq_advertiser_id,
  b.miq_advertiser_name,
  b.agency_id,
  b.agency_name,
  b.campaign_id AS jarvis_campaign_id,
  b.dsp
FROM saket.tableau_geo_report a
  LEFT JOIN bitanshu_adhoc.combined_dsp_lookup b ON a.insertion_order_id = b.insertion_order_id;

DROP TABLE IF EXISTS saket.report_geo_cross_dsp;
CREATE TABLE saket.report_geo_cross_dsp
AS
SELECT
  advertiser_id AS dsp_advertiser_id,
  advertiser,
  agency,
  campaign_group_id,
  campaign_id,
  campaign,
  category,
  city_id,
  city_name,
  city_region,
  de_country_name,
  CAST(NULL AS STRING) AS DMA,
  geo_country,
  GEO_DMA,
  geo_region,
  geo_region_name,
  insertion_order_id,
  insertion_order,
  lat,
  lineitem,
  lon,
  pixel_id,
  pixel,
  POSTAL_CODE,
  CAST(NULL AS STRING) AS Postcode,
  CAST(NULL AS STRING) AS State,
  subcategory,
  TO_DATE(UTC_dt) AS UTC_dt,
  SUM(buyer_spend) AS buyer_spend,
  SUM(click) AS clicks,
  SUM(pc_conv+pv_conv) AS conversions,
  dayserial_numeric,
  SUM(imp) AS impressions,
  CAST(NULL AS STRING) AS map_,
  SUM(media_cost_dollars_cpm) AS media_cost_dollars_cpm,
  SUM(pc_conv) AS pc_conversions,
  SUM(pv_conv) AS pv_conversions,
  miq_advertiser_id,
  miq_advertiser_name,
  agency_id,
  agency_name,
  jarvis_campaign_id,
  dsp
FROM saket.tableau_geo_report_tmp
GROUP BY
  TO_DATE(UTC_dt),
  geo_country,
  de_country_name,
  advertiser_id,
  advertiser,
  category,
  subcategory,
  agency,
  insertion_order_id,
  insertion_order,
  campaign_group_id,
  lineitem,
  campaign_id,
  campaign,
  pixel_id,
  pixel,
  dayserial_numeric,
  geo_region,
  geo_region_name,
  city_id,
  city_name,
  city_region,
  GEO_DMA,
  POSTAL_CODE,
  lat,
  lon,
  miq_advertiser_id,
  miq_advertiser_name,
  agency_id,
  agency_name,
  jarvis_campaign_id,
  dsp;

Drop table if exists saket.google_dbm_insights_geo;
CREATE EXTERNAL TABLE saket.google_dbm_insights_geo(
  utc_date date,
  dt date,
  country string,
  country_name string,
  timezone string,
  adv_tz_offset int,
  advertiser_id int,
  insertion_order_id int,
  line_item_id int,
  floodlight_id int,
  geo_region_id string,
  region_name string,
  city_id string,
  city_name string,
  dma_code string,
  postal_code string,
  lat string,
  lon string,
  impressions int,
  clicks int,
  conversions int,
  buyer_spend double
)
PARTITIONED BY (day_numeric bigint)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim'='\t',
  'serialization.format'='\t')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://dwh-reports-data/srs_reports/dbm_reports/tableau_dbm_insights_geo';

Alter table saket.google_dbm_insights_geo ADD PARTITION (day_numeric=$DAYSERIAL_NUMERIC$);


DROP TABLE IF EXISTS saket.google_dbm_insights_geo_tmp;
CREATE TABLE saket.google_dbm_insights_geo_tmp
AS
SELECT
  a.utc_date,
  a.dt,
  a.country,
  a.country_name,
  a.timezone,
  a.adv_tz_offset,
  a.advertiser_id,
  a.insertion_order_id,
  CAST(a.line_item_id AS STRING) AS line_item_id,
  a.floodlight_id as pixel_id,
  a.geo_region_id,
  a.region_name,
  a.city_id,
  a.city_name,
  a.dma_code,
  a.postal_code,
  a.lat,
  a.lon,
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
FROM saket.google_dbm_insights_geo a
  LEFT JOIN bitanshu_adhoc.combined_dsp_lookup b ON a.insertion_order_id=b.insertion_order_id;

INSERT INTO saket.report_geo_cross_dsp
SELECT
  advertiser_id,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  city_id,
  city_name,
  NULL,
  country_name,
  NULL,
  country,
  dma_code,
  geo_region_id,
  region_name,
  insertion_order_id,
  NULL,
  lat,
  line_item_id,
  lon,
  pixel_id,
  NULL,
  postal_code,
  NULL,
  NULL,
  NULL,
  utc_date,
  SUM(buyer_spend) AS buyer_spend,
  SUM(clicks) AS clicks,
  SUM(conversions) AS conversions,
  NULL,
  SUM(impressions) As impressions,
  NULL,
  NULL,
  NULL,
  NULL,
  miq_advertiser_id,
  miq_advertiser_name,
  agency_id,
  agency_name,
  jarvis_campaign_id,
  dsp
FROM saket.google_dbm_insights_geo_tmp
GROUP BY
  utc_date,
  country,
  country_name,
  advertiser_id,
  insertion_order_id,
  line_item_id,
  geo_region_id,
  region_name,
  city_id,
  city_name,
  dma_code,
  postal_code,
  lat,
  lon,
  pixel_id,
  miq_advertiser_id,
  miq_advertiser_name,
  agency_id,
  agency_name,
  jarvis_campaign_id,
  dsp;

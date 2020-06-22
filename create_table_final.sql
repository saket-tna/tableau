DROP TABLE IF EXISTS cross_dsp_insights_tableau.`report_geo_cross_dsp_campaign`;
CREATE EXTERNAL TABLE cross_dsp_insights_tableau.`report_geo_cross_dsp_campaign`(
  `dsp_advertiser_id` bigint,
  `advertiser` string,
  `agency` string,
  `campaign_group_id` bigint,
  `campaign_id` bigint,
  `campaign` string,
  `category` string,
  `city_id` string,
  `city_name` string,
  `city_region` string,
  `de_country_name` string,
  `dma` string,
  `geo_country` string,
  `geo_dma` string,
  `geo_region` string,
  `geo_region_name` string,
  `insertion_order_id` bigint,
  `insertion_order` string,
  `lat` string,
  `lineitem` string,
  `lon` string,
  `pixel_id` bigint,
  `pixel` string,
  `postal_code` string,
  `postcode` string,
  `state` string,
  `subcategory` string,
  `utc_dt` date,
  `buyer_spend` double,
  `clicks` bigint,
  `conversions` bigint,
  `impressions` bigint,
  `map_` string,
  `media_cost_dollars_cpm` double,
  `pc_conversions` bigint,
  `pv_conversions` bigint,
  `miq_advertiser_name` string,
  `agency_id` string,
  `agency_name` string,
  `dsp` string
)
PARTITIONED BY (
  `miq_advertiser_id` string,
  `jarvis_campaign_id` int,
  `dayserial_numeric` int)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://dwh-reports-data/tableau_cross_dsp_insights/report_geo';


DROP TABLE IF EXISTS cross_dsp_insights_tableau.`report_sd_insights_cross_dsp_campaign`;
CREATE EXTERNAL TABLE cross_dsp_insights_tableau.`report_sd_insights_cross_dsp_campaign`(
  `campaign_group_id` int,
  `campaign_id` int,
  `dt` date,
  `geo_country` string,
  `insertion_order_id` int,
  `pixel_id` int,
  `site_domain` string,
  `site_domain_category` string,
  `timezone` string,
  `clicks` bigint,
  `conversions` bigint,
  `impressions` bigint,
  `miq_advertiser_name` string,
  `agency_id` string,
  `agency_name` string,
  `dsp` string
)
PARTITIONED BY (
  `miq_advertiser_id` string,
  `jarvis_campaign_id` int,
  `dayserial_numeric` int)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://dwh-reports-data/tableau_cross_dsp_insights/report_sitedomain_insights';


DROP TABLE IF EXISTS cross_dsp_insights_tableau.`report_tableau_feed_cross_dsp_campaign`;
CREATE EXTERNAL TABLE cross_dsp_insights_tableau.`report_tableau_feed_cross_dsp_campaign`(
  `dsp_advertiser_id` int,
  `browser_name` string,
  `campaign_group_id` int,
  `campaign_id` int,
  `creative_area` string,
  `creative_id` int,
  `day_hour` int,
  `device_make_name` string,
  `device_model_name` string,
  `device_type` string,
  `dt` date,
  `dt_days` string,
  `geo_country` string,
  `homebiz_type` string,
  `insertion_order_id` int,
  `isp_name` string,
  `label` string,
  `os_family` string,
  `os_name` string,
  `pixel_id` int,
  `supply_type` string,
  `time_of_day` string,
  `timezone` string,
  `weekday` string,
  `impressions` bigint,
  `clicks` bigint,
  `total_conversions` bigint,
  `media_cost` double,
  `miq_advertiser_name` string,
  `agency_id` string,
  `agency_name` string,
  `dsp` string
)
PARTITIONED BY (
  `miq_advertiser_id` string,
  `jarvis_campaign_id` int,
  `dayserial_numeric` int)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://dwh-reports-data/tableau_cross_dsp_insights/tableau_feed_insights';


DROP TABLE IF EXISTS cross_dsp_insights_tableau.`report_timelag_tz_cross_dsp_campaign`;
CREATE EXTERNAL TABLE cross_dsp_insights_tableau.`report_timelag_tz_cross_dsp_campaign`(
  `dsp_advertiser_id` int,
  `advertiser_name` string,
  `browser_name` string,
  `buyer_member_id` int,
  `campaign_group_id` int,
  `campaign_id` int,
  `campaign_name` string,
  `category` string,
  `creative_id` int,
  `creative_name` string,
  `dt` date,
  `geo_country` string,
  `imp_dt` date,
  `insertion_order_id` int,
  `insertion_order_name` string,
  `language` string,
  `leaf_name` string,
  `lineitemname` string,
  `os_family` string,
  `os_name` string,
  `pixel_id` int,
  `pixel_name` string,
  `publisher_id` int,
  `seller_member_id` int,
  `sellername` string,
  `site_domain` string,
  `subcategory` string,
  `tag_id` int,
  `time_lag` string,
  `time_lag_bucket` string,
  `timezone` string,
  `utc_dt` date,
  `utc_imp_dt` date,
  `adv_tz_offset` double,
  `browser` int,
  `conversions` bigint,
  `dayserial_numeric` int,
  `height` int,
  `width` int,
  `imp_type` int,
  `media_cost_dollars_cpm` double,
  `operating_system` int,
  `pcconversions` bigint,
  `pvconversions` bigint,
  `miq_advertiser_name` string,
  `agency_id` string,
  `agency_name` string,
  `dsp` string
)
PARTITIONED BY (
  `miq_advertiser_id` string,
  `jarvis_campaign_id` int)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://dwh-reports-data/tableau_cross_dsp_insights/report_timelag_tz';


DROP TABLE IF EXISTS cross_dsp_insights_tableau.`report_url_keyword_cross_dsp_campaign`;
CREATE EXTERNAL TABLE cross_dsp_insights_tableau.`report_url_keyword_cross_dsp_campaign`(
  `advertiser_category` string,
  `dsp_advertiser_id` int,
  `advertiser_name` string,
  `advertiser_subcategory` string,
  `campaign_group_id` string,
  `campaign_id` int,
  `dt` date,
  `final_wl_bl` string,
  `geo_country` string,
  `insertion_order_id` int,
  `insertion_order_name` string,
  `lineitem_name` string,
  `pixel_id` int,
  `pixel_name` string,
  `placement_name` string,
  `site_domain` string,
  `site_domain_category` string,
  `site_domain_parse` string,
  `site_domain_subcategory` string,
  `week` string,
  `word` string,
  `clicks` bigint,
  `impressions` bigint,
  `media_cost` double,
  `pvconversions` bigint,
  `pcconversions` bigint,
  `monthname` string,
  `lineitem_id` int,
  `dsp_filter` string,
  `miq_advertiser_name` string,
  `agency_id` string,
  `agency_name` string,
  `dsp` string
)
PARTITIONED BY (
  `miq_advertiser_id` string,
  `jarvis_campaign_id` int,
  `dayserial_numeric` int)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://dwh-reports-data/tableau_cross_dsp_insights/url_keyword_sd';


DROP TABLE IF EXISTS cross_dsp_insights_tableau.tableau_feed_cross_dsp_lkup;
CREATE EXTERNAL TABLE cross_dsp_insights_tableau.tableau_feed_cross_dsp_lkup(
  miq_advertiser_name string,
  jarvis_campaign_name string,
  geo_country string,
  insertion_order_id int,
  campaign_id int,
  campaign_group_id int,
  pixel_id int,
  dayserial_numeric int
)PARTITIONED BY (
  miq_advertiser_id string,
  jarvis_campaign_id int)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://dwh-reports-data/tableau_cross_dsp_insights/tableau_feed_cross_dsp_lkup';

ALTER TABLE cross_dsp_insights_tableau.tableau_feed_cross_dsp_lkup RECOVER PARTITIONS;

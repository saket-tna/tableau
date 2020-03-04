use cross_dsp_insights_tableau;
CREATE EXTERNAL TABLE `report_timelag_tz_cross_dsp_agency`(
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
  `miq_advertiser_id` string,
  `miq_advertiser_name` string,
  `agency_name` string,
  `jarvis_campaign_id` string,
  `dsp` string
)
PARTITIONED BY (
  `agency_id` int)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://dwh-reports-data/saket/cross_dsp_reports/report_timelag_tz';

Alter table report_timelag_tz_cross_dsp_agency RECOVER PARTITIONS;

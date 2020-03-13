use cross_dsp_insights_tableau;
CREATE EXTERNAL TABLE `report_tableau_feed_cross_dsp_campaign`(
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
  `miq_advertiser_id` string,
  `miq_advertiser_name` string,
  `agency_id` string,
  `agency_name` string,
  `dsp` string
)
PARTITIONED BY (
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

Alter table report_tableau_feed_cross_dsp_campaign RECOVER PARTITIONS;

use cross_dsp_insights_tableau;
CREATE EXTERNAL TABLE `tableau_sd_insights_cross_dsp_agency`(
  `advertiser_id` int,
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
  `miq_advertiser_id` string,
  `miq_advertiser_name` string,
  `agency_name` string,
  `jarvis_campaign_id` string,
  `dsp` string
)
PARTITIONED BY (
  `agency_id` int,
  `dayserial_numeric` int)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://dwh-reports-data/saket/cross_dsp_reports/report_sitedomain_insights';


Alter table tableau_sd_insights_cross_dsp_agency RECOVER PARTITIONS;

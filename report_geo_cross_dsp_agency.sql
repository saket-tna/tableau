use cross_dsp_insights_tableau;
CREATE EXTERNAL TABLE `report_geo_cross_dsp_agency`(
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
  's3://dwh-reports-data/saket/cross_dsp_reports/report_geo';

Alter table report_geo_cross_dsp_agency RECOVER PARTITIONS;

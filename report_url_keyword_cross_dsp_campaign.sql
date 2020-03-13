use cross_dsp_insights_tableau;
CREATE EXTERNAL TABLE `report_url_keyword_cross_dsp_campaign`(
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
  's3://dwh-reports-data/tableau_cross_dsp_insights/url_keyword_sd';


Alter table report_url_keyword_cross_dsp_campaign RECOVER PARTITIONS;

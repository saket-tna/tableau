SET hive.exec.compress.output = TRUE;
SET mapred.output.compression.type = block;
SET mapred.output.compression.codec = org.apache.hadoop.io.compress.SnappyCodec;

CREATE EXTERNAL TABLE cross_dsp_insights_tableau.tableau_feed_cross_dsp_lkup(
  camp_id string,
  geo_country string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS PARQUET
LOCATION 's3://dwh-reports-data/tableau_cross_dsp_insights/tableau_feed_cross_dsp_lkup';

INSERT INTO cross_dsp_insights_tableau.tableau_feed_cross_dsp_lkup
SELECT
  CONCAT(CAST(jarvis_campaign_id as STRING),'-',miq_advertiser_name),
  geo_country
FROM cross_dsp_insights_tableau.report_tableau_feed_cross_dsp_campaign;

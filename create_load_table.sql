SET mapreduce.map.memory.mb = 3584;
SET mapreduce.reduce.memory.mb = 3584;
SET mapreduce.map.java.opts = -Xmx2867m;
SET mapreduce.reduce.java.opts = -Xmx2867m;
SET mapreduce.map.cpu.vcores = 2;
SET mapreduce.reduce.cpu.vcores = 2;

DROP TABLE if exists saket.tableau_feed_insights;

CREATE EXTERNAL TABLE saket.tableau_feed_insights (
  dt TIMESTAMP,
  timezone string,
  weekday string,
  day_hour INT,
  time_of_day string,
  creative_area string,
  geo_country string,
  os_name string,
  os_family string,
  browser_name string,
  advertiser_id INT,
  insertion_order_id INT,
  campaign_group_id INT,
  campaign_id INT,
  creative_id INT,
  pixel_id INT,
  device_model_name string,
  device_make_name string,
  device_type string,
  supply_type string,
  dayserial_numeric INT,
  homebiz_type string,
  isp_name string,
  imp INT,
  click INT,
  conversion INT,
  media_cost_dollars_cpm DOUBLE
)
PARTITIONED BY (day_numeric BIGINT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION 's3://dwh-reports-data/srs_reports/tableau_reports/tableau_feed_insights/';

DROP TABLE if exists saket.googledbm_insights;

CREATE EXTERNAL TABLE saket.googledbm_insights (
  utc_dt DATE,
  dt DATE,
  country string,
  country_name string,
  timezone string,
  adv_tz_offset INT,
  dayserial_numeric INT,
  advertiser_id INT,
  insertion_order_id INT,
  line_item_id INT,
  creative_id INT,
  creative_area string,
  floodlight_id INT,
  xchange INT,
  ad_position INT,
  geo_region_id INT,
  region_name string,
  city_id INT,
  city_name string,
  os_id INT,
  os_name string,
  browser_id INT,
  browser_name string,
  isp_id INT,
  isp_name string,
  device_type string,
  device_type_name string,
  mobile_make_id INT,
  mobile_make_name string,
  mobile_model_id INT,
  model_name string,
  weekday string,
  dayhour INT,
  time_of_day string,
  impressions INT,
  clicks INT,
  conversions INT,
  buyer_spend DOUBLE
)
PARTITIONED BY (day_numeric BIGINT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION 's3://dwh-reports-data/srs_reports/dbm_reports/tableau_insights_dbm/';

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
LOCATION 's3://dwh-reports-data/srs_reports/tableau_reports/tableau_geo_report/'; --check for 2020-09-19, 2020-09-16

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

drop table if exists saket.tableau_feed_sitedomain_insights;
create external table saket.tableau_feed_sitedomain_insights
(
  dt date,
  timezone string,
  geo_country string,
  site_domain string,
  site_domain_category string,
  advertiser_id int,
  insertion_order_id int,
  campaign_group_id int,
  campaign_id int,
  pixel_id int,
  dayserial_numeric int,
  imp int,
  click int,
  conversions int
)
partitioned by (day_numeric bigint) row format delimited fields terminated by '\t'
stored as textfile
location 's3://dwh-reports-data/srs_reports/tableau_reports/tableau_feed_sitedomain_insights/'; --check for 2020-09-15

drop table if exists saket.report_dbm_url_keword_wl;
create external table saket.report_dbm_url_keword_wl
(
  dayserial_numeric         int,
  dt                        date,
  monthname                 string,
  week                      string,
  geo_country               string,
  advertiser_id             int,
  advertiser_name           string,
  insertion_order_id        int,
  insertion_order_name      string,
  lineitem_id               int,
  lineitem_name             string,
  campaign_id               int,
  placement_name            string,
  pixel_id                  int,
  pixel_name                string,
  site_domain_parse         string,
  site_domain               string,
  site_domain_category      string,
  site_domain_subcategory   string,
  word                      string,
  imps                      int,
  clicks                    int,
  pv_convs                  int,
  pc_convs                  int,
  media_cost                double,
  advertiser_category       string,
  advertiser_subcategory    string,
  dsp_filter                string,
  final_wl_bl               string
)
partitioned by (day_numeric string) row format delimited fields terminated by '\t'
stored as textfile
location 's3://dwh-reports-data/srs_reports/dbm_reports/report_url_keyword_dbm_wl/';

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
PARTITIONED BY (day_numeric bigint)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION 's3://dwh-reports-data/srs_reports/tableau_reports/report_timelag_tz/';

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
PARTITIONED BY (day_numeric bigint)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION 's3://dwh-reports-data/srs_reports/dbm_reports/report_timelag_dbm/';

drop table if exists saket.report_url_keword_wl;
create external table saket.report_url_keword_wl(
  dayserial_numeric         int,
  dt                        date,
  monthname                 string,
  week                      string,
  geo_country               string,
  advertiser_id             int,
  advertiser_name           string,
  insertion_order_id        int,
  insertion_order_name      string,
  lineitem_id               int,
  lineitem_name             string,
  campaign_id               int,
  placement_name            string,
  pixel_id                  int,
  pixel_name                string,
  site_domain_parse         string,
  site_domain               string,
  site_domain_category      string,
  site_domain_subcategory   string,
  word                      string,
  imps                      int,
  clicks                    int,
  pv_convs                  int,
  pc_convs                  int,
  media_cost                double,
  advertiser_category       string,
  advertiser_subcategory    string,
  dsp_filter                string,
  final_wl_bl               string
)
partitioned by (day_numeric string) row format delimited fields terminated by '\t'
stored as textfile
location 's3://dwh-reports-data/srs_reports/daily_adsafe/report_sitedomain_keyword_wl/'; --check after 2020-09-15

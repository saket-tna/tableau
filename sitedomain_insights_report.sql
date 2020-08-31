SET mapreduce.map.memory.mb = 3584;
SET mapreduce.reduce.memory.mb = 3584;
SET mapreduce.map.java.opts = -Xmx2867m;
SET mapreduce.reduce.java.opts = -Xmx2867m;
SET mapreduce.map.cpu.vcores = 2;
SET mapreduce.reduce.cpu.vcores = 2;

drop table if exists saket.tableau_feed_sitedomain_insights_tmp;
create table saket.tableau_feed_sitedomain_insights_tmp
as
select
  a.advertiser_id,
  a.campaign_group_id,
  a.campaign_id,
  a.dt,
  a.geo_country,
  a.insertion_order_id,
  a.pixel_id,
  a.site_domain,
  a.site_domain_category,
  a.timezone,
  a.click,
  a.conversions,
  a.dayserial_numeric,
  a.imp,
  b.miq_advertiser_id,
  b.miq_advertiser_name,
  b.agency_id,
  b.agency_name,
  b.campaign_id AS jarvis_campaign_id,
  b.dsp
from saket.tableau_feed_sitedomain_insights a
LEFT JOIN saket.combined_dsp_lookup b ON a.insertion_order_id = b.insertion_order_id and a.campaign_group_id = b.lineitem_id and a.dt between b.start_date and b.end_date;


drop table if exists saket.report_sd_insights_cross_dsp;
create table saket.report_sd_insights_cross_dsp
as
select
  campaign_group_id,
  campaign_id,
  dt,
  geo_country,
  insertion_order_id,
  pixel_id,
  site_domain,
  site_domain_category,
  timezone,
  sum(click) as clicks,
  sum(conversions) as conversions,
  dayserial_numeric,
  sum(imp) as impressions,
  COALESCE(miq_advertiser_id, -1) AS miq_advertiser_id,
  COALESCE(miq_advertiser_name, 'Unknown') AS miq_advertiser_name,
  agency_id,
  COALESCE(agency_name, 'Unknown') AS agency_name,
  COALESCE(jarvis_campaign_id, -1) AS jarvis_campaign_id,
  COALESCE(dsp, 'Unknown') AS dsp
from saket.tableau_feed_sitedomain_insights_tmp
group by
  campaign_group_id,
  campaign_id,
  dt,
  geo_country,
  insertion_order_id,
  pixel_id,
  site_domain,
  site_domain_category,
  timezone,
  dayserial_numeric,
  miq_advertiser_id,
  miq_advertiser_name,
  agency_id,
  agency_name,
  jarvis_campaign_id,
  dsp;


drop table if exists saket.report_dbm_url_keword_wl_tmp;
create table saket.report_dbm_url_keword_wl_tmp
as
select
  a.dayserial_numeric,
  a.dt,
  a.monthname,
  a.week,
  a.geo_country,
  a.advertiser_id,
  a.advertiser_name,
  a.insertion_order_id,
  a.insertion_order_name,
  a.lineitem_id,
  a.lineitem_name,
  a.campaign_id,
  a.placement_name,
  a.pixel_id,
  a.pixel_name,
  a.site_domain_parse,
  a.site_domain,
  a.site_domain_category,
  a.site_domain_subcategory,
  a.imps,
  a.clicks,
  a.pv_convs,
  a.pc_convs,
  a.media_cost,
  a.advertiser_category,
  a.advertiser_subcategory,
  a.dsp_filter,
  a.final_wl_bl,
  b.miq_advertiser_id,
  b.miq_advertiser_name,
  b.agency_id,
  b.agency_name,
  b.campaign_id AS jarvis_campaign_id,
  b.dsp
from saket.report_dbm_url_keword_wl a
LEFT JOIN saket.combined_dsp_lookup b ON a.insertion_order_id = b.insertion_order_id and a.lineitem_id = b.placement_id and a.dt between b.start_date and b.end_date;

insert into saket.report_sd_insights_cross_dsp
select
  lineitem_id,
  campaign_id,
  dt,
  geo_country,
  insertion_order_id,
  pixel_id,
  site_domain,
  site_domain_category,
  NULL,
  sum(clicks) as clicks,
  sum(pc_convs+pv_convs) as conversions,
  dayserial_numeric,
  sum(imps) as impressions,
  COALESCE(miq_advertiser_id, -1) AS miq_advertiser_id,
  COALESCE(miq_advertiser_name, 'Unknown') AS miq_advertiser_name,
  agency_id,
  COALESCE(agency_name, 'Unknown') AS agency_name,
  COALESCE(jarvis_campaign_id, -1) AS jarvis_campaign_id,
  COALESCE(dsp, 'Unknown') AS dsp
from saket.report_dbm_url_keword_wl_tmp
group by
  lineitem_id,
  campaign_id,
  dt,
  geo_country,
  insertion_order_id,
  pixel_id,
  site_domain,
  site_domain_category,
  dayserial_numeric,
  miq_advertiser_id,
  miq_advertiser_name,
  agency_id,
  agency_name,
  jarvis_campaign_id,
  dsp;

SET mapreduce.map.memory.mb = 3584;
SET mapreduce.reduce.memory.mb = 3584;
SET mapreduce.map.java.opts = -Xmx2867m;
SET mapreduce.reduce.java.opts = -Xmx2867m;
SET mapreduce.map.cpu.vcores = 2;
SET mapreduce.reduce.cpu.vcores = 2;

-- drop table if exists saket.adv_lu;
-- create table saket.adv_lu
-- as
-- select distinct
--   miq_advertiser_id,
--   appnexus_advertiser_id as dsp_advertiser_id
-- from jarvis.advertiser_has_appnexus_advertiser_miq
-- union
-- select
--   a.miq_advertiser_id,
--   b.dbm_advertiser_id as dsp_advertiser_id
-- from jarvis.advertiser_has_dbm_advertiser_miq a
--   join  jarvis.dbm_timezone_advertiser b on a.id = b.id;

drop table if exists saket.pixel_lu;
create table saket.pixel_lu
as
select distinct
  id,
  name,
  "appnexus" as dsp
from jarvis.pixel_apn
union
select distinct
  dcm_floodlight_id as id,
  name,
  "doubleclick" as dsp
from jarvis.pixel_dbm;

drop table if exists saket.io_lu;
create table saket.io_lu
as
select distinct
  id,
  name,
  advertiser_id as dsp_advertiser_id,
  "appnexus" as dsp
from jarvis.insertion_order_apn
union
select distinct
  id,
  name,
  advertiser_id as dsp_advertiser_id,
  "doubleclick" as dsp
from jarvis.insertion_order_dbm;

drop table if exists saket.campaign_group_id_lu;
create table saket.campaign_group_id_lu
as
select distinct
  id,
  name,
  "appnexus" as dsp
from jarvis.line_item_apn
union
select distinct
  id,
  name,
  "doubleclick" as dsp
from jarvis.placement_dbm;


-- drop table if exists saket.tableau_feed_lkup_final;
-- create table saket.tableau_feed_lkup_final
-- as
-- select
--   a.*,
--   b.dsp_advertiser_id,
--   c.dsp
-- from cross_dsp_insights_tableau.tableau_feed_cross_dsp_lkup a
--   join saket.adv_lu b on a.miq_advertiser_id = b.miq_advertiser_id
--   join saket.io_lu c on b.dsp_advertiser_id = c.dsp_advertiser_id;


DROP TABLE IF EXISTS saket.tableau_feed_insights_lu_tmp;
CREATE TABLE saket.tableau_feed_insights_lu_tmp
AS
SELECT DISTINCT
  CONCAT(a.miq_advertiser_name, ' (', CAST(a.miq_advertiser_id as string), ')') AS miq_advertiser_name,
  CONCAT(b.name, ' (', CAST(a.jarvis_campaign_id as string), ')') AS jarvis_campaign_name,
  CONCAT(io.name, ' (', CAST(a.insertion_order_id as string), ')') AS insertion_order_name,
  CONCAT(cg.name, ' (', CAST(a.campaign_group_id as string), ')') AS strategy_name,
  CONCAT(p.name, ' (', CAST(a.pixel_id as string), ')') AS pixel_name,
  a.geo_country,
  a.campaign_id,
  a.dsp,
  a.dayserial_numeric,
  a.miq_advertiser_id,
  a.jarvis_campaign_id
FROM saket.report_tableau_feed_cross_dsp a
  join jarvis.campaign b on a.jarvis_campaign_id = b.id
  join saket.pixel_lu p on a.pixel_id = p.id and a.dsp = p.dsp
  join saket.io_lu io on a.insertion_order_id = io.id and a.dsp = io.dsp
  join saket.campaign_group_id_lu cg on a.campaign_group_id = cg.id and a.dsp = cg.dsp
where a.miq_advertiser_id != -1;

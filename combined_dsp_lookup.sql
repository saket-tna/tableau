SET mapreduce.map.memory.mb = 3584;
SET mapreduce.reduce.memory.mb = 3584;
SET mapreduce.map.java.opts = -Xmx2867m;
SET mapreduce.reduce.java.opts = -Xmx2867m;
SET mapreduce.map.cpu.vcores = 2;
SET mapreduce.reduce.cpu.vcores = 2;

Drop table if exists saket.dsp_dimension_tmp;
Create table saket.dsp_dimension_tmp
as
Select distinct
  io_id as insertion_order_id,
  lineitem_id,
  case when dsp = 'appnexus' then cast(NULL as string) else placement_id end as placement_id,
  dsp
from client_reporting_datastore.dsp_dimension;

Drop table if exists saket.campaign_tmp;
Create table saket.campaign_tmp
as
Select distinct
  id as campaign_id,
  miq_advertiser_id,
  opportunity_id,
  start_date,
  end_date
from jarvis.campaign;

Drop table if exists saket.campaign_io_association_tmp;
Create table saket.campaign_io_association_tmp
as
Select distinct
io_id as insertion_order_id,
campaign_id
from jarvis.campaign_io_association;

drop table if exists saket.opportunity_tmp;
create table saket.opportunity_tmp
as
select distinct
  id as opportunity_id,
  agency_id,
  agency_name
from jarvis.opportunity;

drop table if exists saket.combined_dsp_lookup;
create table saket.combined_dsp_lookup
as
select distinct
  a.insertion_order_id,
  a.lineitem_id,
  a.placement_id,
  a.dsp,
  b.campaign_id,
  c.miq_advertiser_id,
  c.start_date,
  c.end_date,
  d.name as miq_advertiser_name,
  e.agency_id,
  e.agency_name
from saket.dsp_dimension_tmp a
join saket.campaign_io_association_tmp b on a.insertion_order_id = b.insertion_order_id
join saket.campaign_tmp c on b.campaign_id = c.campaign_id
join jarvis.advertiser_miq d on c.miq_advertiser_id = d.id
left join saket.opportunity_tmp e on c.opportunity_id = e.opportunity_id;

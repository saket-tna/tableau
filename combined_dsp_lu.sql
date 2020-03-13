Drop table if exists saket.campaign_tmp;
Create table saket.campaign_tmp
as
Select distinct miq_advertiser_id as miq_advertiser_id,name as miq_advertiser_name, client_id as agency_id,id as campaign_id,io_id as insertion_order_id from jarvis.campaign;

Drop table if exists saket.client_miq;
Create table saket.client_miq
as
Select distinct id as agency_id,name as agency_name from jarvis.client_miq;

Drop table if exists saket.campaign_io_association_tmp;
Create table saket.campaign_io_association_tmp
as
Select distinct campaign_id as campaign_id,dsp,io_id as insertion_order_id from jarvis.campaign_io_association;

Drop table if exists saket.campaign_advertiser_agency;
Create table saket.campaign_advertiser_agency
as
Select a.miq_advertiser_id, a.miq_advertiser_name,a.agency_id,b.agency_name,a.campaign_id,a.insertion_order_id from saket.campaign_tmp a left join saket.client_miq b on a.agency_id=b.agency_id;

Drop table if exists saket.combined_dsp_lookup;
Create table saket.combined_dsp_lookup
as
Select a.miq_advertiser_id,a.miq_advertiser_name,a.agency_id,a.agency_name,a.campaign_id,b.insertion_order_id,b.dsp from saket.campaign_advertiser_agency a join saket.campaign_io_association_tmp b on a.campaign_id = b.campaign_id ;

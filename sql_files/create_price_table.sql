create or replace table lakehouse_development.ml_features.analysis_job_lead_prices
as
select
    from_utc_timestamp (a.job_lead_created_timestamp, 'Australia/Sydney') as lead_created_timestamp,
    from_utc_timestamp (j.__dl_updated_ts, 'Australia/Sydney') as job_updated_timestamp,
    a.lead_id,
    a.job_id,
    rtm.is_test_region_level,
    b.master_category,
    rlm.state_code,
    a.job_lead_price,
    a.job_lead_claimed,
    case
        when a.job_lead_round_type = 'first-round'
        and job_lead_invitation_round = 0 then 1
        else 0
    end as is_first_round_first_batch,
    case
        when a.job_lead_round_type = 'first-round'
        and job_lead_invitation_round <> 0 then 1
        else 0
    end as is_first_round_other_batches,
    case
        when a.job_lead_round_type = 're-invitation' then 1
        else 0
    end as is_reinvitation_round,
    case
        when a.job_lead_round_type not in ('re-invitation', 'first-round') then 1
        else 0
    end as is_other_rounds
from
    lakehouse_production.gold.lead_management__fact_job_leads_enriched as a
    left join hive_metastore.long_lake.dim_category_databricks as b on a.job_category_id = b.category_dim_key
    inner join lakehouse_production.gold.jobs__fact_jobs_validated as j on a.job_id = j.job_id
    inner join lakehouse_production.ml_features.int_regionid_statecode_mapping__location as rlm on j.job_suburb_id = rlm.suburb_id
    inner join lakehouse_production.ml_features_location_static.regionid_test_mapping as rtm on rtm.region_id = rlm.region_id
where
    a.job_lead_created_date_dim_key > 20250101
    and a.job_lead_claimed = true
    and b.master_category in (
        'Insulation',
        'Interior Design',
        'Staircases',
        'Security & Home Entertainment',
        'Pest Control',
        'Stonemasonry',
        'Locksmithing',
        'Fireplaces',
        'Furniture Removal',
        'Demolition & Asbestos Removal',
        'Pool Building & Maintainance',
        'Outdoor Constructions',
        'Appliances & Climate Control'
    )
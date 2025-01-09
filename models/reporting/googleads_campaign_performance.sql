{{ config (
    alias = target.database + '_googleads_campaign_performance'
)}}

SELECT 
account_id,
campaign_name,
campaign_id,
campaign_status,
campaign_type_default,
date,
date_granularity,
spend,
impressions,
clicks,
conversions as purchases,
conversions_value as revenue,
search_impression_share,
search_budget_lost_impression_share,
search_rank_lost_impression_share,
CASE WHEN date < '2024-12-16' THEN "ga4booknowevolvemedspacomwebbooking_completed"
    WHEN date >= '2024-12-16' THEN "evolvemedspazenoticomwebservice_completed" 
END as appointments_scheduled
FROM {{ ref('googleads_performance_by_campaign') }}

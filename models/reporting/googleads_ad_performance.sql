{{ config (
    alias = target.database + '_googleads_ad_performance'
)}}

SELECT
account_id,
ad_id,
campaign_name,
campaign_id,
campaign_status,
campaign_type_default,
ad_group_name,
ad_group_id,
date,
date_granularity,
spend,
impressions,
clicks,
conversions as purchases,
conversions_value as revenue,
CASE WHEN date < '2024-12-16' THEN "ga4booknowevolvemedspacomwebbooking_completed"
    WHEN date >= '2024-12-16' THEN "evolvemedspazenoticomwebservice_completed" 
END as appointments_scheduled
FROM {{ ref('googleads_performance_by_ad') }}

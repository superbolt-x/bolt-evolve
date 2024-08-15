{{ config (
    alias = target.database + '_facebook_ad_performance'
)}}

SELECT 
campaign_name,
campaign_id,
campaign_effective_status,
campaign_type_default,
adset_name,
adset_id,
adset_effective_status,
audience,
SPLIT_PART(adset_name,' - ',1) AS location,
ad_name,
ad_id,
ad_effective_status,
visual,
copy,
format_visual,
visual_copy,
SPLIT_PART(ad_name,' - ',2) AS service,
date,
date_granularity,
spend,
impressions,
link_clicks,
onfacebook_leads+website_leads as leads
FROM {{ ref('facebook_performance_by_ad') }}

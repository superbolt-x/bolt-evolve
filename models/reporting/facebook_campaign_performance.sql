{{ config (
    alias = target.database + '_facebook_campaign_performance'
)}}

SELECT 
campaign_name,
campaign_id,
campaign_effective_status,
campaign_type_default,
date,
date_granularity,
spend,
impressions,
link_clicks,
onfacebook_leads+website_leads as leads,
schedule_total as appointments_scheduled
FROM {{ ref('facebook_performance_by_campaign') }}

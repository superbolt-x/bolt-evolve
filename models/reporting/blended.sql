{{ config (
    alias = target.database + '_blended'
)}}

SELECT 
'Facebook' as channel,
campaign_name,
campaign_type_default,
adset_name as ad_group_name,
ad_name,
date,
date_granularity,
spend,
impressions,
link_clicks as clicks,
purchases,
revenue
from {{ source('reporting','facebook_ad_performance') }}

UNION ALL 

SELECT
'Google' as channel,
campaign_name,
campaign_type_default,
ad_group_name,
ad_name,
date,
date_granularity,
spend,
impressions,
clicks,
purchases,
revenue
from {{ source('reporting','googleads_ad_performance') }}

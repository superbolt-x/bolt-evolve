{{ config (
    alias = target.database + '_blended'
)}}

{% set date_granularity_list = ['day', 'week', 'month', 'quarter', 'year'] %}

WITH initial_bookings_data as 
    (SELECT *, 
        CASE WHEN url ~* 'googledirect' THEN SPLIT_PART(SPLIT_PART(url,'campaignid=',2),'&',1) WHEN url ~* 'facebookdirect' THEN SPLIT_PART(SPLIT_PART(url,'campaign_id=',2),'&',1) END as campaign_id, 
        SPLIT_PART(SPLIT_PART(url,'adgroupid=',2),'&',1) as ad_group_id,
        SPLIT_PART(SPLIT_PART(url,'adid=',2),'&',1) as ad_id, 
        {{ get_date_parts('date_created') }} 
    FROM {{ source('gsheet_raw','bookings_completed') }} ),

bookings_data as
    ({%- for date_granularity in date_granularity_list %}    
    SELECT '{{date_granularity}}' as date_granularity, {{date_granularity}} as date,
        CASE WHEN url ~* 'facebookdirect' THEN 'Facebook' WHEN url ~* 'googledirect' THEN 'Google' ELSE 'Other' END as channel, 
        campaign_id, ad_group_id, ad_id,
        COUNT(DISTINCT "order") as bookings_completed, COALESCE(SUM(subtotal),0) as subtotal_sales, COALESCE(SUM(total),0) as total_sales
    FROM initial_bookings_data
    WHERE state ~* 'final'
    GROUP BY 1,2,3,4,5,6),

initial_leads_data as 
    (SELECT *, {{ get_date_parts('created_on') }} FROM {{ source('gsheet_raw','crm_leads') }}),

leads_data as
    ({%- for date_granularity in date_granularity_list %}    
    SELECT '{{date_granularity}}' as date_granularity, {{date_granularity}} as date, 
        CASE WHEN source ~* 'facebook' THEN 'Facebook' WHEN source ~* 'google' THEN 'Google' ELSE 'Other' END as channel, 
        SPLIT_PART(utm_medium,' - ',1) as location, utm_campaign as campaign_name, utm_medium as ad_group_name,
        COUNT(opportunity_name) as leads
    FROM initial_leads_data
    GROUP BY 1,2,3,4,5,6),
    
fb_data as 
    (SELECT 'Facebook' as channel, campaign_name, campaign_type, ad_group_name, location, date, date_granularity, 
        COALESCE(SUM(spend),0) as spend, COALESCE(SUM(impressions),0) as impressions, COALESCE(SUM(clicks),0) as clicks, 
        COALESCE(SUM(bookings_completed),0) as bookings_completed, COALESCE(SUM(leads),0) as leads
    FROM
        (SELECT campaign_name, 
            CASE WHEN campaign_name ~* 'Prospecting' THEN 'Prospecting' WHEN campaign_name ~* 'Retargeting' THEN 'Retargeting' END as campaign_type,
            adset_name as ad_group_name, location, date, date_granularity, 
            spend, impressions, link_clicks as clicks, 0 as bookings_completed, 0 as leads
        FROM {{ source('reporting','facebook_ad_performance') }}
        UNION ALL
        SELECT campaign_name, 
            CASE WHEN campaign_name ~* 'Prospecting' THEN 'Prospecting' WHEN campaign_name ~* 'Retargeting' THEN 'Retargeting' END as campaign_type,
            ad_group_name, SPLIT_PART(ad_group_name,' - ',1) as location, date, date_granularity,
            0 as spend, 0 as impressions, 0 as clicks, COALESCE(SUM(bookings_completed),0) as bookings_completed, 0 as leads
        FROM bookings_data
        LEFT JOIN (SELECT DISTINCT campaign_id::text, campaign_name, adset_id::text as ad_group_id, adset_name as ad_group_name, ad_id::text FROM {{ source('reporting','facebook_ad_performance') }}) USING(campaign_id,ad_id)   
        WHERE channel = 'Facebook'
        GROUP BY 1,2,3,4,5,6
        UNION ALL
        SELECT campaign_name, 
            CASE WHEN campaign_name ~* 'Prospecting' THEN 'Prospecting' WHEN campaign_name ~* 'Retargeting' THEN 'Retargeting' END as campaign_type,
            ad_group_name, location, date, date_granularity,
            0 as spend, 0 as impressions, 0 as clicks, 0 as bookings_completed, COALESCE(SUM(leads),0) as leads
        FROM leads_data
        GROUP BY 1,2,3,4,5,6
        )
    GROUP BY 1,2,3,4,5,6,7),

adw_data as 
    (SELECT 'Google' as channel, campaign_name, campaign_type, ad_group_name, location, date, date_granularity, 
        COALESCE(SUM(spend),0) as spend, COALESCE(SUM(impressions),0) as impressions, COALESCE(SUM(clicks),0) as clicks, 
        COALESCE(SUM(bookings_completed),0) as bookings_completed, COALESCE(SUM(leads),0) as leads
    FROM
        (SELECT campaign_name, 
            CASE WHEN campaign_name ~* 'Search - Branded' THEN 'Search - Branded' WHEN campaign_name ~* 'Search - Non Brand' THEN 'Search - Non Brand' WHEN campaign_name ~* 'PMax' THEN 'PMax' END as campaign_type,
            ad_group_name, location, date, date_granularity, 
            spend, impressions, clicks, 0 as bookings_completed, 0 as leads
        FROM {{ source('reporting','googleads_ad_performance') }}
        UNION ALL
        SELECT campaign_name, 
            CASE WHEN campaign_name ~* 'Search - Branded' THEN 'Search - Branded' WHEN campaign_name ~* 'Search - Non Brand' THEN 'Search - Non Brand' WHEN campaign_name ~* 'PMax' THEN 'PMax' END as campaign_type,
            ad_group_name, SPLIT_PART(ad_group_name,' - ',1) as location, date, date_granularity,
            0 as spend, 0 as impressions, 0 as clicks, COALESCE(SUM(bookings_completed),0) as bookings_completed, 0 as leads
        FROM bookings_data
        LEFT JOIN (SELECT DISTINCT campaign_id::text, campaign_name, ad_group_id::text, ad_group_name FROM {{ source('reporting','googleads_ad_performance') }}) USING(campaign_id,ad_group_id)   
        WHERE channel = 'Google'
        GROUP BY 1,2,3,4,5,6
        UNION ALL
        SELECT campaign_name, 
            CASE WHEN campaign_name ~* 'Search - Branded' THEN 'Search - Branded' WHEN campaign_name ~* 'Search - Non Brand' THEN 'Search - Non Brand' WHEN campaign_name ~* 'PMax' THEN 'PMax' END as campaign_type,
            ad_group_name, location, date, date_granularity,
            0 as spend, 0 as impressions, 0 as clicks, 0 as bookings_completed, COALESCE(SUM(leads),0) as leads
        FROM leads_data
        GROUP BY 1,2,3,4,5,6
        )
    GROUP BY 1,2,3,4,5,6,7)

SELECT channel, campaign_name, campaign_type, ad_group_name, location, date, date_granularity, spend, impressions, clicks, bookings_completed, leads
FROM 
    (SELECT * FROM fb_data
    UNION ALL 
    SELECT * FROM adw_data)

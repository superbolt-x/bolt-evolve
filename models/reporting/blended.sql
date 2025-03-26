{{ config (
    alias = target.database + '_blended'
)}}

WITH initial_bookings_data as 
    (SELECT *, 
        CASE WHEN url ~* 'googledirect' AND url ~* 'campaignid=' THEN SPLIT_PART(SPLIT_PART(url,'campaignid=',2),'&',1) 
            WHEN url ~* 'googledirect' AND url !~* 'campaignid=' THEN SPLIT_PART(SPLIT_PART(url,'utm_campaign=',2),'&',1) 
            WHEN url ~* 'facebookdirect' THEN SPLIT_PART(SPLIT_PART(url,'campaign_id=',2),'&',1) 
        END as campaign_id, 
        CASE WHEN url ~* 'googledirect' AND url ~* 'adgroupid=' THEN SPLIT_PART(SPLIT_PART(url,'adgroupid=',2),'&',1) 
            WHEN url ~* 'googledirect' AND url !~* 'adgroupid=' THEN SPLIT_PART(SPLIT_PART(url,'utm_adgroup=',2),'&',1) 
        END as ad_group_id,
        SPLIT_PART(SPLIT_PART(url,'ad_id=',2),'&',1) as ad_id
    FROM {{ source('gsheet_raw','bookings_completed') }} ),

bookings_data as
    (SELECT 'day' as date_granularity, DATE_TRUNC('day',date_completed::date) as date,
        CASE WHEN url ~* 'facebookdirect' THEN 'Facebook' WHEN url ~* 'googledirect' THEN 'Google' ELSE 'Other' END as channel, 
        campaign_id, ad_group_id, ad_id,
        COUNT(DISTINCT "order") as bookings_completed, COALESCE(SUM(subtotal),0) as subtotal_sales, COALESCE(SUM(total),0) as total_sales
    FROM initial_bookings_data
    WHERE state ~* 'final'
    GROUP BY 1,2,3,4,5,6
    UNION ALL
    SELECT 'week' as date_granularity, DATE_TRUNC('week',date_completed::date) as date,
        CASE WHEN url ~* 'facebookdirect' THEN 'Facebook' WHEN url ~* 'googledirect' THEN 'Google' ELSE 'Other' END as channel, 
        campaign_id, ad_group_id, ad_id,
        COUNT(DISTINCT "order") as bookings_completed, COALESCE(SUM(subtotal),0) as subtotal_sales, COALESCE(SUM(total),0) as total_sales
    FROM initial_bookings_data
    WHERE state ~* 'final'
    GROUP BY 1,2,3,4,5,6
    UNION ALL
    SELECT 'month' as date_granularity, DATE_TRUNC('month',date_completed::date) as date,
        CASE WHEN url ~* 'facebookdirect' THEN 'Facebook' WHEN url ~* 'googledirect' THEN 'Google' ELSE 'Other' END as channel, 
        campaign_id, ad_group_id, ad_id,
        COUNT(DISTINCT "order") as bookings_completed, COALESCE(SUM(subtotal),0) as subtotal_sales, COALESCE(SUM(total),0) as total_sales
    FROM initial_bookings_data
    WHERE state ~* 'final'
    GROUP BY 1,2,3,4,5,6
    UNION ALL
    SELECT 'quarter' as date_granularity, DATE_TRUNC('quarter',date_completed::date) as date,
        CASE WHEN url ~* 'facebookdirect' THEN 'Facebook' WHEN url ~* 'googledirect' THEN 'Google' ELSE 'Other' END as channel, 
        campaign_id, ad_group_id, ad_id,
        COUNT(DISTINCT "order") as bookings_completed, COALESCE(SUM(subtotal),0) as subtotal_sales, COALESCE(SUM(total),0) as total_sales
    FROM initial_bookings_data
    WHERE state ~* 'final'
    GROUP BY 1,2,3,4,5,6
    UNION ALL
    SELECT 'year' as date_granularity, DATE_TRUNC('year',date_completed::date) as date,
        CASE WHEN url ~* 'facebookdirect' THEN 'Facebook' WHEN url ~* 'googledirect' THEN 'Google' ELSE 'Other' END as channel, 
        campaign_id, ad_group_id, ad_id,
        COUNT(DISTINCT "order") as bookings_completed, COALESCE(SUM(subtotal),0) as subtotal_sales, COALESCE(SUM(total),0) as total_sales
    FROM initial_bookings_data
    WHERE state ~* 'final'
    GROUP BY 1,2,3,4,5,6),

leads_data as
    (SELECT 'day' as date_granularity, DATE_TRUNC('day',created_on::date) as date,
        CASE WHEN source ~* 'facebook' THEN 'Facebook' WHEN source ~* 'google' THEN 'Google' ELSE 'Other' END as channel, 
        CASE WHEN utm_medium ~* 'SB' THEN SPLIT_PART(utm_medium,' - ',2) WHEN utm_medium !~* 'SB' THEN SPLIT_PART(utm_medium,' - ',1) END as location,
        utm_campaign as campaign_name, utm_medium as ad_group_name,
        CASE WHEN utm_content ~* 'lip flip' THEN 'Lip Flip'
            WHEN utm_content ~* 'hydrafacial' THEN 'Hydrafacial'
            WHEN utm_content ~* 'diamondglow' THEN 'Diamondglow'
            WHEN utm_content ~* 'LHR' THEN 'LHR'
            WHEN utm_content ~* 'Botox' THEN 'Botox'
            WHEN utm_content ~* 'Facial Filler' THEN 'Facial Filler'
            WHEN utm_content ~* 'Lip Filler' THEN 'Lip Filler'
            WHEN utm_content ~* 'Microneedling' THEN 'Microneedling'
            WHEN utm_content ~* 'coolsculpting' OR utm_content ~* 'emsculpt' OR utm_content ~* 'body contouring' THEN 'Body Contouring'
            ELSE 'Others'
        END AS service,
        COUNT(opportunity_name) as leads,
        SUM(CASE WHEN stage = 'Replied' THEN 1 ELSE 0 END) as replied,
        SUM(CASE WHEN stage = 'Cold' THEN 1 ELSE 0 END) as cold,
        SUM(CASE WHEN stage = 'Booked' THEN 1 ELSE 0 END) as booked,
        SUM(CASE WHEN stage = 'Patient' THEN 1 ELSE 0 END) as patient
    FROM {{ source('gsheet_raw','crm_leads') }}
    GROUP BY 1,2,3,4,5,6,7
    UNION ALL
    SELECT 'week' as date_granularity, DATE_TRUNC('week',created_on::date) as date,
        CASE WHEN source ~* 'facebook' THEN 'Facebook' WHEN source ~* 'google' THEN 'Google' ELSE 'Other' END as channel, 
        CASE WHEN utm_medium ~* 'SB' THEN SPLIT_PART(utm_medium,' - ',2) WHEN utm_medium !~* 'SB' THEN SPLIT_PART(utm_medium,' - ',1) END as location,
        utm_campaign as campaign_name, utm_medium as ad_group_name,
        CASE WHEN utm_content ~* 'lip flip' THEN 'Lip Flip'
            WHEN utm_content ~* 'hydrafacial' THEN 'Hydrafacial'
            WHEN utm_content ~* 'diamondglow' THEN 'Diamondglow'
            WHEN utm_content ~* 'LHR' THEN 'LHR'
            WHEN utm_content ~* 'Botox' THEN 'Botox'
            WHEN utm_content ~* 'Facial Filler' THEN 'Facial Filler'
            WHEN utm_content ~* 'Lip Filler' THEN 'Lip Filler'
            WHEN utm_content ~* 'Microneedling' THEN 'Microneedling'
            WHEN utm_content ~* 'coolsculpting' OR utm_content ~* 'emsculpt' OR utm_content ~* 'body contouring' THEN 'Body Contouring'
            ELSE 'Others'
        END AS service,
        COUNT(opportunity_name) as leads,
        SUM(CASE WHEN stage = 'Replied' THEN 1 ELSE 0 END) as replied,
        SUM(CASE WHEN stage = 'Cold' THEN 1 ELSE 0 END) as cold,
        SUM(CASE WHEN stage = 'Booked' THEN 1 ELSE 0 END) as booked,
        SUM(CASE WHEN stage = 'Patient' THEN 1 ELSE 0 END) as patient
    FROM {{ source('gsheet_raw','crm_leads') }}
    GROUP BY 1,2,3,4,5,6,7
    UNION ALL
    SELECT 'month' as date_granularity, DATE_TRUNC('month',created_on::date) as date,
        CASE WHEN source ~* 'facebook' THEN 'Facebook' WHEN source ~* 'google' THEN 'Google' ELSE 'Other' END as channel, 
        CASE WHEN utm_medium ~* 'SB' THEN SPLIT_PART(utm_medium,' - ',2) WHEN utm_medium !~* 'SB' THEN SPLIT_PART(utm_medium,' - ',1) END as location,
        utm_campaign as campaign_name, utm_medium as ad_group_name,
        CASE WHEN utm_content ~* 'lip flip' THEN 'Lip Flip'
            WHEN utm_content ~* 'hydrafacial' THEN 'Hydrafacial'
            WHEN utm_content ~* 'diamondglow' THEN 'Diamondglow'
            WHEN utm_content ~* 'LHR' THEN 'LHR'
            WHEN utm_content ~* 'Botox' THEN 'Botox'
            WHEN utm_content ~* 'Facial Filler' THEN 'Facial Filler'
            WHEN utm_content ~* 'Lip Filler' THEN 'Lip Filler'
            WHEN utm_content ~* 'Microneedling' THEN 'Microneedling'
            WHEN utm_content ~* 'coolsculpting' OR utm_content ~* 'emsculpt' OR utm_content ~* 'body contouring' THEN 'Body Contouring'
            ELSE 'Others'
        END AS service,
        COUNT(opportunity_name) as leads,
        SUM(CASE WHEN stage = 'Replied' THEN 1 ELSE 0 END) as replied,
        SUM(CASE WHEN stage = 'Cold' THEN 1 ELSE 0 END) as cold,
        SUM(CASE WHEN stage = 'Booked' THEN 1 ELSE 0 END) as booked,
        SUM(CASE WHEN stage = 'Patient' THEN 1 ELSE 0 END) as patient
    FROM {{ source('gsheet_raw','crm_leads') }}
    GROUP BY 1,2,3,4,5,6,7
    UNION ALL
    SELECT 'quarter' as date_granularity, DATE_TRUNC('quarter',created_on::date) as date,
        CASE WHEN source ~* 'facebook' THEN 'Facebook' WHEN source ~* 'google' THEN 'Google' ELSE 'Other' END as channel, 
        CASE WHEN utm_medium ~* 'SB' THEN SPLIT_PART(utm_medium,' - ',2) WHEN utm_medium !~* 'SB' THEN SPLIT_PART(utm_medium,' - ',1) END as location,
        utm_campaign as campaign_name, utm_medium as ad_group_name,
        CASE WHEN utm_content ~* 'lip flip' THEN 'Lip Flip'
            WHEN utm_content ~* 'hydrafacial' THEN 'Hydrafacial'
            WHEN utm_content ~* 'diamondglow' THEN 'Diamondglow'
            WHEN utm_content ~* 'LHR' THEN 'LHR'
            WHEN utm_content ~* 'Botox' THEN 'Botox'
            WHEN utm_content ~* 'Facial Filler' THEN 'Facial Filler'
            WHEN utm_content ~* 'Lip Filler' THEN 'Lip Filler'
            WHEN utm_content ~* 'Microneedling' THEN 'Microneedling'
            WHEN utm_content ~* 'coolsculpting' OR utm_content ~* 'emsculpt' OR utm_content ~* 'body contouring' THEN 'Body Contouring'
            ELSE 'Others'
        END AS service,
        COUNT(opportunity_name) as leads,
        SUM(CASE WHEN stage = 'Replied' THEN 1 ELSE 0 END) as replied,
        SUM(CASE WHEN stage = 'Cold' THEN 1 ELSE 0 END) as cold,
        SUM(CASE WHEN stage = 'Booked' THEN 1 ELSE 0 END) as booked,
        SUM(CASE WHEN stage = 'Patient' THEN 1 ELSE 0 END) as patient
    FROM {{ source('gsheet_raw','crm_leads') }}
    GROUP BY 1,2,3,4,5,6,7
    UNION ALL
    SELECT 'year' as date_granularity, DATE_TRUNC('year',created_on::date) as date,
        CASE WHEN source ~* 'facebook' THEN 'Facebook' WHEN source ~* 'google' THEN 'Google' ELSE 'Other' END as channel, 
        CASE WHEN utm_medium ~* 'SB' THEN SPLIT_PART(utm_medium,' - ',2) WHEN utm_medium !~* 'SB' THEN SPLIT_PART(utm_medium,' - ',1) END as location,
        utm_campaign as campaign_name, utm_medium as ad_group_name,
        CASE WHEN utm_content ~* 'lip flip' THEN 'Lip Flip'
            WHEN utm_content ~* 'hydrafacial' THEN 'Hydrafacial'
            WHEN utm_content ~* 'diamondglow' THEN 'Diamondglow'
            WHEN utm_content ~* 'LHR' THEN 'LHR'
            WHEN utm_content ~* 'Botox' THEN 'Botox'
            WHEN utm_content ~* 'Facial Filler' THEN 'Facial Filler'
            WHEN utm_content ~* 'Lip Filler' THEN 'Lip Filler'
            WHEN utm_content ~* 'Microneedling' THEN 'Microneedling'
            WHEN utm_content ~* 'coolsculpting' OR utm_content ~* 'emsculpt' OR utm_content ~* 'body contouring' THEN 'Body Contouring'
            ELSE 'Others'
        END AS service,
        COUNT(opportunity_name) as leads,
        SUM(CASE WHEN stage = 'Replied' THEN 1 ELSE 0 END) as replied,
        SUM(CASE WHEN stage = 'Cold' THEN 1 ELSE 0 END) as cold,
        SUM(CASE WHEN stage = 'Booked' THEN 1 ELSE 0 END) as booked,
        SUM(CASE WHEN stage = 'Patient' THEN 1 ELSE 0 END) as patient
    FROM {{ source('gsheet_raw','crm_leads') }}
    GROUP BY 1,2,3,4,5,6,7),
    
fb_data as 
    (SELECT 'Facebook' as channel, campaign_name, campaign_type, ad_group_name, location, service, date, date_granularity, 
        COALESCE(SUM(spend),0) as spend, COALESCE(SUM(impressions),0) as impressions, COALESCE(SUM(clicks),0) as clicks, 
        COALESCE(SUM(bookings_completed),0) as bookings_completed, COALESCE(SUM(leads),0) as leads, COALESCE(SUM(appointments_scheduled),0) as appointments_scheduled,
        COALESCE(SUM(platform_leads),0) as platform_leads 
    FROM
        (SELECT campaign_name, 
            CASE WHEN campaign_name = '[SB] - Prospecting - DTB' THEN 'Prospecting DTB' 
                WHEN campaign_name = '[SB] - Prospecting - Leads' THEN 'Prospecting Leads' 
                WHEN campaign_name = '[SB] - Retargeting - DTB' THEN 'Retargeting DTB'
                WHEN campaign_name = '[SB] - DTB - Catch All - Black Friday 2024' THEN 'Black Friday DTB'
                ELSE 'Other'
            END as campaign_type,
            adset_name as ad_group_name, location, service, date, date_granularity, 
            spend, impressions, link_clicks as clicks, 0 as bookings_completed, 0 as leads, 0 as replied, 0 as cold, 0 as booked, 0 as patient, 
            appointments_scheduled, leads as platform_leads
        FROM {{ source('reporting','facebook_ad_performance') }}
        UNION ALL
        SELECT campaign_name, 
            CASE WHEN campaign_name = '[SB] - Prospecting - DTB' THEN 'Prospecting DTB' 
                WHEN campaign_name = '[SB] - Prospecting - Leads' THEN 'Prospecting Leads' 
                WHEN campaign_name = '[SB] - Retargeting - DTB' THEN 'Retargeting DTB'
                WHEN campaign_name = '[SB] - DTB - Catch All - Black Friday 2024' THEN 'Black Friday DTB'
                ELSE 'Other'
            END as campaign_type,
            ad_group_name, CASE WHEN ad_group_name ~* 'SB' THEN SPLIT_PART(ad_group_name,' - ',2) WHEN ad_group_name !~* 'SB' THEN SPLIT_PART(ad_group_name,' - ',1) END as location, 
            CASE WHEN ad_name ~* 'lip flip' THEN 'Lip Flip'
                WHEN ad_name ~* 'hydrafacial' THEN 'Hydrafacial'
                WHEN ad_name ~* 'diamondglow' THEN 'Diamondglow'
                WHEN ad_name ~* 'LHR' THEN 'LHR'
                WHEN ad_name ~* 'Botox' THEN 'Botox'
                WHEN ad_name ~* 'Facial Filler' THEN 'Facial Filler'
                WHEN ad_name ~* 'Lip Filler' THEN 'Lip Filler'
                WHEN ad_name ~* 'Microneedling' THEN 'Microneedling'
                WHEN ad_name ~* 'coolsculpting' OR ad_name ~* 'emsculpt' OR ad_name ~* 'body contouring' THEN 'Body Contouring'
                ELSE 'Others'
            END AS service,
            date, date_granularity,
            0 as spend, 0 as impressions, 0 as clicks, COALESCE(SUM(bookings_completed),0) as bookings_completed, 0 as leads, 0 as leads, 
            0 as replied, 0 as cold, 0 as booked, 0 as patient, 0 as appointments_scheduled, 0 as platform_leads
        FROM bookings_data
        LEFT JOIN (SELECT DISTINCT campaign_id::text, campaign_name, adset_id::text as ad_group_id, adset_name as ad_group_name, ad_id::text, ad_name FROM {{ source('reporting','facebook_ad_performance') }}) USING(campaign_id,ad_id)   
        WHERE channel = 'Facebook'
        GROUP BY 1,2,3,4,5,6,7
        UNION ALL
        SELECT campaign_name, 
            CASE WHEN campaign_name = '[SB] - Prospecting - DTB' THEN 'Prospecting DTB' 
                WHEN campaign_name = '[SB] - Prospecting - Leads' THEN 'Prospecting Leads' 
                WHEN campaign_name = '[SB] - Retargeting - DTB' THEN 'Retargeting DTB'
                WHEN campaign_name = '[SB] - DTB - Catch All - Black Friday 2024' THEN 'Black Friday DTB'
                ELSE 'Other'
            END as campaign_type,
            ad_group_name, location, service, date, date_granularity,
            0 as spend, 0 as impressions, 0 as clicks, 0 as bookings_completed, 
            COALESCE(SUM(leads),0) as leads, COALESCE(SUM(replied),0) as replied, COALESCE(SUM(cold),0) as cold, COALESCE(SUM(booked),0) as booked, 
            COALESCE(SUM(patient),0) as patient, 0 as appointments_scheduled, 0 as platform_leads
        FROM leads_data
        WHERE channel = 'Facebook'
        GROUP BY 1,2,3,4,5,6,7          
        )
    GROUP BY 1,2,3,4,5,6,7,8),

adw_data as 
    (SELECT 'Google' as channel, campaign_name, campaign_type, ad_group_name, location, service, date, date_granularity, 
        COALESCE(SUM(spend),0) as spend, COALESCE(SUM(impressions),0) as impressions, COALESCE(SUM(clicks),0) as clicks, 
        COALESCE(SUM(bookings_completed),0) as bookings_completed, COALESCE(SUM(leads),0) as leads, COALESCE(SUM(appointments_scheduled),0) as appointments_scheduled,
        COALESCE(SUM(platform_leads),0) as platform_leads
    FROM
        (SELECT campaign_name, 
            CASE WHEN campaign_name ~* 'Search - Branded' THEN 'Search - Branded' WHEN campaign_name ~* 'Search - Non Brand' THEN 'Search - Non Brand' WHEN campaign_name ~* 'PMax' THEN 'PMax' END as campaign_type,
            '(not set)' as ad_group_name, null as location, null as service, date, date_granularity, 
            spend, impressions, clicks, 0 as bookings_completed, 0 as leads, 0 as leads, 0 as replied, 0 as cold, 0 as booked, 0 as patient, 
            appointments_scheduled, 0 as platform_leads
        FROM {{ source('reporting','googleads_campaign_performance') }}
        UNION ALL
        SELECT campaign_name, 
            CASE WHEN campaign_name ~* 'Search - Branded' THEN 'Search - Branded' WHEN campaign_name ~* 'Search - Non Brand' THEN 'Search - Non Brand' WHEN campaign_name ~* 'PMax' THEN 'PMax' END as campaign_type,
            '(not set)' as ad_group_name, null as location, null as service, date, date_granularity,
            0 as spend, 0 as impressions, 0 as clicks, COALESCE(SUM(bookings_completed),0) as bookings_completed, 0 as leads, 
            0 as leads, 0 as replied, 0 as cold, 0 as booked, 0 as patient, 0 as appointments_scheduled, 0 as platform_leads
        FROM bookings_data
        LEFT JOIN (SELECT DISTINCT campaign_id::text, campaign_name FROM {{ source('reporting','googleads_campaign_performance') }}) USING(campaign_id)   
        WHERE channel = 'Google'
        GROUP BY 1,2,3,4,5,6,7
        UNION ALL
        SELECT campaign_name, 
            CASE WHEN campaign_name ~* 'Search - Branded' THEN 'Search - Branded' WHEN campaign_name ~* 'Search - Non Brand' THEN 'Search - Non Brand' WHEN campaign_name ~* 'PMax' THEN 'PMax' END as campaign_type,
            '(not set)' as ad_group_name, null as location, null as service, date, date_granularity,
            0 as spend, 0 as impressions, 0 as clicks, 0 as bookings_completed, 
            COALESCE(SUM(leads),0) as leads, COALESCE(SUM(replied),0) as replied, COALESCE(SUM(cold),0) as cold, COALESCE(SUM(booked),0) as booked, 
            COALESCE(SUM(patient),0) as patient, 0 as appointments_scheduled, 0 as platform_leads
        FROM leads_data
        WHERE channel = 'Google'
        GROUP BY 1,2,3,4,5,6,7
        )
    GROUP BY 1,2,3,4,5,6,7,8)

SELECT channel, campaign_name, campaign_type, ad_group_name, location, service, date, date_granularity, 
    spend, impressions, clicks, bookings_completed, leads, appointments_scheduled, platform_leads, replied, cold, booked, patient
FROM 
    (SELECT * FROM fb_data
    UNION ALL 
    SELECT * FROM adw_data)

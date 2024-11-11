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
CASE WHEN adset_name ~* 'SB' THEN SPLIT_PART(adset_name,' - ',2) WHEN adset_name !~* 'SB' THEN SPLIT_PART(adset_name,' - ',1) END as location,
ad_name,
ad_id,
ad_effective_status,
visual,
copy,
format_visual,
visual_copy,
CASE WHEN ad_name ~* 'lip flip' THEN 'Lip Flip'
    WHEN ad_name ~* 'BBL' THEN 'BBL'
    WHEN ad_name ~* 'hydrafacial' OR ad_name ~* 'diamondglow' THEN 'Facials'
    WHEN ad_name ~* 'LHR' THEN 'LHR'
    WHEN ad_name ~* 'Botox' THEN 'Tox'
    WHEN ad_name ~* 'Filler' THEN 'Fillers'
    WHEN ad_name ~* 'Morpheus' THEN 'Morpheus'
    WHEN ad_name ~* 'coolsculpting' OR ad_name ~* 'emsculpt' THEN 'Body Contouring'
    ELSE 'Others'
END AS service,
date,
date_granularity,
spend,
impressions,
link_clicks,
onfacebook_leads+website_leads as leads,
schedule_total-"offsite_conversion.fb_pixel_custom.dtbcomplete" as appointments_scheduled
FROM {{ ref('facebook_performance_by_ad') }}

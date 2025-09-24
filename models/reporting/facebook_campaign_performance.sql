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
schedule_total as appointments_scheduled,
"offsite_conversion.fb_pixel_custom.booked appointment" as booked_appointment,
"offsite_conversion.fb_pixel_custom.completed appointment" as completed_appointment,
"offsite_conversion.fb_pixel_custom.bookingcomplete" as booking_complete
FROM {{ ref('facebook_performance_by_campaign') }}

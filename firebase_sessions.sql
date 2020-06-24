
WITH
  -- Subquery to define static and/or dynamic start and end date for the whole query
  period AS (
  SELECT '20200607' AS start_date, FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) AS end_date),
  -- Subquery to get all sessions with a length > 10 seconds
  session_length AS (
  SELECT user_pseudo_id, (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS sessionid,
    TIMESTAMP_MICROS(MIN(event_timestamp)) AS session_start_time,
    (MAX(event_timestamp)-MIN(event_timestamp))/1000000 AS session_length_seconds
  FROM
    `project_id.analytics_191462280.events_*`, period
  WHERE _table_suffix BETWEEN period.start_date AND period.end_date 
  GROUP BY 1, 2
  HAVING
    -- Change this number to adjust the desired session length
    session_length_seconds >= 10
  ORDER BY 1,2),
  
  -- Subquery to get all sessions with 2 or more (unique) page views
  multiple_pageviews AS (
  SELECT user_pseudo_id, sessionid, session_start_time, pageviews
  FROM (
    SELECT user_pseudo_id, sessionid, session_start_time, COUNT(pageview_location) OVER (PARTITION BY user_pseudo_id, sessionid) AS pageviews,
      ROW_NUMBER() OVER (PARTITION BY user_pseudo_id, sessionid) AS row_number
    FROM (
      SELECT user_pseudo_id, event_timestamp, (
        SELECT value.int_value FROM UNNEST(event_params)
        WHERE event_name = 'page_view' AND key = 'ga_session_id') AS sessionid,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE event_name = 'page_view' AND key = 'page_location') AS pageview_location,
        TIMESTAMP_MICROS(MIN(event_timestamp)) AS session_start_time,
      FROM
        -- Change this to your Google Analytics App + Web export location in BigQuery
        `project_id.analytics_191462280.events_*`, period
      WHERE _table_suffix BETWEEN period.start_date AND period.end_date
      GROUP BY 1,2,3,4
      HAVING sessionid IS NOT NULL)
    GROUP BY user_pseudo_id, sessionid, pageview_location, session_start_time
    ORDER BY 1,2 DESC)
  WHERE row_number = 1
    -- Change this number to adjust the desired amount of page views
    AND pageviews > 1),
  
  
  -- Subquery to get all sessions with a conversion event (in this example 'first_visit')
  conversion_event AS (
  SELECT user_pseudo_id, (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS sessionid,
    TIMESTAMP_MICROS(MIN(event_timestamp)) AS session_start_time
  FROM
    -- Change this to your Google Analytics App + Web export location in BigQuery
    `project_id.analytics_191462280.events_*`, period
  WHERE _table_suffix BETWEEN period.start_date AND period.end_date
    -- Change this event_name to adjust the desired conversion event
    AND REGEXP_CONTAINS(event_name, 'first_visit|address_clicked|app_clear_data|app_exception|app_open|app_open_time|app_remove|app_update|banner_clicked|begin_checkout|begin_checkout_native|Contact|dynamic_link_app_open|dynamic_link_first_open|ecommerce_purchase_native|first_open|LeadReceived|os_update|registration_completed|screen_view|session_start|store_selection|user_location|view_product|view_search_results')
  GROUP BY 1,2
  ORDER BY 1,2 )
  -- Main query to count unique engaged sessions by date
SELECT DATE(session_start_time) AS date, COUNT(DISTINCT CONCAT(user_pseudo_id,sessionid)) AS engaged_sessions
FROM (
    -- Subquery to combine and deduplicate all subqueries generated earlier
  SELECT user_pseudo_id, sessionid, session_start_time FROM session_length
--   UNION DISTINCT
--   SELECT user_pseudo_id, sessionid, session_start_time FROM multiple_pageviews
--   UNION DISTINCT
--   SELECT user_pseudo_id, sessionid, session_start_time FROM conversion_event
)
GROUP BY 1
ORDER BY 1 DESC

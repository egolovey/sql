-- !DATE! Funnel owoxbi sesions one day process
SELECT
'!DATE!' AS date,
  source,
  medium,
  campaign,
  keyword,
  device,
  region,
  sessions_count,
  cost,
  transactions_count,
  last_click_attribution_attributed_revenue_online AS last_click_attribution_attributed_revenue_online,
  _3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_attributed_revenue_online AS _funnel_based_attribution_attributed_revenue_online,
  _3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_attributed_revenue AS _funnel_based_attribution_attributed_revenue,

  last_click_attribution_value_online AS last_click_attribution_value_online,
  _3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_value_online AS _funnel_based_attribution_value_online,
  _3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_value AS _funnel_based_attribution_value,

FROM (
  SELECT
    source AS source,
    medium AS medium,
    campaign AS campaign,
    keyword AS keyword,
    device AS device,
    region AS region,
    sessions_count,
    transactions_count,
    cost,
    last_click_attribution_value_online AS last_click_attribution_value_online,
    last_click_attribution_value_online_summary AS last_click_attribution_value_online_summary,
    last_click_attribution_attributed_revenue_online AS last_click_attribution_attributed_revenue_online,
    last_click_attribution_attributed_revenue_online_summary AS last_click_attribution_attributed_revenue_online_summary,
    sessions_count_summary,
    transactions_count_summary,
    cost_summary
    --Funnel (owox sess)
    ,
    _3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_value_online AS _3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_value_online,
    _3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_value_online_summary AS _3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_value_online_summary,
    _3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_value AS _3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_value,
    _3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_value_summary AS _3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_value_summary,
    _3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_attributed_revenue_online AS _3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_attributed_revenue_online,
    _3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_attributed_revenue_online_summary AS _3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_attributed_revenue_online_summary,
    _3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_attributed_revenue AS _3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_attributed_revenue,
    _3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_attributed_revenue_summary AS _3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_attributed_revenue_summary,
    _3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_value_online_change,
    _3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_value_change,
    _3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_attributed_revenue_online_change,
    _3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_attributed_revenue_change
  FROM (
    SELECT
      COALESCE(t1.source, t2.source) AS source,
      COALESCE(t1.medium, t2.medium) AS medium,
      COALESCE(t1.campaign, t2.campaign) AS campaign,
      COALESCE(t1.keyword, t2.keyword) AS keyword,
      COALESCE(t1.device, t2.device) AS device,
      COALESCE(t1.region, t2.region) AS region,
      IFNULL(t1.cost, 0) AS cost,
      t1.last_click_attribution_value_online AS last_click_attribution_value_online,
      FIRST_VALUE(t1.last_click_attribution_value_online_summary) OVER() AS last_click_attribution_value_online_summary,
      t1.last_click_attribution_attributed_revenue_online AS last_click_attribution_attributed_revenue_online,
      FIRST_VALUE(t1.last_click_attribution_attributed_revenue_online_summary) OVER() AS last_click_attribution_attributed_revenue_online_summary,
      t1.cost_summary AS cost_summary,
      t1.sessions_count AS sessions_count,
      FIRST_VALUE(t1.sessions_count_summary) OVER() AS sessions_count_summary,
      t1.transactions_count AS transactions_count,
      FIRST_VALUE(t1.transactions_count_summary) OVER() AS transactions_count_summary
      --Funnel (owox sess)
      ,
      t2._3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_value_online AS _3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_value_online,
      FIRST_VALUE(t2._3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_value_online_summary) OVER() AS _3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_value_online_summary,
      t2._3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_value AS _3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_value,
      FIRST_VALUE(t2._3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_value_summary) OVER() AS _3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_value_summary,
      t2._3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_attributed_revenue_online AS _3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_attributed_revenue_online,
      FIRST_VALUE(t2._3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_attributed_revenue_online_summary) OVER() AS _3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_attributed_revenue_online_summary,
      t2._3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_attributed_revenue AS _3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_attributed_revenue,
      FIRST_VALUE(t2._3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_attributed_revenue_summary) OVER() AS _3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_attributed_revenue_summary,
      (t2._3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_value_online - t1.last_click_attribution_value_online) / t1.last_click_attribution_value_online AS _3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_value_online_change,
      (t2._3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_value - t1.last_click_attribution_value) / t1.last_click_attribution_value AS _3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_value_change,
      (t2._3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_attributed_revenue_online - t1.last_click_attribution_attributed_revenue_online) / t1.last_click_attribution_attributed_revenue_online AS _3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_attributed_revenue_online_change,
      (t2._3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_attributed_revenue - t1.last_click_attribution_attributed_revenue) / t1.last_click_attribution_attributed_revenue AS _3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_attributed_revenue_change
    FROM (
      SELECT
        *
      FROM (
        SELECT
          source,
          medium,
          campaign,
          keyword,
          device,
          region,
          value_online AS last_click_attribution_value_online,
          value AS last_click_attribution_value,
          SUM(last_click_attribution_value_online) OVER() AS last_click_attribution_value_online_summary,
          SUM(last_click_attribution_value) OVER() AS last_click_attribution_value_summary,
          attributed_revenue_online AS last_click_attribution_attributed_revenue_online,
          attributed_revenue AS last_click_attribution_attributed_revenue,
          SUM(last_click_attribution_attributed_revenue_online) OVER() AS last_click_attribution_attributed_revenue_online_summary,
          SUM(last_click_attribution_attributed_revenue) OVER() AS last_click_attribution_attributed_revenue_summary,
          IFNULL(cost, 0) AS cost,
          SUM(cost) OVER() AS cost_summary,
          sessions_count,
          SUM(sessions_count) OVER() AS sessions_count_summary,
          transactions_count,
          SUM(transactions_count) OVER() AS transactions_count_summary
        FROM (
          SELECT
            COALESCE(sessions.source, revenues.source) AS source,
            COALESCE(sessions.medium, revenues.medium) AS medium,
            COALESCE(sessions.campaign, revenues.campaign) AS campaign,
            COALESCE(sessions.keyword, revenues.keyword) AS keyword,
            IFNULL(sessions.device, '(not set)') AS device,
            IFNULL(sessions.region, '(not set)') AS region,
            SUM(revenues.attributed_revenue) AS attributed_revenue,
            SUM(revenues.attributed_revenue_online) AS attributed_revenue_online,
            SUM(revenues.value) AS value,
            SUM(revenues.value_online) AS value_online,
            SUM(costs.cost) AS cost,
            COUNT(DISTINCT summary.session_id, 1000000) AS sessions_count,
            SUM(revenues.transactions_count) AS transactions_count
          FROM (
            SELECT
              sessionId AS session_id,
              FIRST(trafficSource.source) AS source,
              FIRST(trafficSource.medium) AS medium,
              FIRST(trafficSource.campaign) AS campaign,
              FIRST(trafficSource.keyword) AS keyword,
              FIRST(device.deviceCategory) AS device,
              FIRST(geoNetwork.region) AS region,
            FROM
              TABLE_DATE_RANGE([delta-frame-184620:OWOXBI_Streaming.owoxbi_sessions_],
                DATE_ADD(TIMESTAMP(TIMESTAMP('!DATE!')), -90, 'DAY'),
                TIMESTAMP(DATE_ADD(DATE_ADD(UTC_USEC_TO_DAY(TIMESTAMP('!DATE!')), 1, 'day'), -1, 'second')))
            GROUP EACH BY
              session_id ) AS sessions
          LEFT JOIN EACH (
            SELECT
              sessionId AS session_id,
            FROM
              TABLE_DATE_RANGE([delta-frame-184620:OWOXBI_Streaming.owoxbi_sessions_],
                TIMESTAMP(TIMESTAMP('!DATE!')),
                TIMESTAMP(DATE_ADD(DATE_ADD(UTC_USEC_TO_DAY(TIMESTAMP('!DATE!')), 1, 'day'), -1, 'second')))
            GROUP EACH BY
              session_id ) AS summary
          ON
            summary.session_id = sessions.session_id
          LEFT JOIN EACH (
            SELECT
              session_id,
              FIRST(source) AS source,
              FIRST(medium) AS medium,
              FIRST(campaign) AS campaign,
              FIRST(keyword) AS keyword,
              SUM(value) AS value,
              SUM(IF(transaction_source!=101, value, 0)) AS value_online,
              SUM(revenue * value) AS attributed_revenue,
              SUM(IF(transaction_source!=101, revenue, 0) * IF(transaction_source!=101, value, 0)) AS attributed_revenue_online,
              SUM(IF(active_step == 5, 1, 0)) AS transactions_count
            FROM (
              SELECT
                value_from_sid AS session_id,
                1 AS value,
                IFNULL(source, 'NULL') AS source,
                IFNULL(medium, 'NULL') AS medium,
                IFNULL(campaign, 'NULL') AS campaign,
                IFNULL(keyword, 'NULL') AS keyword,
                revenue,
                active_step,
                MAX(IF(active_step == 5, data_source, NULL)) OVER(PARTITION BY transaction_id) AS transaction_source,
                RANK() OVER(PARTITION BY transaction_id ORDER BY date DESC) AS rowf
              FROM
                [delta-frame-184620:Attribution_FunnelBased_2_US.values]
              WHERE
                DATE(_PARTITIONTIME) BETWEEN DATE(DATE_ADD(TIMESTAMP('!DATE!'), -90,'DAY'))
                AND DATE(DATE_ADD(DATE_ADD(UTC_USEC_TO_DAY(TIMESTAMP('!DATE!')), 1, 'day'), -1, 'second'))
                AND DATE(MSEC_TO_TIMESTAMP(transaction_time)) BETWEEN DATE(TIMESTAMP('!DATE!'))
                AND DATE(DATE_ADD(DATE_ADD(UTC_USEC_TO_DAY(TIMESTAMP('!DATE!')), 1, 'day'), -1, 'second'))
                AND active_step == 5 )
            WHERE
              rowf = 1
            GROUP EACH BY
              session_id ) AS revenues
          ON
            revenues.session_id = sessions.session_id
          LEFT JOIN EACH (
            SELECT
              sessionId AS session_id,
              SUM(trafficSource.attributedAdCost) AS cost
            FROM
              TABLE_DATE_RANGE([delta-frame-184620:OWOXBI_Streaming.owoxbi_sessions_],
                TIMESTAMP(TIMESTAMP('!DATE!')),
                TIMESTAMP(DATE_ADD(DATE_ADD(UTC_USEC_TO_DAY(TIMESTAMP('!DATE!')), 1, 'day'), -1, 'second')))
            GROUP EACH BY
              session_id ) AS costs
          ON
            costs.session_id = sessions.session_id
          GROUP EACH BY
            source,
            medium,
            campaign,
            keyword,
            device,
            region )
        WHERE
          cost>0
          OR attributed_revenue IS NOT NULL
          OR value IS NOT NULL ) ) AS t1
    FULL OUTER JOIN EACH (
      SELECT
        *
      FROM (
        SELECT
          source,
          medium,
          campaign,
          keyword,
          device,
          region,
          value_online AS _3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_value_online,
          value AS _3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_value,
          SUM(_3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_value_online) OVER() AS _3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_value_online_summary,
          SUM(_3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_value) OVER() AS _3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_value_summary,
          attributed_revenue_online AS _3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_attributed_revenue_online,
          attributed_revenue AS _3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_attributed_revenue,
          SUM(_3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_attributed_revenue_online) OVER() AS _3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_attributed_revenue_online_summary,
          SUM(_3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_attributed_revenue) OVER() AS _3e5a63e1fd734d219440d7e8dc6ea3f6_attribution_attributed_revenue_summary,
          IFNULL(cost, 0) AS cost,
          SUM(cost) OVER() AS cost_summary,
          sessions_count,
          SUM(sessions_count) OVER() AS sessions_count_summary,
          transactions_count,
          SUM(transactions_count) OVER() AS transactions_count_summary
        FROM (
          SELECT
            COALESCE(revenues.source, sessions.source) AS source,
            COALESCE(revenues.medium, sessions.medium) AS medium,
            COALESCE(revenues.campaign, sessions.campaign) AS campaign,
            COALESCE(revenues.keyword, sessions.keyword) AS keyword,
            IFNULL(sessions.device, '(not set)') AS device,
            IFNULL(sessions.region, '(not set)') AS region,
            SUM(revenues.attributed_revenue) AS attributed_revenue,
            SUM(revenues.attributed_revenue_online) AS attributed_revenue_online,
            SUM(revenues.value) AS value,
            SUM(revenues.value_online) AS value_online,
            SUM(costs.cost) AS cost,
            COUNT(DISTINCT summary.session_id, 1000000) AS sessions_count,
            SUM(revenues.transactions_count) AS transactions_count
          FROM (
            SELECT
              sessionId AS session_id,
              FIRST(trafficSource.source) AS source,
              FIRST(trafficSource.medium) AS medium,
              FIRST(trafficSource.campaign) AS campaign,
              FIRST(trafficSource.keyword) AS keyword,
              FIRST(device.deviceCategory) AS device,
              FIRST(geoNetwork.region) AS region,
            FROM
              TABLE_DATE_RANGE([delta-frame-184620:OWOXBI_Streaming.owoxbi_sessions_],
                DATE_ADD(TIMESTAMP(TIMESTAMP('!DATE!')), -90, 'DAY'),
                TIMESTAMP(DATE_ADD(DATE_ADD(UTC_USEC_TO_DAY(TIMESTAMP('!DATE!')), 1, 'day'), -1, 'second')))
            GROUP EACH BY
              session_id ) AS sessions
          LEFT JOIN EACH (
            SELECT
              sessionId AS session_id,
            FROM
              TABLE_DATE_RANGE([delta-frame-184620:OWOXBI_Streaming.owoxbi_sessions_],
                TIMESTAMP(TIMESTAMP('!DATE!')),
                TIMESTAMP(DATE_ADD(DATE_ADD(UTC_USEC_TO_DAY(TIMESTAMP('!DATE!')), 1, 'day'), -1, 'second')))
            GROUP EACH BY
              session_id ) AS summary
          ON
            summary.session_id = sessions.session_id
          LEFT JOIN EACH (
            SELECT
              session_id,
              FIRST(source) AS source,
              FIRST(medium) AS medium,
              FIRST(campaign) AS campaign,
              FIRST(keyword) AS keyword,
              SUM(value) AS value,
              SUM(IF(transaction_source!=101, value, 0)) AS value_online,
              SUM(revenue * value) AS attributed_revenue,
              SUM(IF(transaction_source!=101, revenue, 0) * IF(transaction_source!=101, value, 0)) AS attributed_revenue_online,
              SUM(IF(active_step == 5, 1, 0)) AS transactions_count
            FROM (
              SELECT
                session_id,
                value,
                IFNULL(source, 'NULL') AS source,
                IFNULL(medium, 'NULL') AS medium,
                IFNULL(campaign, 'NULL') AS campaign,
                IFNULL(keyword, 'NULL') AS keyword,
                revenue,
                active_step,
                MAX(IF(active_step == 5, data_source, NULL)) OVER(PARTITION BY transaction_id) AS transaction_source,
                RANK() OVER(PARTITION BY transaction_id ORDER BY date DESC) AS rowf
              FROM
                [delta-frame-184620:Attribution_FunnelBased_2_US.values]
              WHERE
                DATE(_PARTITIONTIME) BETWEEN DATE(DATE_ADD(TIMESTAMP('!DATE!'), -90,'DAY'))
                AND DATE(DATE_ADD(DATE_ADD(UTC_USEC_TO_DAY(TIMESTAMP('!DATE!')), 1, 'day'), -1, 'second'))
                AND DATE(MSEC_TO_TIMESTAMP(transaction_time)) BETWEEN DATE(TIMESTAMP('!DATE!'))
                AND DATE(DATE_ADD(DATE_ADD(UTC_USEC_TO_DAY(TIMESTAMP('!DATE!')), 1, 'day'), -1, 'second')) )
            WHERE
              rowf = 1
            GROUP EACH BY
              session_id ) AS revenues
          ON
            revenues.session_id = sessions.session_id
          LEFT JOIN EACH (
            SELECT
              sessionId AS session_id,
              SUM(trafficSource.attributedAdCost) AS cost
            FROM
              TABLE_DATE_RANGE([delta-frame-184620:OWOXBI_Streaming.owoxbi_sessions_],
                TIMESTAMP(TIMESTAMP('!DATE!')),
                TIMESTAMP(DATE_ADD(DATE_ADD(UTC_USEC_TO_DAY(TIMESTAMP('!DATE!')), 1, 'day'), -1, 'second')))
            GROUP EACH BY
              session_id ) AS costs
          ON
            costs.session_id = sessions.session_id
          GROUP EACH BY
            source,
            medium,
            campaign,
            keyword,
            device,
            region )
        WHERE
          cost>0
          OR attributed_revenue IS NOT NULL
          OR value IS NOT NULL ) ) AS t2
    ON
      t1.source = t2.source
      AND t1.medium = t2.medium
      AND t1.campaign = t2.campaign
      AND t1.keyword = t2.keyword
      AND t1.device = t2.device
      AND t1.region = t2.region ) )


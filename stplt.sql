-- !DATE! Как отличается атрибутированный доход по ключевым словам и типам устройств и регионам и типам пользователей согласно модели атрибуции GA Last Non-Direct Click в сравнении с Funnel (skip email/direct/mkb) с 1 июня 2018 по 1 июня 2018?
SELECT
  '!DATE!' as date,
  source,
  medium,
  campaign,
  keyword,
  device,
  region,
  user_type,
  sessions_count,
  cost,
  transactions_count,
  last_click_attribution_attributed_revenue_online AS last_click_attribution_attributed_revenue_online,
  --Funnel (skip email/direct/mkb)
  _funnel_skip_emaildirectmkb_attribution_attributed_revenue_online AS _funnel_skip_emaildirectmkb_attribution_attributed_revenue_online,
  _funnel_skip_emaildirectmkb_attribution_attributed_revenue AS _funnel_skip_emaildirectmkb_attribution_attributed_revenue,
  _funnel_skip_emaildirectmkb_attribution_attributed_revenue_online_change,
  _funnel_skip_emaildirectmkb_attribution_attributed_revenue_change,
FROM (
  SELECT
    source AS source,
    medium AS medium,
    campaign AS campaign,
    keyword AS keyword,
    device AS device,
    region AS region,
    user_type AS user_type,
    sessions_count,
    transactions_count,
    cost,
    last_click_attribution_attributed_revenue_online AS last_click_attribution_attributed_revenue_online,
    last_click_attribution_attributed_revenue_online_summary AS last_click_attribution_attributed_revenue_online_summary,
    sessions_count_summary,
    transactions_count_summary,
    cost_summary
    --Funnel (skip email/direct/mkb)
    ,
    _funnel_skip_emaildirectmkb_attribution_attributed_revenue_online AS _funnel_skip_emaildirectmkb_attribution_attributed_revenue_online,
    _funnel_skip_emaildirectmkb_attribution_attributed_revenue_online_summary AS _funnel_skip_emaildirectmkb_attribution_attributed_revenue_online_summary,
    _funnel_skip_emaildirectmkb_attribution_attributed_revenue AS _funnel_skip_emaildirectmkb_attribution_attributed_revenue,
    _funnel_skip_emaildirectmkb_attribution_attributed_revenue_summary AS _funnel_skip_emaildirectmkb_attribution_attributed_revenue_summary,
    _funnel_skip_emaildirectmkb_attribution_attributed_revenue_online_change,
    _funnel_skip_emaildirectmkb_attribution_attributed_revenue_change
  FROM (
    SELECT
      COALESCE(t1.source, t2.source) AS source,
      COALESCE(t1.medium, t2.medium) AS medium,
      COALESCE(t1.campaign, t2.campaign) AS campaign,
      COALESCE(t1.keyword, t2.keyword) AS keyword,
      COALESCE(t1.device, t2.device) AS device,
      COALESCE(t1.region, t2.region) AS region,
      COALESCE(t1.user_type, t2.user_type) AS user_type,
      IFNULL(t1.cost, 0) AS cost,
      t1.last_click_attribution_attributed_revenue_online AS last_click_attribution_attributed_revenue_online,
      t1.last_click_attribution_attributed_revenue_online_summary AS last_click_attribution_attributed_revenue_online_summary,
      t1.cost_summary AS cost_summary,
      t1.sessions_count AS sessions_count,
      t1.sessions_count_summary AS sessions_count_summary,
      t1.transactions_count AS transactions_count,
      t1.transactions_count_summary AS transactions_count_summary
      --Funnel (skip email/direct/mkb)
      ,
      t2._funnel_skip_emaildirectmkb_attribution_attributed_revenue_online AS _funnel_skip_emaildirectmkb_attribution_attributed_revenue_online,
      t2._funnel_skip_emaildirectmkb_attribution_attributed_revenue_online_summary AS _funnel_skip_emaildirectmkb_attribution_attributed_revenue_online_summary,
      t2._funnel_skip_emaildirectmkb_attribution_attributed_revenue AS _funnel_skip_emaildirectmkb_attribution_attributed_revenue,
      t2._funnel_skip_emaildirectmkb_attribution_attributed_revenue_summary AS _funnel_skip_emaildirectmkb_attribution_attributed_revenue_summary,
      (t2._funnel_skip_emaildirectmkb_attribution_attributed_revenue_online - t1.last_click_attribution_attributed_revenue_online) / t1.last_click_attribution_attributed_revenue_online AS _funnel_skip_emaildirectmkb_attribution_attributed_revenue_online_change,
      (t2._funnel_skip_emaildirectmkb_attribution_attributed_revenue - t2._funnel_skip_emaildirectmkb_attribution_attributed_revenue_online) / t2._funnel_skip_emaildirectmkb_attribution_attributed_revenue_online AS _funnel_skip_emaildirectmkb_attribution_attributed_revenue_change
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
          user_type,
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
            COALESCE(sessions.original_source, source) AS source,
            COALESCE(sessions.original_medium, medium) AS medium,
            COALESCE(sessions.original_campaign, campaign) AS campaign,
            COALESCE(sessions.original_keyword, keyword) AS keyword,
            IFNULL(device, 'NULL') AS device,
            IFNULL(region, 'NULL') AS region,
            IFNULL(user_type, 'New') AS user_type,
            SUM(attributed_revenue) AS attributed_revenue,
            SUM(attributed_revenue_online) AS attributed_revenue_online,
            SUM(value) AS value,
            SUM(value_online) AS value_online,
            SUM(costs.cost) AS cost,
            EXACT_COUNT_DISTINCT(sessions.session_id) AS sessions_count,
            SUM(transactions_count) AS transactions_count
          FROM (
            SELECT
              sessionId AS session_id,
              FIRST(device.deviceCategory) AS device,
              FIRST(geoNetwork.region) AS region,
              FIRST(geoNetwork.city) AS city,
              FIRST(geoNetwork.country) AS country,
              FIRST(trafficSource.source) AS original_source,
              FIRST(trafficSource.medium) AS original_medium,
              FIRST(trafficSource.campaign) AS original_campaign,
              FIRST(trafficSource.keyword) AS original_keyword,
              FIRST(IF (visitNumber IS NULL, '(not set)', IF (visitNumber=1, 'New', 'Returning'))) AS user_type,
              FIRST(trafficSource.channelGrouping) AS channel_grouping
            FROM
              TABLE_DATE_RANGE([stolplit-197214:OWOXBI_Streaming.owoxbi_sessions_],
                TIMESTAMP('!DATE!'),
                TIMESTAMP('!DATE!'))
            GROUP EACH BY
              session_id ) AS sessions
          OUTER JOIN EACH (
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
                MAX(IF( active_step == 5,data_source,NULL)) OVER(PARTITION BY transaction_id) AS transaction_source,
                RANK() OVER(PARTITION BY transaction_id ORDER BY date DESC) AS rowf
              FROM
                [stolplit-197214:Attribution_FunnelBased_2.values]
              WHERE
                _PARTITIONTIME BETWEEN DATE_ADD(TIMESTAMP('!DATE!'), -60,'DAY')
                AND TIMESTAMP('!DATE!')
                AND DATE(MSEC_TO_TIMESTAMP(transaction_time)) BETWEEN DATE('!DATE!')
                AND DATE('!DATE!')
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
              TABLE_DATE_RANGE([stolplit-197214:OWOXBI_Streaming.owoxbi_sessions_],
                TIMESTAMP('!DATE!'),
                TIMESTAMP('!DATE!'))
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
            region,
            user_type )
        WHERE
          cost>0
          OR attributed_revenue IS NOT NULL ) ) AS t1
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
          user_type,
          attributed_revenue_online AS _funnel_skip_emaildirectmkb_attribution_attributed_revenue_online,
          attributed_revenue AS _funnel_skip_emaildirectmkb_attribution_attributed_revenue,
          SUM(_funnel_skip_emaildirectmkb_attribution_attributed_revenue_online) OVER() AS _funnel_skip_emaildirectmkb_attribution_attributed_revenue_online_summary,
          SUM(_funnel_skip_emaildirectmkb_attribution_attributed_revenue) OVER() AS _funnel_skip_emaildirectmkb_attribution_attributed_revenue_summary,
          IFNULL(cost, 0) AS cost,
          SUM(cost) OVER() AS cost_summary,
          sessions_count,
          SUM(sessions_count) OVER() AS sessions_count_summary,
        FROM (
          SELECT
            COALESCE(source, sessions.original_source) AS source,
            COALESCE(medium, sessions.original_medium) AS medium,
            COALESCE(campaign, sessions.original_campaign) AS campaign,
            COALESCE(keyword, sessions.original_keyword) AS keyword,
            IFNULL(device, 'NULL') AS device,
            IFNULL(region, 'NULL') AS region,
            IFNULL(user_type, 'New') AS user_type,
            SUM(attributed_revenue) AS attributed_revenue,
            SUM(attributed_revenue_online) AS attributed_revenue_online,
            SUM(value) AS value,
            SUM(value_online) AS value_online,
            SUM(costs.cost) AS cost,
            EXACT_COUNT_DISTINCT(sessions.session_id) AS sessions_count,
          FROM (
            SELECT
              sessionId AS session_id,
              FIRST(device.deviceCategory) AS device,
              FIRST(geoNetwork.region) AS region,
              FIRST(geoNetwork.city) AS city,
              FIRST(geoNetwork.country) AS country,
              FIRST(trafficSource.source) AS original_source,
              FIRST(trafficSource.medium) AS original_medium,
              FIRST(trafficSource.campaign) AS original_campaign,
              FIRST(trafficSource.keyword) AS original_keyword,
              FIRST(IF (visitNumber IS NULL, '(not set)', IF (visitNumber=1, 'New', 'Returning'))) AS user_type,
              FIRST(trafficSource.channelGrouping) AS channel_grouping
            FROM
              TABLE_DATE_RANGE([stolplit-197214:OWOXBI_Streaming.owoxbi_sessions_],
                TIMESTAMP('!DATE!'),
                TIMESTAMP('!DATE!'))
            GROUP EACH BY
              session_id ) AS sessions
          OUTER JOIN EACH (
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
                MAX(IF( active_step == 5,data_source,NULL)) OVER(PARTITION BY transaction_id) AS transaction_source,
                RANK() OVER(PARTITION BY transaction_id ORDER BY date DESC) AS rowf
              FROM
                [stolplit-197214:Attribution_FunnelBased_2.values]
              WHERE
                _PARTITIONTIME BETWEEN DATE_ADD(TIMESTAMP('!DATE!'), -60,'DAY')
                AND TIMESTAMP('!DATE!')
                AND DATE(MSEC_TO_TIMESTAMP(transaction_time)) BETWEEN DATE('!DATE!')
                AND DATE('!DATE!') )
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
              TABLE_DATE_RANGE([stolplit-197214:OWOXBI_Streaming.owoxbi_sessions_],
                TIMESTAMP('!DATE!'),
                TIMESTAMP('!DATE!'))
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
            region,
            user_type )
        WHERE
          cost>0
          OR attributed_revenue IS NOT NULL ) ) AS t2
    ON
      t1.source = t2.source
      AND t1.medium = t2.medium
      AND t1.campaign = t2.campaign
      AND t1.keyword = t2.keyword
      AND t1.device = t2.device
      AND t1.region = t2.region
      AND t1.user_type = t2.user_type ) )

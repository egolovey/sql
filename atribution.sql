










--Атрибутированный доход и ценность по регионам и ключевым словам и типам устройств в сравнении между моделями атрибуции Funnel Based и GA Last Non-Direct Click с 15 января 2019 по 15 января 2019
SELECT
    region,
    source,
    medium,
    campaign,
    keyword,
    device,
    sessions_count,
    cost,
    transactions_count,
    _3d1078704fe64d30a4403da9d6d1a389_attribution_attributed_revenue_online as _3d1078704fe64d30a4403da9d6d1a389_attribution_attributed_revenue_online
,
_3d1078704fe64d30a4403da9d6d1a389_attribution_attributed_revenue as _3d1078704fe64d30a4403da9d6d1a389_attribution_attributed_revenue
,
_3d1078704fe64d30a4403da9d6d1a389_attribution_value_online as _3d1078704fe64d30a4403da9d6d1a389_attribution_value_online
,
_3d1078704fe64d30a4403da9d6d1a389_attribution_value as _3d1078704fe64d30a4403da9d6d1a389_attribution_value,



        --GA Last Non-Direct Click
        _last_click_attribution_attributed_revenue_online as _last_click_attribution_attributed_revenue_online
,
_last_click_attribution_value_online as _last_click_attribution_value_online,

            _last_click_attribution_attributed_revenue_online_change,
                _last_click_attribution_attributed_revenue_change,
            _last_click_attribution_value_online_change,
                _last_click_attribution_value_change,



FROM
(



    SELECT
        region as region,
        source as source,
        medium as medium,
        campaign as campaign,
        keyword as keyword,
        device as device,
        sessions_count,
        transactions_count,
        cost,
        _3d1078704fe64d30a4403da9d6d1a389_attribution_attributed_revenue_online as _3d1078704fe64d30a4403da9d6d1a389_attribution_attributed_revenue_online
,
_3d1078704fe64d30a4403da9d6d1a389_attribution_attributed_revenue_online_summary as
_3d1078704fe64d30a4403da9d6d1a389_attribution_attributed_revenue_online_summary
,
_3d1078704fe64d30a4403da9d6d1a389_attribution_attributed_revenue as _3d1078704fe64d30a4403da9d6d1a389_attribution_attributed_revenue
,
_3d1078704fe64d30a4403da9d6d1a389_attribution_attributed_revenue_summary as
_3d1078704fe64d30a4403da9d6d1a389_attribution_attributed_revenue_summary
,
_3d1078704fe64d30a4403da9d6d1a389_attribution_value_online as _3d1078704fe64d30a4403da9d6d1a389_attribution_value_online
,
_3d1078704fe64d30a4403da9d6d1a389_attribution_value_online_summary as
_3d1078704fe64d30a4403da9d6d1a389_attribution_value_online_summary
,
_3d1078704fe64d30a4403da9d6d1a389_attribution_value as _3d1078704fe64d30a4403da9d6d1a389_attribution_value
,
_3d1078704fe64d30a4403da9d6d1a389_attribution_value_summary as
_3d1078704fe64d30a4403da9d6d1a389_attribution_value_summary,

        sessions_count_summary,
        transactions_count_summary,
        cost_summary

        --GA Last Non-Direct Click
        , _last_click_attribution_attributed_revenue_online as _last_click_attribution_attributed_revenue_online
,
_last_click_attribution_attributed_revenue_online_summary as
_last_click_attribution_attributed_revenue_online_summary
,
_last_click_attribution_value_online as _last_click_attribution_value_online
,
_last_click_attribution_value_online_summary as
_last_click_attribution_value_online_summary,
                _last_click_attribution_attributed_revenue_online_change,
                _last_click_attribution_attributed_revenue_change,
                _last_click_attribution_value_online_change,
                _last_click_attribution_value_change

    FROM
    (


    SELECT
            COALESCE(t1.region, t2.region) as region,
            COALESCE(t1.source, t2.source) as source,
            COALESCE(t1.medium, t2.medium) as medium,
            COALESCE(t1.campaign, t2.campaign) as campaign,
            COALESCE(t1.keyword, t2.keyword) as keyword,
            COALESCE(t1.device, t2.device) as device,
        IFNULL(t1.cost, 0) as cost,

        t1._3d1078704fe64d30a4403da9d6d1a389_attribution_attributed_revenue_online as _3d1078704fe64d30a4403da9d6d1a389_attribution_attributed_revenue_online
,
FIRST_VALUE(t1._3d1078704fe64d30a4403da9d6d1a389_attribution_attributed_revenue_online_summary) OVER() as
_3d1078704fe64d30a4403da9d6d1a389_attribution_attributed_revenue_online_summary
,
t1._3d1078704fe64d30a4403da9d6d1a389_attribution_attributed_revenue as _3d1078704fe64d30a4403da9d6d1a389_attribution_attributed_revenue
,
FIRST_VALUE(t1._3d1078704fe64d30a4403da9d6d1a389_attribution_attributed_revenue_summary) OVER() as
_3d1078704fe64d30a4403da9d6d1a389_attribution_attributed_revenue_summary
,
t1._3d1078704fe64d30a4403da9d6d1a389_attribution_value_online as _3d1078704fe64d30a4403da9d6d1a389_attribution_value_online
,
FIRST_VALUE(t1._3d1078704fe64d30a4403da9d6d1a389_attribution_value_online_summary) OVER() as
_3d1078704fe64d30a4403da9d6d1a389_attribution_value_online_summary
,
t1._3d1078704fe64d30a4403da9d6d1a389_attribution_value as _3d1078704fe64d30a4403da9d6d1a389_attribution_value
,
FIRST_VALUE(t1._3d1078704fe64d30a4403da9d6d1a389_attribution_value_summary) OVER() as
_3d1078704fe64d30a4403da9d6d1a389_attribution_value_summary,

        t1.cost_summary as cost_summary,
        t1.sessions_count as sessions_count,
        FIRST_VALUE(t1.sessions_count_summary) OVER() as sessions_count_summary,
        t1.transactions_count as transactions_count,
        FIRST_VALUE(t1.transactions_count_summary) OVER() as transactions_count_summary

            --GA Last Non-Direct Click

            , t2._last_click_attribution_attributed_revenue_online as _last_click_attribution_attributed_revenue_online
,
FIRST_VALUE(t2._last_click_attribution_attributed_revenue_online_summary) OVER() as
_last_click_attribution_attributed_revenue_online_summary
,
t2._last_click_attribution_value_online as _last_click_attribution_value_online
,
FIRST_VALUE(t2._last_click_attribution_value_online_summary) OVER() as
_last_click_attribution_value_online_summary,


                (t2._last_click_attribution_attributed_revenue_online - t1._3d1078704fe64d30a4403da9d6d1a389_attribution_attributed_revenue_online) / t1._3d1078704fe64d30a4403da9d6d1a389_attribution_attributed_revenue_online as _last_click_attribution_attributed_revenue_online_change,
                (t2._last_click_attribution_attributed_revenue - t1._3d1078704fe64d30a4403da9d6d1a389_attribution_attributed_revenue) / t1._3d1078704fe64d30a4403da9d6d1a389_attribution_attributed_revenue as _last_click_attribution_attributed_revenue_change,

                (t2._last_click_attribution_value_online - t1._3d1078704fe64d30a4403da9d6d1a389_attribution_value_online) / t1._3d1078704fe64d30a4403da9d6d1a389_attribution_value_online as _last_click_attribution_value_online_change,
                (t2._last_click_attribution_value - t1._3d1078704fe64d30a4403da9d6d1a389_attribution_value) / t1._3d1078704fe64d30a4403da9d6d1a389_attribution_value as _last_click_attribution_value_change


    FROM
    (



    SELECT
        *
    FROM
    (
        SELECT
            region,
            source,
            medium,
            campaign,
            keyword,
            device,

                attributed_revenue_online as _3d1078704fe64d30a4403da9d6d1a389_attribution_attributed_revenue_online,
                attributed_revenue as _3d1078704fe64d30a4403da9d6d1a389_attribution_attributed_revenue,
                    SUM(_3d1078704fe64d30a4403da9d6d1a389_attribution_attributed_revenue_online) OVER() as _3d1078704fe64d30a4403da9d6d1a389_attribution_attributed_revenue_online_summary,
                    SUM(_3d1078704fe64d30a4403da9d6d1a389_attribution_attributed_revenue) OVER() as _3d1078704fe64d30a4403da9d6d1a389_attribution_attributed_revenue_summary,
                value_online as _3d1078704fe64d30a4403da9d6d1a389_attribution_value_online,
                value as _3d1078704fe64d30a4403da9d6d1a389_attribution_value,
                    SUM(_3d1078704fe64d30a4403da9d6d1a389_attribution_value_online) OVER() as _3d1078704fe64d30a4403da9d6d1a389_attribution_value_online_summary,
                    SUM(_3d1078704fe64d30a4403da9d6d1a389_attribution_value) OVER() as _3d1078704fe64d30a4403da9d6d1a389_attribution_value_summary,

            IFNULL(cost, 0) as cost,
            SUM(cost) OVER() AS cost_summary,
            sessions_count,
            SUM(sessions_count) OVER() as sessions_count_summary,
            transactions_count,
            SUM(transactions_count) OVER() as transactions_count_summary
        FROM
        (
            SELECT
                        IFNULL(sessions.region, '(not set)') as region,
                        COALESCE(revenues.source, sessions.source) as source,
                        COALESCE(revenues.medium, sessions.medium) as medium,
                        COALESCE(revenues.campaign, sessions.campaign) as campaign,
                        COALESCE(revenues.keyword, sessions.keyword) as keyword,
                        IFNULL(sessions.device, '(not set)') as device,
                SUM(revenues.attributed_revenue) AS attributed_revenue,
                SUM(revenues.attributed_revenue_online) as attributed_revenue_online,
                SUM(revenues.value) AS value,
                SUM(revenues.value_online) AS value_online,
                SUM(costs.cost) AS cost,
                COUNT(DISTINCT summary.session_id, 1000000) as sessions_count,
                SUM(revenues.transactions_count) as transactions_count
            FROM
            (
        
    SELECT
        sessionId as session_id,
        MIN(SEC_TO_TIMESTAMP(hits.time)) as sessionTimestamp,
            FIRST(geoNetwork.region) as region,
            FIRST(trafficSource.source) as source,
            FIRST(trafficSource.medium) as medium,
            FIRST(trafficSource.campaign) as campaign,
            FIRST(trafficSource.keyword) as keyword,
            FIRST(device.deviceCategory) as device,


FROM TABLE_DATE_RANGE([delta-frame-184620:OWOXBI_Streaming.session_streaming_], DATE_ADD(TIMESTAMP('!DATE!'), -90, 'DAY'), DATE_ADD(DATE_ADD(DATE_ADD(UTC_USEC_TO_DAY(TIMESTAMP('!DATE!')), 1, 'day'), -1, 'second'), 1, 'DAY'))    GROUP EACH BY
        session_id


    HAVING sessionTimestamp BETWEEN DATE_ADD(TIMESTAMP('!DATE!'), -90, 'DAY') AND DATE_ADD(DATE_ADD(UTC_USEC_TO_DAY(TIMESTAMP('!DATE!')), 1, 'day'), -1, 'second')
        
            ) AS sessions
            LEFT JOIN EACH
            (
        
    SELECT
        sessionId as session_id,
        MIN(SEC_TO_TIMESTAMP(hits.time)) as sessionTimestamp,


FROM TABLE_DATE_RANGE([delta-frame-184620:OWOXBI_Streaming.session_streaming_], TIMESTAMP('!DATE!'), DATE_ADD(DATE_ADD(DATE_ADD(UTC_USEC_TO_DAY(TIMESTAMP('!DATE!')), 1, 'day'), -1, 'second'), 1, 'DAY'))    GROUP EACH BY
        session_id


    HAVING sessionTimestamp BETWEEN TIMESTAMP('!DATE!') AND DATE_ADD(DATE_ADD(UTC_USEC_TO_DAY(TIMESTAMP('!DATE!')), 1, 'day'), -1, 'second')
        
            ) AS summary
            ON
                summary.session_id = sessions.session_id
            LEFT JOIN EACH
            (
    SELECT
        session_id,
            FIRST(source) as source,
            FIRST(medium) as medium,
            FIRST(campaign) as campaign,
            FIRST(keyword) as keyword,
        SUM(value) as value,
        SUM(IF(transaction_source!=101, value, 0)) as value_online,
        SUM(revenue * value) AS attributed_revenue,
        SUM(IF(transaction_source!=101, revenue, 0) * IF(transaction_source!=101, value, 0)) as attributed_revenue_online,
        SUM(IF(active_step == 5, 1, 0)) AS transactions_count
    FROM
    (
        SELECT
            session_id,
            value,
                    IFNULL(source, 'NULL') as source,
                    IFNULL(medium, 'NULL') as medium,
                    IFNULL(campaign, 'NULL') as campaign,
                    IFNULL(keyword, 'NULL') as keyword,
            revenue,
            active_step,
            MAX(IF(active_step == 5, data_source, NULL)) OVER(PARTITION BY transaction_id) as transaction_source,
    RANK() OVER(PARTITION BY transaction_id ORDER BY date DESC) AS rowf
        FROM
            [delta-frame-184620:Attribution_FunnelBased.values]
        WHERE
        DATE(_PARTITIONTIME) BETWEEN DATE(DATE_ADD(TIMESTAMP('!DATE!'), -90,'DAY')) AND DATE(DATE_ADD(DATE_ADD(UTC_USEC_TO_DAY(TIMESTAMP('!DATE!')), 1, 'day'), -1, 'second'))
        AND DATE(MSEC_TO_TIMESTAMP(transaction_time)) BETWEEN DATE(TIMESTAMP('!DATE!')) AND DATE(DATE_ADD(DATE_ADD(UTC_USEC_TO_DAY(TIMESTAMP('!DATE!')), 1, 'day'), -1, 'second'))

    )
    WHERE
        rowf = 1
    GROUP EACH BY
        session_id
            ) AS revenues
            ON
                revenues.session_id = sessions.session_id
            LEFT JOIN EACH
            (
        
    SELECT
        sessionId as session_id,
        SUM(trafficSource.attributedAdCost) as cost

FROM TABLE_DATE_RANGE([delta-frame-184620:OWOXBI_Streaming.session_streaming_], TIMESTAMP('!DATE!'), DATE_ADD(DATE_ADD(UTC_USEC_TO_DAY(TIMESTAMP('!DATE!')), 1, 'day'), -1, 'second'))    GROUP EACH BY
        session_id
        
            ) AS costs
            ON
                costs.session_id = sessions.session_id
            GROUP EACH BY
                region,
                source,
                medium,
                campaign,
                keyword,
                device
        )
    )

    ) as t1
        FULL OUTER JOIN EACH
        (



    SELECT
        *
    FROM
    (
        SELECT
            region,
            source,
            medium,
            campaign,
            keyword,
            device,

                attributed_revenue_online as _last_click_attribution_attributed_revenue_online,
                attributed_revenue as _last_click_attribution_attributed_revenue,
                    SUM(_last_click_attribution_attributed_revenue_online) OVER() as _last_click_attribution_attributed_revenue_online_summary,
                    SUM(_last_click_attribution_attributed_revenue) OVER() as _last_click_attribution_attributed_revenue_summary,
                value_online as _last_click_attribution_value_online,
                value as _last_click_attribution_value,
                    SUM(_last_click_attribution_value_online) OVER() as _last_click_attribution_value_online_summary,
                    SUM(_last_click_attribution_value) OVER() as _last_click_attribution_value_summary,

            IFNULL(cost, 0) as cost,
            SUM(cost) OVER() AS cost_summary,
            sessions_count,
            SUM(sessions_count) OVER() as sessions_count_summary,
            transactions_count,
            SUM(transactions_count) OVER() as transactions_count_summary
        FROM
        (
            SELECT
                        IFNULL(sessions.region, '(not set)') as region,
                        COALESCE(sessions.source, revenues.source) as source,
                        COALESCE(sessions.medium, revenues.medium) as medium,
                        COALESCE(sessions.campaign, revenues.campaign) as campaign,
                        COALESCE(sessions.keyword, revenues.keyword) as keyword,
                        IFNULL(sessions.device, '(not set)') as device,
                SUM(revenues.attributed_revenue) AS attributed_revenue,
                SUM(revenues.attributed_revenue_online) as attributed_revenue_online,
                SUM(revenues.value) AS value,
                SUM(revenues.value_online) AS value_online,
                SUM(costs.cost) AS cost,
                COUNT(DISTINCT summary.session_id, 1000000) as sessions_count,
                SUM(revenues.transactions_count) as transactions_count
            FROM
            (
        
    SELECT
        sessionId as session_id,
        MIN(SEC_TO_TIMESTAMP(hits.time)) as sessionTimestamp,
            FIRST(geoNetwork.region) as region,
            FIRST(trafficSource.source) as source,
            FIRST(trafficSource.medium) as medium,
            FIRST(trafficSource.campaign) as campaign,
            FIRST(trafficSource.keyword) as keyword,
            FIRST(device.deviceCategory) as device,


FROM TABLE_DATE_RANGE([delta-frame-184620:OWOXBI_Streaming.session_streaming_], DATE_ADD(TIMESTAMP('!DATE!'), -90, 'DAY'), DATE_ADD(DATE_ADD(DATE_ADD(UTC_USEC_TO_DAY(TIMESTAMP('!DATE!')), 1, 'day'), -1, 'second'), 1, 'DAY'))    GROUP EACH BY
        session_id


    HAVING sessionTimestamp BETWEEN DATE_ADD(TIMESTAMP('!DATE!'), -90, 'DAY') AND DATE_ADD(DATE_ADD(UTC_USEC_TO_DAY(TIMESTAMP('!DATE!')), 1, 'day'), -1, 'second')
        
            ) AS sessions
            LEFT JOIN EACH
            (
        
    SELECT
        sessionId as session_id,
        MIN(SEC_TO_TIMESTAMP(hits.time)) as sessionTimestamp,


FROM TABLE_DATE_RANGE([delta-frame-184620:OWOXBI_Streaming.session_streaming_], TIMESTAMP('!DATE!'), DATE_ADD(DATE_ADD(DATE_ADD(UTC_USEC_TO_DAY(TIMESTAMP('!DATE!')), 1, 'day'), -1, 'second'), 1, 'DAY'))    GROUP EACH BY
        session_id


    HAVING sessionTimestamp BETWEEN TIMESTAMP('!DATE!') AND DATE_ADD(DATE_ADD(UTC_USEC_TO_DAY(TIMESTAMP('!DATE!')), 1, 'day'), -1, 'second')
        
            ) AS summary
            ON
                summary.session_id = sessions.session_id
            LEFT JOIN EACH
            (
    SELECT
        session_id,
            FIRST(source) as source,
            FIRST(medium) as medium,
            FIRST(campaign) as campaign,
            FIRST(keyword) as keyword,
        SUM(value) as value,
        SUM(IF(transaction_source!=101, value, 0)) as value_online,
        SUM(revenue * value) AS attributed_revenue,
        SUM(IF(transaction_source!=101, revenue, 0) * IF(transaction_source!=101, value, 0)) as attributed_revenue_online,
        SUM(IF(active_step == 5, 1, 0)) AS transactions_count
    FROM
    (
        SELECT
            value_from_sid as session_id,
            1 as value,
                    IFNULL(source, 'NULL') as source,
                    IFNULL(medium, 'NULL') as medium,
                    IFNULL(campaign, 'NULL') as campaign,
                    IFNULL(keyword, 'NULL') as keyword,
            revenue,
            active_step,
            MAX(IF(active_step == 5, data_source, NULL)) OVER(PARTITION BY transaction_id) as transaction_source,
    RANK() OVER(PARTITION BY transaction_id ORDER BY date DESC) AS rowf
        FROM
            [delta-frame-184620:Attribution_FunnelBased.values]
        WHERE
        DATE(_PARTITIONTIME) BETWEEN DATE(DATE_ADD(TIMESTAMP('!DATE!'), -90,'DAY')) AND DATE(DATE_ADD(DATE_ADD(UTC_USEC_TO_DAY(TIMESTAMP('!DATE!')), 1, 'day'), -1, 'second'))
        AND DATE(MSEC_TO_TIMESTAMP(transaction_time)) BETWEEN DATE(TIMESTAMP('!DATE!')) AND DATE(DATE_ADD(DATE_ADD(UTC_USEC_TO_DAY(TIMESTAMP('!DATE!')), 1, 'day'), -1, 'second'))

                AND active_step == 5
    )
    WHERE
        rowf = 1
    GROUP EACH BY
        session_id
            ) AS revenues
            ON
                revenues.session_id = sessions.session_id
            LEFT JOIN EACH
            (
        
    SELECT
        sessionId as session_id,
        SUM(trafficSource.attributedAdCost) as cost

FROM TABLE_DATE_RANGE([delta-frame-184620:OWOXBI_Streaming.session_streaming_], TIMESTAMP('!DATE!'), DATE_ADD(DATE_ADD(UTC_USEC_TO_DAY(TIMESTAMP('!DATE!')), 1, 'day'), -1, 'second'))    GROUP EACH BY
        session_id
        
            ) AS costs
            ON
                costs.session_id = sessions.session_id
            GROUP EACH BY
                region,
                source,
                medium,
                campaign,
                keyword,
                device
        )
    )

        ) as t2
        ON
            t1.region = t2.region AND
            t1.source = t2.source AND
            t1.medium = t2.medium AND
            t1.campaign = t2.campaign AND
            t1.keyword = t2.keyword AND
            t1.device = t2.device




    )



)

        ORDER BY cost DESC
        LIMIT 2500

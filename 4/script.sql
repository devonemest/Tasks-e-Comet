SELECT
    phrase,
    groupArray((hour, total_views)) AS views_by_hour
FROM (
    SELECT
        phrase,
        toHour(dt) AS hour,
        SUM(views) AS total_views
    FROM phrases_views
    where campaign_id = 1111111
    GROUP BY phrase, hour
    order by hour
) AS aggregated_data
GROUP BY phrase
ORDER BY phrase;

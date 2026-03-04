----Query 1----
-- top 10 property types ranked by average review score
SELECT
    property_type,
    COUNT(*)                                        AS total_listings,
    ROUND(AVG(mmt_review_score)::numeric, 2)        AS avg_review_score,
    ROUND(AVG(hotel_star_rating)::numeric, 2)       AS avg_star_rating,
    SUM(mmt_review_count)                           AS total_reviews,
    COUNT(*) FILTER (WHERE is_value_plus = 1)       AS value_plus_count
FROM hotel_listings
WHERE property_type <> 'Unknown'
  AND mmt_review_score IS NOT NULL
GROUP BY property_type
ORDER BY avg_review_score DESC, total_listings DESC
LIMIT 10;


---Query 2---
-- monthly listing count with a running cumulative total
SELECT
    TO_CHAR(DATE_TRUNC('month', crawl_date::date), 'YYYY-MM') AS month,
    COUNT(*)                                                    AS listings_scraped,
    COUNT(DISTINCT city)                                        AS cities_covered,
    ROUND(AVG(mmt_review_score)::numeric, 2)                    AS avg_score,
    SUM(COUNT(*)) OVER (ORDER BY DATE_TRUNC('month', crawl_date::date)) AS cumulative_total
FROM hotel_listings
WHERE crawl_date IS NOT NULL
GROUP BY DATE_TRUNC('month', crawl_date::date)
ORDER BY month;


---Query 3---
-- top 20 cities by number of listings, with average ratings (min 5 listings)
SELECT
    city,
    state,
    COUNT(*)                                        AS total_listings,
    ROUND(AVG(mmt_review_score)::numeric, 2)        AS avg_review_score,
    ROUND(AVG(hotel_star_rating)::numeric, 2)       AS avg_star_rating,
    ROUND(AVG(mmt_location_rating)::numeric, 2)     AS avg_location_score,
    COUNT(*) FILTER (WHERE hotel_star_rating >= 4)  AS luxury_count,
    SUM(mmt_review_count)                           AS total_reviews
FROM hotel_listings
WHERE city IS NOT NULL
GROUP BY city, state
HAVING COUNT(*) >= 5
ORDER BY total_listings DESC, avg_review_score DESC
LIMIT 20;

WITH productcategories AS (
SELECT 
  product_id, 
  SPLIT(CAST(category AS STRING), '|')[OFFSET(0)] AS main_category, 
  SPLIT(CAST(category AS STRING), '|')[OFFSET(1)] AS sub_category 
FROM 
  `amazonsales.amazonsales`
), reviews AS (
  SELECT 
    product_id,
    review_id,
    user_id,
    user_name,
    review_title,
    review_content,
    -- Unnesting lists into individual rows by splitting on commas
    SPLIT(review_id, ',') AS review_ids,
    SPLIT(user_id, ',') AS user_ids,
    SPLIT(user_name, ',') AS user_names,
    SPLIT(review_title, ',') AS review_titles,
    SPLIT(review_content, ',') AS review_contents
  FROM 
    `amazonsales.amazonsales`
),
unnested_reviews AS (
  SELECT 
    product_id, 
    -- Unnest the arrays created by SPLIT into individual rows
    review_id,
    user_id,
    review_title
  FROM 
    reviews,
    UNNEST(review_ids) AS review_id,
    UNNEST(user_ids) AS user_id,
    UNNEST(review_titles) AS review_title
)
SELECT 
    amazondata.product_id,
    COALESCE(product_name, 'Unknown') AS product_name,
    COALESCE(main_category, 'Unknown') AS main_category,
    COALESCE(sub_category, 'Unknown') AS sub_category,
    COALESCE(discounted_price, 0) AS discounted_price,
    COALESCE(actual_price, 0) AS actual_price,
    COALESCE(discount_percentage, 0) AS discount_percentage,
    COALESCE(rating, '0.0') AS rating,
    COALESCE(rating_count, 0) AS rating_count,
    COALESCE(about_product, 'No description available') AS about_product,
    SAFE_DIVIDE(actual_price - discounted_price, actual_price) AS discount_ratio,
    unnested_reviews.review_id,
    unnested_reviews.user_id,
    unnested_reviews.review_title
  FROM `amazonsales.amazonsales` amazondata 
  JOIN productcategories ON productcategories.product_id = amazondata.product_id
  JOIN unnested_reviews ON unnested_reviews.product_id = amazondata.product_id

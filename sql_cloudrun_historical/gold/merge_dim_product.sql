MERGE `{{dataset}}.dim_product` T
USING (
  SELECT
    p.id AS product_id,
    p.title,
    p.category,
    p.brand,
    p.price,
    p.rating AS product_rating,
    p.stock,
    p.availabilityStatus,
    COUNT(r.product_id) AS total_reviews,
    AVG(r.rating) AS avg_review_rating
  FROM `{{dataset}}.silver_products` p
LEFT JOIN `{{dataset}}.silver_product_reviews` r
    ON p.id = r.product_id
  GROUP BY
    p.id,
    p.title,
    p.category,
    p.brand,
    p.price,
    p.rating,
    p.stock,
    p.availabilityStatus
) S
ON T.product_id = S.product_id

WHEN MATCHED THEN
  UPDATE SET
    title = S.title,
    category = S.category,
    brand = S.brand,
    price = S.price,
    product_rating = S.product_rating,
    stock = S.stock,
    availabilityStatus = S.availabilityStatus,
    total_reviews = S.total_reviews,
    avg_review_rating = S.avg_review_rating

WHEN NOT MATCHED THEN
  INSERT (
    product_id,
    title,
    category,
    brand,
    price,
    product_rating,
    stock,
    availabilityStatus,
    total_reviews,
    avg_review_rating
  )
  VALUES (
    S.product_id,
    S.title,
    S.category,
    S.brand,
    S.price,
    S.product_rating,
    S.stock,
    S.availabilityStatus,
    S.total_reviews,
    S.avg_review_rating
  );

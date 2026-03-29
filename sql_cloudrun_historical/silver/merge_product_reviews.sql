
MERGE `{{dataset}}.silver_product_reviews` T

USING (

  SELECT *
  FROM (
    SELECT
      b.id AS product_id,
      r.reviewerName,
      r.reviewerEmail,
      r.rating,
      r.comment,
      r.date AS review_date,
      b.extraction_timestamp,

      ROW_NUMBER() OVER (
        PARTITION BY 
          b.id,
          r.reviewerEmail,
          r.date
        ORDER BY b.extraction_timestamp DESC
      ) AS rn

    FROM `{{dataset}}.bronze_products` b,
    UNNEST(b.reviews) r

  )
  WHERE rn = 1

) S

ON T.product_id = S.product_id
   AND T.reviewerEmail = S.reviewerEmail
   AND T.review_date = S.review_date

WHEN MATCHED THEN UPDATE SET
  rating = S.rating,
  comment = S.comment,
  extraction_timestamp = S.extraction_timestamp

WHEN NOT MATCHED THEN INSERT (
  product_id,
  reviewerName,
  reviewerEmail,
  rating,
  comment,
  review_date,
  extraction_timestamp
)
VALUES (
  S.product_id,
  S.reviewerName,
  S.reviewerEmail,
  S.rating,
  S.comment,
  S.review_date,
  S.extraction_timestamp
);

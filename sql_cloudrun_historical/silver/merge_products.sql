
MERGE `{{dataset}}.silver_products` T
USING (

  SELECT *
  FROM (
    SELECT
      id,
      title,
      description,
      category,
      price,
      discountPercentage,
      rating,
      stock,
      brand,
      sku,
      weight,
      warrantyInformation,
      shippingInformation,
      availabilityStatus,
      returnPolicy,
      minimumOrderQuantity,
      thumbnail,
      extraction_timestamp,

      ROW_NUMBER() OVER (
        PARTITION BY id
        ORDER BY extraction_timestamp DESC
      ) AS rn

   FROM `{{dataset}}.bronze_products`

  )
  WHERE rn = 1

) S

ON T.id = S.id

WHEN MATCHED THEN UPDATE SET
  title = S.title,
  description = S.description,
  category = S.category,
  price = S.price,
  discountPercentage = S.discountPercentage,
  rating = S.rating,
  stock = S.stock,
  brand = S.brand,
  sku = S.sku,
  weight = S.weight,
  warrantyInformation = S.warrantyInformation,
  shippingInformation = S.shippingInformation,
  availabilityStatus = S.availabilityStatus,
  returnPolicy = S.returnPolicy,
  minimumOrderQuantity = S.minimumOrderQuantity,
  thumbnail = S.thumbnail,
  extraction_timestamp = S.extraction_timestamp

WHEN NOT MATCHED THEN INSERT (
  id,
  title,
  description,
  category,
  price,
  discountPercentage,
  rating,
  stock,
  brand,
  sku,
  weight,
  warrantyInformation,
  shippingInformation,
  availabilityStatus,
  returnPolicy,
  minimumOrderQuantity,
  thumbnail,
  extraction_timestamp
)
VALUES (
  S.id,
  S.title,
  S.description,
  S.category,
  S.price,
  S.discountPercentage,
  S.rating,
  S.stock,
  S.brand,
  S.sku,
  S.weight,
  S.warrantyInformation,
  S.shippingInformation,
  S.availabilityStatus,
  S.returnPolicy,
  S.minimumOrderQuantity,
  S.thumbnail,
  S.extraction_timestamp
);


LOAD DATA INTO `{{dataset}}.bronze_products`
FROM FILES (
  format = 'JSON',
  uris = ['{{gcs_uri}}']
);

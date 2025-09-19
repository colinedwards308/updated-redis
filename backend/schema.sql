CREATE TABLE IF NOT EXISTS clients (
  id SERIAL PRIMARY KEY,
  name VARCHAR(120) NOT NULL,
  email VARCHAR(200) UNIQUE NOT NULL,
  created_at TIMESTAMP DEFAULT NOW(),
  last_active TIMESTAMP NULL
);

CREATE TABLE IF NOT EXISTS products (
  id SERIAL PRIMARY KEY,
  name VARCHAR(200) NOT NULL,
  category VARCHAR(120) NOT NULL,
  price NUMERIC(10,2) NOT NULL,
  purchase_count INT DEFAULT 0
);

CREATE TABLE IF NOT EXISTS carts (
  id SERIAL PRIMARY KEY,
  client_id INT NOT NULL REFERENCES clients(id),
  created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS cart_items (
  id SERIAL PRIMARY KEY,
  cart_id INT NOT NULL REFERENCES carts(id),
  product_id INT NOT NULL REFERENCES products(id),
  quantity INT NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_cart_client ON carts(client_id);
CREATE INDEX IF NOT EXISTS idx_item_cart ON cart_items(cart_id);
CREATE INDEX IF NOT EXISTS idx_item_product ON cart_items(product_id);
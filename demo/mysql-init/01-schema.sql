-- Demo e-commerce dataset for Pipeline Agent
-- Provides realistic data for MySQL → PostgreSQL pipeline demos

USE demo_ecommerce;

-- -------------------------------------------------------
-- Products
-- -------------------------------------------------------
CREATE TABLE products (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    sku         VARCHAR(32) NOT NULL UNIQUE,
    name        VARCHAR(200) NOT NULL,
    category    VARCHAR(100),
    price       DECIMAL(10,2) NOT NULL,
    cost        DECIMAL(10,2),
    weight_kg   DECIMAL(6,2),
    is_active   BOOLEAN DEFAULT TRUE,
    created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

INSERT INTO products (sku, name, category, price, cost, weight_kg) VALUES
('SKU-001','Wireless Mouse','Electronics',29.99,12.50,0.15),
('SKU-002','Mechanical Keyboard','Electronics',89.99,35.00,0.85),
('SKU-003','USB-C Hub 7-port','Electronics',49.99,18.00,0.20),
('SKU-004','27" 4K Monitor','Electronics',399.99,220.00,6.50),
('SKU-005','Laptop Stand','Accessories',34.99,14.00,1.20),
('SKU-006','Webcam 1080p','Electronics',59.99,22.00,0.12),
('SKU-007','Noise-Cancelling Headphones','Audio',199.99,85.00,0.30),
('SKU-008','Desk Lamp LED','Office',44.99,16.00,0.90),
('SKU-009','Ergonomic Chair','Furniture',549.99,280.00,18.00),
('SKU-010','Standing Desk Mat','Accessories',39.99,12.00,1.50),
('SKU-011','Cable Management Kit','Accessories',19.99,5.00,0.40),
('SKU-012','Monitor Arm','Accessories',79.99,32.00,2.80),
('SKU-013','Bluetooth Speaker','Audio',69.99,28.00,0.55),
('SKU-014','Portable SSD 1TB','Storage',109.99,55.00,0.10),
('SKU-015','Wireless Charger','Electronics',24.99,8.00,0.15),
('SKU-016','Mouse Pad XL','Accessories',14.99,3.50,0.30),
('SKU-017','Desk Organizer','Office',29.99,10.00,0.80),
('SKU-018','Blue Light Glasses','Accessories',34.99,8.00,0.05),
('SKU-019','Microphone USB','Audio',129.99,52.00,0.45),
('SKU-020','Power Strip Smart','Electronics',39.99,15.00,0.60);

-- -------------------------------------------------------
-- Customers
-- -------------------------------------------------------
CREATE TABLE customers (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    email       VARCHAR(255) NOT NULL UNIQUE,
    first_name  VARCHAR(100) NOT NULL,
    last_name   VARCHAR(100) NOT NULL,
    company     VARCHAR(200),
    city        VARCHAR(100),
    state       VARCHAR(50),
    country     VARCHAR(2) DEFAULT 'US',
    tier        ENUM('free','pro','enterprise') DEFAULT 'free',
    created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

INSERT INTO customers (email, first_name, last_name, company, city, state, tier) VALUES
('alice@example.com','Alice','Johnson','Acme Corp','San Francisco','CA','enterprise'),
('bob@example.com','Bob','Smith',NULL,'New York','NY','pro'),
('carol@example.com','Carol','Williams','TechStart','Austin','TX','pro'),
('dave@example.com','Dave','Brown','BigData Inc','Seattle','WA','enterprise'),
('eve@example.com','Eve','Davis',NULL,'Denver','CO','free'),
('frank@example.com','Frank','Garcia','CloudOps','Chicago','IL','pro'),
('grace@example.com','Grace','Martinez','DataFlow','Boston','MA','enterprise'),
('hank@example.com','Hank','Anderson',NULL,'Portland','OR','free'),
('iris@example.com','Iris','Thomas','Analytix','Miami','FL','pro'),
('jack@example.com','Jack','Jackson',NULL,'Nashville','TN','free'),
('karen@example.com','Karen','White','DevTools','Phoenix','AZ','pro'),
('leo@example.com','Leo','Harris','ScaleUp','Atlanta','GA','enterprise'),
('mia@example.com','Mia','Clark',NULL,'Minneapolis','MN','free'),
('noah@example.com','Noah','Lewis','InfraCore','Dallas','TX','pro'),
('olivia@example.com','Olivia','Robinson',NULL,'Salt Lake City','UT','free'),
('pat@example.com','Pat','Walker','SaaSify','Los Angeles','CA','enterprise'),
('quinn@example.com','Quinn','Hall',NULL,'Detroit','MI','free'),
('rosa@example.com','Rosa','Allen','PipelineHQ','San Diego','CA','pro'),
('sam@example.com','Sam','Young',NULL,'Raleigh','NC','free'),
('tina@example.com','Tina','King','DataBridge','Washington','DC','enterprise');

-- -------------------------------------------------------
-- Orders
-- -------------------------------------------------------
CREATE TABLE orders (
    id            INT AUTO_INCREMENT PRIMARY KEY,
    customer_id   INT NOT NULL,
    order_number  VARCHAR(20) NOT NULL UNIQUE,
    status        ENUM('pending','processing','shipped','delivered','cancelled') DEFAULT 'pending',
    subtotal      DECIMAL(10,2) NOT NULL,
    tax           DECIMAL(10,2) DEFAULT 0.00,
    shipping      DECIMAL(10,2) DEFAULT 0.00,
    total         DECIMAL(10,2) NOT NULL,
    currency      VARCHAR(3) DEFAULT 'USD',
    shipped_at    DATETIME,
    delivered_at  DATETIME,
    created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customers(id)
);

INSERT INTO orders (customer_id, order_number, status, subtotal, tax, shipping, total, shipped_at, delivered_at, created_at) VALUES
(1,'ORD-10001','delivered',89.99,7.20,5.99,103.18,'2025-12-02 10:00:00','2025-12-05 14:00:00','2025-12-01 09:15:00'),
(2,'ORD-10002','delivered',429.98,34.40,0.00,464.38,'2025-12-03 11:00:00','2025-12-06 16:00:00','2025-12-01 14:30:00'),
(3,'ORD-10003','shipped',49.99,4.00,5.99,59.98,NULL,NULL,'2025-12-05 08:45:00'),
(1,'ORD-10004','delivered',199.99,16.00,0.00,215.99,'2025-12-08 09:00:00','2025-12-11 12:00:00','2025-12-07 11:20:00'),
(4,'ORD-10005','delivered',549.99,44.00,0.00,593.99,'2025-12-10 14:00:00','2025-12-14 10:00:00','2025-12-09 16:00:00'),
(5,'ORD-10006','cancelled',29.99,2.40,5.99,38.38,NULL,NULL,'2025-12-10 10:30:00'),
(6,'ORD-10007','delivered',164.98,13.20,5.99,184.17,'2025-12-12 08:00:00','2025-12-15 11:00:00','2025-12-11 09:00:00'),
(7,'ORD-10008','shipped',309.98,24.80,0.00,334.78,NULL,NULL,'2025-12-15 13:45:00'),
(8,'ORD-10009','processing',79.99,6.40,5.99,92.38,NULL,NULL,'2025-12-18 07:30:00'),
(9,'ORD-10010','delivered',44.99,3.60,5.99,54.58,'2025-12-20 10:00:00','2025-12-23 09:00:00','2025-12-19 15:00:00'),
(10,'ORD-10011','pending',129.99,10.40,0.00,140.39,NULL,NULL,'2025-12-22 11:00:00'),
(2,'ORD-10012','delivered',34.99,2.80,5.99,43.78,'2025-12-24 09:00:00','2025-12-27 14:00:00','2025-12-23 08:15:00'),
(11,'ORD-10013','shipped',89.99,7.20,5.99,103.18,NULL,NULL,'2025-12-28 10:30:00'),
(12,'ORD-10014','delivered',599.98,48.00,0.00,647.98,'2026-01-02 11:00:00','2026-01-05 16:00:00','2026-01-01 09:00:00'),
(3,'ORD-10015','processing',69.99,5.60,5.99,81.58,NULL,NULL,'2026-01-05 14:20:00'),
(13,'ORD-10016','delivered',24.99,2.00,5.99,32.98,'2026-01-08 08:00:00','2026-01-11 10:00:00','2026-01-07 12:00:00'),
(14,'ORD-10017','shipped',159.98,12.80,0.00,172.78,NULL,NULL,'2026-01-12 09:45:00'),
(4,'ORD-10018','delivered',39.99,3.20,5.99,49.18,'2026-01-15 10:00:00','2026-01-18 15:00:00','2026-01-14 16:30:00'),
(15,'ORD-10019','pending',249.98,20.00,0.00,269.98,NULL,NULL,'2026-01-20 08:00:00'),
(1,'ORD-10020','delivered',109.99,8.80,0.00,118.79,'2026-01-23 12:00:00','2026-01-26 09:00:00','2026-01-22 10:15:00'),
(16,'ORD-10021','shipped',449.99,36.00,0.00,485.99,NULL,NULL,'2026-01-28 11:30:00'),
(6,'ORD-10022','delivered',59.99,4.80,5.99,70.78,'2026-02-01 09:00:00','2026-02-04 14:00:00','2026-01-31 13:00:00'),
(17,'ORD-10023','processing',14.99,1.20,5.99,22.18,NULL,NULL,'2026-02-05 07:45:00'),
(7,'ORD-10024','delivered',199.99,16.00,0.00,215.99,'2026-02-08 10:00:00','2026-02-11 12:00:00','2026-02-07 09:30:00'),
(18,'ORD-10025','pending',89.99,7.20,5.99,103.18,NULL,NULL,'2026-02-12 15:00:00'),
(9,'ORD-10026','delivered',549.99,44.00,0.00,593.99,'2026-02-15 11:00:00','2026-02-18 16:00:00','2026-02-14 08:20:00'),
(19,'ORD-10027','cancelled',34.99,2.80,5.99,43.78,NULL,NULL,'2026-02-18 10:00:00'),
(20,'ORD-10028','shipped',129.99,10.40,0.00,140.39,NULL,NULL,'2026-02-22 14:15:00'),
(11,'ORD-10029','delivered',79.99,6.40,5.99,92.38,'2026-02-25 09:00:00','2026-02-28 11:00:00','2026-02-24 12:00:00'),
(2,'ORD-10030','processing',269.98,21.60,0.00,291.58,NULL,NULL,'2026-03-01 08:30:00');

-- -------------------------------------------------------
-- Order Items
-- -------------------------------------------------------
CREATE TABLE order_items (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    order_id    INT NOT NULL,
    product_id  INT NOT NULL,
    quantity    INT NOT NULL DEFAULT 1,
    unit_price  DECIMAL(10,2) NOT NULL,
    line_total  DECIMAL(10,2) NOT NULL,
    created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (order_id) REFERENCES orders(id),
    FOREIGN KEY (product_id) REFERENCES products(id)
);

INSERT INTO order_items (order_id, product_id, quantity, unit_price, line_total) VALUES
(1,2,1,89.99,89.99),
(2,4,1,399.99,399.99),(2,1,1,29.99,29.99),
(3,3,1,49.99,49.99),
(4,7,1,199.99,199.99),
(5,9,1,549.99,549.99),
(6,1,1,29.99,29.99),
(7,2,1,89.99,89.99),(7,5,1,34.99,34.99),(7,15,1,24.99,24.99),(7,16,1,14.99,14.99),
(8,7,1,199.99,199.99),(8,14,1,109.99,109.99),
(9,12,1,79.99,79.99),
(10,8,1,44.99,44.99),
(11,19,1,129.99,129.99),
(12,5,1,34.99,34.99),
(13,2,1,89.99,89.99),
(14,9,1,549.99,549.99),(14,3,1,49.99,49.99),
(15,13,1,69.99,69.99),
(16,15,1,24.99,24.99),
(17,6,1,59.99,59.99),(17,10,1,39.99,39.99),(17,11,1,19.99,19.99),(17,16,1,14.99,14.99),(17,18,1,34.99,34.99),
(18,10,1,39.99,39.99),
(19,7,1,199.99,199.99),(19,3,1,49.99,49.99),
(20,14,1,109.99,109.99),
(21,4,1,399.99,399.99),(21,3,1,49.99,49.99),
(22,6,1,59.99,59.99),
(23,16,1,14.99,14.99),
(24,7,1,199.99,199.99),
(25,2,1,89.99,89.99),
(26,9,1,549.99,549.99),
(27,5,1,34.99,34.99),
(28,19,1,129.99,129.99),
(29,12,1,79.99,79.99),
(30,1,1,29.99,29.99),(30,8,1,44.99,44.99),(30,7,1,199.99,199.99);

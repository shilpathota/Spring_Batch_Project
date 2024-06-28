CREATE TABLE IF NOT EXISTS customer_info
(
    customer_id         SERIAL PRIMARY KEY,
    first_name VARCHAR(255),
    last_name  VARCHAR(255),
    gender VARCHAR(20),
    email_id VARCHAR(255),
    country VARCHAR(50)
);
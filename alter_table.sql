---------------/* orders_table */------------
-- Finds lengths of longest value in the column to be used in data casting VARCHAR
SELECT length(max(cast(card_number as Text)))
FROM orders_table
GROUP BY card_number
ORDER BY length(max(cast(card_number as Text))) desc
LIMIT 1;
-- largest number = 19

SELECT length(max(cast(store_code as Text)))
FROM orders_table
GROUP BY store_code
ORDER BY length(max(cast(store_code as Text))) desc
LIMIT 1; -- = to 12

SELECT length(max(cast(product_code as Text)))
FROM orders_table
GROUP BY product_code
ORDER BY length(max(cast(product_code as Text))) desc
LIMIT 1; --  = to 11

    ALTER TABLE orders_table ALTER COLUMN product_quantity TYPE SMALLINT;
    ALTER TABLE orders_table ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid;
    ALTER TABLE orders_table ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid;
    ALTER TABLE orders_table ALTER COLUMN card_number TYPE varchar (19);
    ALTER TABLE orders_table ALTER COLUMN store_code TYPE varchar (12);
    ALTER TABLE orders_table ALTER COLUMN product_code TYPE varchar (11);

---------------/* dim_users */------------
-- Finds max length of country_code in dim_users
SELECT length(max(cast(country_code as Text)))
FROM dim_users
GROUP BY country_code
ORDER BY length(max(cast(country_code as Text))) desc
LIMIT 1; -- = to 3

-- 'join_date' converted in data_cleaning
    ALTER TABLE dim_users ALTER COLUMN date_of_birth TYPE DATE USING CAST(date_of_birth AS DATE);
    ALTER TABLE dim_users ALTER COLUMN first_name TYPE varchar (255);
    ALTER TABLE dim_users ALTER COLUMN last_name TYPE varchar (255);
    ALTER TABLE dim_users ALTER COLUMN country_code TYPE varchar (3);
    ALTER TABLE dim_users ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid; 

--------------/* dim_store_details */------------------
-- Finds lengths of longest value in the column to be used in data casting VARCHAR
SELECT length(max(cast(country_code as Text)))
FROM dim_store_details
GROUP BY country_code
ORDER BY length(max(cast(country_code as Text))) desc
LIMIT 1; -- = to 2


SELECT length(max(cast(store_code as Text)))
FROM dim_store_details
GROUP BY store_code
ORDER BY length(max(cast(store_code as Text))) desc
LIMIT 1; -- = to 11

-- Updates NA values to NULL
UPDATE dim_store_details 
SET address = NULL
WHERE address = 'N/A';

UPDATE dim_store_details 
SET locality = NULL
WHERE locality = 'N/A';



-- 'opening_date' converted in data_cleaning
    ALTER TABLE dim_store_details ALTER COLUMN longitude TYPE float USING longitude::double precision;
    ALTER TABLE dim_store_details ALTER COLUMN locality TYPE varchar (255);
    ALTER TABLE dim_store_details ALTER COLUMN store_code TYPE varchar (12);
    ALTER TABLE dim_store_details ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::integer;
    ALTER TABLE dim_store_details ALTER COLUMN store_type TYPE varchar (255);
    ALTER TABLE dim_store_details ALTER COLUMN latitude TYPE float USING longitude::double precision;
    ALTER TABLE dim_store_details ALTER COLUMN country_code TYPE varchar (2);
    ALTER TABLE dim_store_details ALTER COLUMN continent TYPE varchar (255); 

--------------/* dim_products */--------------------
-- 'date_added' changed to date in data_cleaning 
-- Removes the pound sign in the product price column
UPDATE dim_products
SET product_price = REPLACE(product_price, '£', '');

-- Alters the column to a float type
ALTER TABLE dim_products 
	ALTER COLUMN weight TYPE FLOAT USING CAST(weight as FLOAT),
	ADD COLUMN weight_class VARCHAR;

-- Rename column in dim_products    
ALTER TABLE dim_products
        RENAME COLUMN stil_available to still_available; 

-- Adds text categories based on the weights of the products
UPDATE dim_products
SET weight_class =
        CASE 
                WHEN weight < 2.0 THEN 'Light'
                WHEN weight >= 2
                        AND weight < 40 THEN 'Mid_Sized'
                WHEN weight >= 40
                        AND weight < 140 THEN 'Heavy' 
                WHEN weight >= 140 THEN 'Truck_Required'
        END;  

    SELECT length(max(product_code)) 
    FROM dim_products
    GROUP BY product_code
    ORDER BY length(max(product_code)) DESC
    LIMIT 1; -- = 11

    SELECT length(max(weight_class))
    FROM dim_products
    GROUP BY weight_class
    ORDER BY length(max(weight_class)) DESC
    LIMIT 2; -- = to 14


SELECT length(max("EAN"))
FROM dim_products
GROUP BY "EAN"
ORDER BY length(max("EAN")) desc
LIMIT 1; -- = to 17
                           

-- Permenantly alters data types in the dim_products table
    ALTER TABLE dim_products ALTER COLUMN product_price TYPE FLOAT USING CAST(product_price as FLOAT);
    ALTER TABLE dim_products ALTER COLUMN product_code TYPE varchar (11);
    ALTER TABLE dim_products ALTER COLUMN uuid TYPE uuid USING uuid::uuid;
    ALTER TABLE dim_products ALTER COLUMN weight_class TYPE varchar (14); 
    ALTER TABLE dim_products ALTER COLUMN still_available TYPE boolean USING (still_available ='Still_available');
    ALTER TABLE dim_products ALTER COLUMN "EAN" TYPE varchar (17);

-- Updates the dim_date_times table data types

SELECT length(max(month ))
FROM dim_date_times
GROUP BY month
ORDER BY length(max(month)) desc
LIMIT 1; -- = to 2


SELECT length(max(year))
FROM dim_date_times
GROUP BY year
ORDER BY length(max(year)) desc
LIMIT 1; -- = to 4

SELECT length(max(day))
FROM dim_date_times
GROUP BY day
ORDER BY length(max(day)) desc
LIMIT 1; -- = to 2

SELECT length(max(time_period))
FROM dim_date_times
GROUP BY time_period
ORDER BY length(max(time_period)) desc
LIMIT 1; -- = to 10    


-- Alters dim_date_times 
	ALTER TABLE dim_date_times ALTER COLUMN month TYPE VARCHAR(2);
	ALTER TABLE dim_date_times ALTER COLUMN year TYPE VARCHAR(4);
	ALTER TABLE dim_date_times ALTER COLUMN day TYPE VARCHAR(2);
	ALTER TABLE dim_date_times ALTER COLUMN time_period TYPE VARCHAR(10);
	ALTER TABLE dim_date_times ALTER COLUMN date_uuid TYPE UUID USING CAST(date_uuid as UUID); 

-- Updating the dim_card_details

SELECT length(max(card_number))
FROM dim_card_details
GROUP BY card_number
ORDER BY length(max(card_number)) desc
LIMIT 1; -- = to 22

SELECT length(max(expiry_date))
FROM dim_card_details
GROUP BY expiry_date
ORDER BY length(max(expiry_date)) desc
LIMIT 1; -- = 10


	ALTER TABLE dim_card_details ALTER COLUMN card_number TYPE VARCHAR(22);
	ALTER TABLE dim_card_details ALTER COLUMN expiry_date TYPE VARCHAR(10);
	ALTER TABLE dim_card_details ALTER COLUMN date_payment_confirmed TYPE DATE USING CAST(date_payment_confirmed as DATE);

---------------/* Adding primary keys to all dim_ tables */------------
    ALTER TABLE dim_card_details ADD PRIMARY KEY (card_number);
    ALTER TABLE dim_date_times ADD PRIMARY KEY (date_uuid);
    ALTER TABLE dim_products ADD PRIMARY KEY (product_code);
    ALTER TABLE dim_store_details ADD PRIMARY KEY (store_code);
    ALTER TABLE dim_users ADD PRIMARY KEY (user_uuid);

-- Finds all card_numbers in orders_table that are not in dim_card_details
SELECT orders_table.card_number 
FROM orders_table
LEFT JOIN dim_card_details
ON orders_table.card_number = dim_card_details.card_number
WHERE dim_card_details.card_number IS NULL;

-- Inserts all card_numbers from orders_table not present in dim_card_details initally, into dim_card_details
INSERT INTO dim_card_details (card_number)
SELECT DISTINCT orders_table.card_number
FROM orders_table
WHERE orders_table.card_number NOT IN 
	(SELECT dim_card_details.card_number
	FROM dim_card_details);

SELECT orders_table.product_code 
FROM orders_table
LEFT JOIN dim_products
ON orders_table.product_code = dim_products.product_code
WHERE dim_products.product_code IS NULL;

INSERT INTO dim_products (product_code)
SELECT DISTINCT orders_table.product_code
FROM orders_table
WHERE orders_table.product_code NOT IN 
	(SELECT dim_products.product_code
	FROM dim_products);

SELECT orders_table.store_code
FROM orders_table
LEFT JOIN dim_store_details
ON orders_table.store_code = dim_store_details.store_code
WHERE dim_store_details.store_code IS NULL;

INSERT INTO dim_store_details (store_code)
SELECT DISTINCT orders_table.store_code
FROM orders_table
WHERE orders_table.store_code NOT IN 
	(SELECT dim_store_details.store_code
	FROM dim_store_details); 

SELECT orders_table.product_code 
FROM orders_table
LEFT JOIN dim_products
ON orders_table.product_code = dim_products.product_code
WHERE dim_products.product_code IS NULL;

INSERT INTO dim_users (user_uuid)
SELECT DISTINCT orders_table.user_uuid
FROM orders_table
WHERE orders_table.user_uuid NOT IN 
	(SELECT dim_users.user_uuid
	FROM dim_users); 

----------------/* Adding foreign keys to orders table */---------
-- adss the foreign keys to the orders table
ALTER TABLE orders_table
	ADD FOREIGN KEY (card_number)
	REFERENCES dim_card_details(card_number);
	
ALTER TABLE orders_table
	ADD FOREIGN KEY (date_uuid)
	REFERENCES dim_date_times(date_uuid);
	
ALTER TABLE orders_table
	ADD FOREIGN KEY (product_code)
	REFERENCES dim_products(product_code);
	
ALTER TABLE orders_table
	ADD FOREIGN KEY (store_code)
	REFERENCES dim_store_details(store_code);
	
ALTER TABLE orders_table
	ADD FOREIGN KEY (user_uuid)
	REFERENCES dim_users(user_uuid);
	       

           
SELECT *

FROM INFORMATION_SCHEMA.COLUMNS

WHERE TABLE_NAME = 'orders_table'



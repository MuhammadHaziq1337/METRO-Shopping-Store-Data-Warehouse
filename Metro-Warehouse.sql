drop database if exists metro;
create database metro;
use metro;
-- Drop tables in correct order
DROP TABLE IF EXISTS FACT_SALES;
DROP TABLE IF EXISTS TRANSACTIONS;
DROP TABLE IF EXISTS DIM_DATE;
DROP TABLE IF EXISTS DIM_PRODUCT;
DROP TABLE IF EXISTS DIM_STORE;
DROP TABLE IF EXISTS DIM_CUSTOMER;
-- Create dimension tables
CREATE TABLE DIM_CUSTOMER (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    gender VARCHAR(10) CHECK (gender IN ('Male', 'Female'))
);
CREATE TABLE DIM_STORE (
    store_id INT PRIMARY KEY,
    store_name VARCHAR(100) NOT NULL,
    location VARCHAR(100)
);
CREATE TABLE DIM_PRODUCT (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    product_price DECIMAL(10,2) NOT NULL,
    supplier_id INT NOT NULL,
    supplier_name VARCHAR(100) NOT NULL,
    store_id INT,
    FOREIGN KEY (store_id) REFERENCES DIM_STORE(store_id)
);
CREATE TABLE DIM_DATE (
    date_id INT PRIMARY KEY AUTO_INCREMENT,
    full_date DATE NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    day_of_month INT NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR(10) NOT NULL,
    is_weekend BOOLEAN GENERATED ALWAYS AS (CASE WHEN day_of_week IN (1, 7) THEN TRUE ELSE FALSE END) STORED,
    CONSTRAINT date_range CHECK (full_date >= '2019-01-01' AND full_date <= '2024-12-31')
);
-- Create TRANSACTIONS table for streaming data
CREATE TABLE TRANSACTIONS (
    order_id INT PRIMARY KEY,
    order_date DATE NOT NULL,
    product_id INT NOT NULL,
    customer_id INT NOT NULL,
    quantity INT NOT NULL,
    FOREIGN KEY (product_id) REFERENCES DIM_PRODUCT(product_id),
    FOREIGN KEY (customer_id) REFERENCES DIM_CUSTOMER(customer_id)
);
-- Create fact table
CREATE TABLE FACT_SALES (
    sale_id INT PRIMARY KEY AUTO_INCREMENT,
    order_id VARCHAR(20) NOT NULL,
    order_date DATE NOT NULL,
    date_id INT,
    customer_id INT,
    product_id INT,
    store_id INT,
    quantity INT NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(10,2) NOT NULL CHECK (unit_price > 0),
    total_sale DECIMAL(10,2), -- Remove the GENERATED ALWAYS part
    FOREIGN KEY (date_id) REFERENCES DIM_DATE(date_id),
    FOREIGN KEY (customer_id) REFERENCES DIM_CUSTOMER(customer_id),
    FOREIGN KEY (product_id) REFERENCES DIM_PRODUCT(product_id),
    FOREIGN KEY (store_id) REFERENCES DIM_STORE(store_id)
);

-- Create indexes
CREATE INDEX idx_fact_customer ON FACT_SALES(customer_id);
CREATE INDEX idx_fact_product ON FACT_SALES(product_id);
CREATE INDEX idx_fact_store ON FACT_SALES(store_id);
CREATE INDEX idx_fact_date ON FACT_SALES(date_id);
CREATE INDEX idx_product_store ON DIM_PRODUCT(store_id);
CREATE INDEX idx_fact_order ON FACT_SALES(order_id);
CREATE INDEX idx_date_weekend ON DIM_DATE(is_weekend);
show tables;

SELECT COUNT(*) as total_transactions FROM FACT_SALES;


-- Q1 : Top Revenue-Generating Products on Weekdays and Weekends --
WITH MonthlyProductSales AS (
    SELECT 
        YEAR(fs.order_date) as sale_year,
        MONTH(fs.order_date) as sale_month,
        dd.is_weekend,
        dp.product_id,
        dp.product_name,
        SUM(fs.total_sale) as total_revenue,
        RANK() OVER (
            PARTITION BY YEAR(fs.order_date), MONTH(fs.order_date), dd.is_weekend 
            ORDER BY SUM(fs.total_sale) DESC
        ) as revenue_rank
    FROM FACT_SALES fs
    JOIN DIM_PRODUCT dp ON fs.product_id = dp.product_id
    JOIN DIM_DATE dd ON fs.date_id = dd.date_id
    GROUP BY 
        YEAR(fs.order_date),
        MONTH(fs.order_date),
        dd.is_weekend,
        dp.product_id,
        dp.product_name
)
SELECT 
    sale_year,
    sale_month,
    CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END as day_type,
    product_name,
    ROUND(total_revenue, 2) as total_revenue
FROM MonthlyProductSales
WHERE revenue_rank <= 5
ORDER BY 
    sale_year,
    sale_month,
    is_weekend,
    total_revenue DESC;
  
SELECT DISTINCT YEAR(order_date) as available_years 
FROM FACT_SALES 
ORDER BY YEAR(order_date);  


-- Q2 : Trend Analysis of Store Revenue Growth Rate for 2019 --
WITH QuarterlyRevenue AS (
    SELECT 
        ds.store_id,
        ds.store_name,
        YEAR(fs.order_date) as year,
        QUARTER(fs.order_date) as quarter,
        SUM(fs.total_sale) as quarterly_revenue,
        LAG(SUM(fs.total_sale)) OVER (
            PARTITION BY ds.store_id 
            ORDER BY YEAR(fs.order_date), QUARTER(fs.order_date)
        ) as prev_quarter_revenue
    FROM FACT_SALES fs
    JOIN DIM_STORE ds ON fs.store_id = ds.store_id
    WHERE YEAR(fs.order_date) = 2019
    GROUP BY 
        ds.store_id,
        ds.store_name,
        YEAR(fs.order_date),
        QUARTER(fs.order_date)
)
SELECT 
    store_name,
    quarter,
    ROUND(quarterly_revenue, 2) as quarterly_revenue,
    ROUND(prev_quarter_revenue, 2) as prev_quarter_revenue,
    CASE 
        WHEN prev_quarter_revenue IS NULL OR prev_quarter_revenue = 0 THEN NULL
        ELSE ROUND(((quarterly_revenue - prev_quarter_revenue) / prev_quarter_revenue * 100), 2)
    END as growth_rate_percentage
FROM QuarterlyRevenue
ORDER BY 
    store_name,
    quarter;
    
-- Q3 : Detailed Supplier Sales Contribution by Store and Product:--

WITH SupplierSales AS (
    SELECT 
        ds.store_id,
        ds.store_name,
        dp.supplier_id,
        dp.supplier_name,
        dp.product_id,
        dp.product_name,
        SUM(fs.total_sale) as total_sales,
        SUM(SUM(fs.total_sale)) OVER (PARTITION BY ds.store_id) as store_total_sales
    FROM FACT_SALES fs
    JOIN DIM_STORE ds ON fs.store_id = ds.store_id
    JOIN DIM_PRODUCT dp ON fs.product_id = dp.product_id
    GROUP BY 
        ds.store_id,
        ds.store_name,
        dp.supplier_id,
        dp.supplier_name,
        dp.product_id,
        dp.product_name
)
SELECT 
    store_name,
    supplier_name,
    product_name,
    ROUND(total_sales, 2) as total_sales,
    ROUND((total_sales / store_total_sales * 100), 2) as contribution_percentage
FROM SupplierSales
ORDER BY 
    store_name,
    supplier_name,
    total_sales DESC;
    
-- Q4. Seasonal Analysis of Product Sales Using Dynamic Drill-Down
WITH SeasonalSales AS (
    SELECT 
        dp.product_id,
        dp.product_name,
        CASE 
            WHEN MONTH(fs.order_date) IN (3, 4, 5) THEN 'Spring'
            WHEN MONTH(fs.order_date) IN (6, 7, 8) THEN 'Summer'
            WHEN MONTH(fs.order_date) IN (9, 10, 11) THEN 'Fall'
            ELSE 'Winter'
        END as season,
        SUM(fs.total_sale) as total_sales,
        COUNT(*) as number_of_sales
    FROM FACT_SALES fs
    JOIN DIM_PRODUCT dp ON fs.product_id = dp.product_id
    GROUP BY 
        dp.product_id,
        dp.product_name,
        CASE 
            WHEN MONTH(fs.order_date) IN (3, 4, 5) THEN 'Spring'
            WHEN MONTH(fs.order_date) IN (6, 7, 8) THEN 'Summer'
            WHEN MONTH(fs.order_date) IN (9, 10, 11) THEN 'Fall'
            ELSE 'Winter'
        END
)
SELECT 
    product_name,
    season,
    ROUND(total_sales, 2) as total_sales,
    number_of_sales,
    ROUND(total_sales / number_of_sales, 2) as avg_sale_per_transaction
FROM SeasonalSales
ORDER BY 
    product_name,
    CASE season
        WHEN 'Spring' THEN 1
        WHEN 'Summer' THEN 2
        WHEN 'Fall' THEN 3
        WHEN 'Winter' THEN 4
    END;
    
-- Q5: Store-Wise and Supplier-Wise Monthly Revenue Volatility
WITH MonthlyRevenue AS (
    SELECT 
        ds.store_id,  -- Added store_id
        ds.store_name,
        dp.supplier_id,  -- Added supplier_id
        dp.supplier_name,
        DATE_FORMAT(fs.order_date, '%Y-%m') as sales_month,
        SUM(fs.total_sale) as monthly_revenue,
        LAG(SUM(fs.total_sale)) OVER (
            PARTITION BY ds.store_id, dp.supplier_id 
            ORDER BY DATE_FORMAT(fs.order_date, '%Y-%m')
        ) as prev_month_revenue
    FROM FACT_SALES fs
    JOIN DIM_STORE ds ON fs.store_id = ds.store_id
    JOIN DIM_PRODUCT dp ON fs.product_id = dp.product_id
    GROUP BY 
        ds.store_id,      
        ds.store_name,
        dp.supplier_id,   
        dp.supplier_name,
        DATE_FORMAT(fs.order_date, '%Y-%m')
)
SELECT 
    store_name,
    supplier_name,
    sales_month,
    ROUND(monthly_revenue, 2) as monthly_revenue,
    ROUND(prev_month_revenue, 2) as prev_month_revenue,
    CASE 
        WHEN prev_month_revenue IS NULL OR prev_month_revenue = 0 THEN NULL
        ELSE ROUND(ABS((monthly_revenue - prev_month_revenue) / prev_month_revenue * 100), 2)
    END as volatility_percentage
FROM MonthlyRevenue
ORDER BY 
    store_name,
    supplier_name,
    sales_month;


-- Q6: Products frequently purchased 
WITH SameHourPurchases AS (
    SELECT 
        p1.product_id as product1_id,
        p1.product_name as product1_name,
        p2.product_id as product2_id,
        p2.product_name as product2_name,
        COUNT(*) as purchase_frequency,
        MIN(ABS(TIMESTAMPDIFF(MINUTE, fs1.order_date, fs2.order_date))) as min_time_diff
    FROM FACT_SALES fs1
    JOIN FACT_SALES fs2 
        ON DATE(fs1.order_date) = DATE(fs2.order_date)  -- Same day
        AND HOUR(fs1.order_date) = HOUR(fs2.order_date)  -- Same hour
        AND fs1.order_id <> fs2.order_id  -- Different orders
        AND fs1.product_id < fs2.product_id  -- Avoid duplicates
    JOIN DIM_PRODUCT p1 ON fs1.product_id = p1.product_id
    JOIN DIM_PRODUCT p2 ON fs2.product_id = p2.product_id
    GROUP BY 
        p1.product_id,
        p1.product_name,
        p2.product_id,
        p2.product_name
)
SELECT 
    product1_name as "Product 1",
    product2_name as "Product 2",
    purchase_frequency as "Times Purchased Together",
    min_time_diff as "Minimum Minutes Apart",
    ROUND(
        purchase_frequency * 100.0 / (
            SELECT COUNT(*) FROM FACT_SALES
        ),
        2
    ) as "Percentage of Total Orders"
FROM SameHourPurchases
WHERE purchase_frequency > 1  -- At least purchased together twice
ORDER BY purchase_frequency DESC, min_time_diff ASC
LIMIT 5;

-- Q7 -- : Yearly Revenue Trends with ROLLUP
SELECT 
    COALESCE(YEAR(fs.order_date), 'Total') as year,
    COALESCE(ds.store_name, 'All Stores') as store_name,
    COALESCE(dp.supplier_name, 'All Suppliers') as supplier_name,
    COALESCE(dp.product_name, 'All Products') as product_name,
    ROUND(SUM(fs.total_sale), 2) as total_revenue,
    COUNT(DISTINCT fs.order_id) as number_of_orders,
    ROUND(SUM(fs.total_sale) / COUNT(DISTINCT fs.order_id), 2) as avg_order_value
FROM FACT_SALES fs
JOIN DIM_STORE ds ON fs.store_id = ds.store_id
JOIN DIM_PRODUCT dp ON fs.product_id = dp.product_id
GROUP BY 
    YEAR(fs.order_date),
    ds.store_name,
    dp.supplier_name,
    dp.product_name
WITH ROLLUP
HAVING year IS NOT NULL  -- To exclude the grand total row
ORDER BY 
    year,
    store_name,
    supplier_name,
    product_name;

-- Verification queries to ensure data quality
SELECT 'Total Orders' as metric, COUNT(DISTINCT order_id) as value
FROM FACT_SALES
UNION ALL
SELECT 'Total Products', COUNT(DISTINCT product_id)
FROM FACT_SALES
UNION ALL
SELECT 'Date Range', CONCAT(MIN(order_date), ' to ', MAX(order_date))
FROM FACT_SALES;


-- Q8 -- : Half-Yearly Revenue and Volume Analysis
WITH HalfYearlySales AS (
    SELECT 
        dp.product_id,
        dp.product_name,
        YEAR(fs.order_date) as sale_year,
        CASE 
            WHEN MONTH(fs.order_date) <= 6 THEN 'H1'
            ELSE 'H2'
        END as half_year,
        SUM(fs.quantity) as total_quantity,
        SUM(fs.total_sale) as total_revenue
    FROM FACT_SALES fs
    JOIN DIM_PRODUCT dp ON fs.product_id = dp.product_id
    GROUP BY 
        dp.product_id,
        dp.product_name,
        YEAR(fs.order_date),
        CASE 
            WHEN MONTH(fs.order_date) <= 6 THEN 'H1'
            ELSE 'H2'
        END
)
SELECT 
    product_name,
    sale_year,
    SUM(CASE WHEN half_year = 'H1' THEN total_quantity ELSE 0 END) as h1_quantity,
    SUM(CASE WHEN half_year = 'H1' THEN total_revenue ELSE 0 END) as h1_revenue,
    SUM(CASE WHEN half_year = 'H2' THEN total_quantity ELSE 0 END) as h2_quantity,
    SUM(CASE WHEN half_year = 'H2' THEN total_revenue ELSE 0 END) as h2_revenue,
    SUM(total_quantity) as yearly_quantity,
    SUM(total_revenue) as yearly_revenue,
    ROUND(
        ((SUM(CASE WHEN half_year = 'H2' THEN total_revenue ELSE 0 END) - 
          SUM(CASE WHEN half_year = 'H1' THEN total_revenue ELSE 0 END)) / 
         NULLIF(SUM(CASE WHEN half_year = 'H1' THEN total_revenue ELSE 0 END), 0) * 100
        ), 2) as revenue_growth_percentage
FROM HalfYearlySales
GROUP BY 
    product_name,
    sale_year
ORDER BY 
    sale_year,
    yearly_revenue DESC;

-- Q9-- : Revenue Spikes and Outliers Analysis
WITH DailyProductStats AS (
    SELECT 
        dp.product_id,
        dp.product_name,
        fs.order_date,
        SUM(fs.total_sale) as daily_sales,
        AVG(SUM(fs.total_sale)) OVER (
            PARTITION BY dp.product_id
        ) as avg_daily_sales,
        STDDEV(SUM(fs.total_sale)) OVER (
            PARTITION BY dp.product_id
        ) as stddev_daily_sales
    FROM FACT_SALES fs
    JOIN DIM_PRODUCT dp ON fs.product_id = dp.product_id
    GROUP BY 
        dp.product_id,
        dp.product_name,
        fs.order_date
)
SELECT 
    product_name,
    order_date,
    ROUND(daily_sales, 2) as daily_sales,
    ROUND(avg_daily_sales, 2) as average_daily_sales,
    ROUND((daily_sales - avg_daily_sales) / stddev_daily_sales, 2) as z_score,
    CASE 
        WHEN daily_sales > (2 * avg_daily_sales) THEN 'High Spike'
        WHEN daily_sales < (0.5 * avg_daily_sales) THEN 'Low Spike'
        ELSE 'Normal'
    END as sales_pattern,
    ROUND((daily_sales / avg_daily_sales - 1) * 100, 2) as percentage_deviation
FROM DailyProductStats
WHERE daily_sales > (2 * avg_daily_sales)  -- Show only significant spikes
ORDER BY 
    percentage_deviation DESC;

-- Q10 -- : Create View for Store Quarterly Sales
DROP VIEW IF EXISTS STORE_QUARTERLY_SALES;
CREATE VIEW STORE_QUARTERLY_SALES AS
WITH QuarterlyMetrics AS (
    SELECT 
        ds.store_id,
        ds.store_name,
        YEAR(fs.order_date) as sale_year,
        QUARTER(fs.order_date) as quarter,
        SUM(fs.total_sale) as quarterly_revenue,
        COUNT(DISTINCT fs.order_id) as number_of_orders,
        SUM(fs.quantity) as total_units_sold,
        SUM(fs.total_sale) / COUNT(DISTINCT fs.order_id) as avg_order_value,
        COUNT(DISTINCT dp.supplier_id) as number_of_suppliers,
        COUNT(DISTINCT fs.product_id) as number_of_products
    FROM FACT_SALES fs
    JOIN DIM_STORE ds ON fs.store_id = ds.store_id
    JOIN DIM_PRODUCT dp ON fs.product_id = dp.product_id
    GROUP BY 
        ds.store_id,
        ds.store_name,
        YEAR(fs.order_date),
        QUARTER(fs.order_date)
)
SELECT 
    store_name,
    sale_year,
    quarter,
    ROUND(quarterly_revenue, 2) as quarterly_revenue,
    number_of_orders,
    total_units_sold,
    ROUND(avg_order_value, 2) as avg_order_value,
    number_of_suppliers,
    number_of_products,
    ROUND(quarterly_revenue / NULLIF(total_units_sold, 0), 2) as revenue_per_unit,
    ROUND(quarterly_revenue / NULLIF(number_of_orders, 0), 2) as revenue_per_order
FROM QuarterlyMetrics
ORDER BY 
    store_name,
    sale_year,
    quarter;

-- Sample query to test the view
SELECT * FROM STORE_QUARTERLY_SALES
WHERE sale_year = 2019
ORDER BY store_name, quarter;

-- Additional verification queries
-- Check for data distribution
SELECT 
    'Total Orders' as metric, 
    COUNT(DISTINCT order_id) as value
FROM FACT_SALES
UNION ALL
SELECT 
    'Total Days with Sales', 
    COUNT(DISTINCT order_date)
FROM FACT_SALES
UNION ALL
SELECT 
    'Average Daily Orders',
    ROUND(COUNT(DISTINCT order_id) / COUNT(DISTINCT order_date), 2)
FROM FACT_SALES;
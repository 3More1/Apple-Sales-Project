-- Revenue KPIs

CREATE VIEW vw_Total_Revenue AS
SELECT 
    SUM(p.price * s.quantity) AS Total_Revenue
FROM 
    dbo.sales s
JOIN  
    dbo.products p ON s.product_id = p.product_id;




CREATE VIEW vw_Revenue_By_Category AS
SELECT 
    c.category_name,
    SUM(p.price * s.quantity) AS Category_Revenue,
    (SUM(p.price * s.quantity) * 100.0 / (SELECT SUM(p2.price * s2.quantity) 
                                          FROM dbo.sales s2 
                                          JOIN dbo.products p2 ON s2.product_id = p2.product_id)) AS Revenue_Percentage
FROM 
    dbo.sales s
JOIN  
    dbo.products p ON s.product_id = p.product_id
JOIN 
    dbo.category c ON p.category_id = c.category_id
GROUP BY 
    c.category_name;


CREATE VIEW vw_Revenue_By_Product AS
SELECT 
    p.product_name,
    SUM(p.price * s.quantity) AS Product_Revenue,
   (SUM(p.price * s.quantity) * 100.0 / (SELECT SUM(p2.price * s2.quantity) 
                                          FROM dbo.sales s2 
                                          JOIN dbo.products p2 ON s2.product_id = p2.product_id)) AS Revenue_Percentage
FROM 
    dbo.sales s
JOIN  
    dbo.products p ON s.product_id = p.product_id
GROUP BY 
    p.product_name;


CREATE VIEW vw_Revenue_By_Store AS
SELECT 
    st.Store_Name,
    st.City,
    st.Country,
    SUM(p.price * s.quantity) AS Store_Revenue,
    (SUM(p.price * s.quantity) * 100.0 / (SELECT SUM(p2.price * s2.quantity) 
                                          FROM dbo.sales s2 
                                          JOIN dbo.products p2 ON s2.product_id = p2.product_id)) AS Revenue_Percentage
FROM 
    dbo.sales s
JOIN  
    dbo.products p ON s.product_id = p.product_id
JOIN 
    dbo.stores st ON st.Store_ID = s.store_id
GROUP BY 
    st.Store_Name, st.City, st.Country;


CREATE VIEW vw_Revenue_By_Country AS
SELECT 
    st.Country,
    SUM(p.price * s.quantity) AS Country_Revenue,
    (SUM(p.price * s.quantity) * 100.0 / (SELECT SUM(p2.price * s2.quantity) 
                                          FROM dbo.sales s2 
                                          JOIN dbo.products p2 ON s2.product_id = p2.product_id)) AS Revenue_Percentage
FROM 
    dbo.sales s
JOIN  
    dbo.products p ON s.product_id = p.product_id
JOIN 
    dbo.stores st ON st.Store_ID = s.store_id
GROUP BY 
    st.Country;


CREATE VIEW vw_Average_Order_Value AS
SELECT 
    SUM(p.price * s.quantity) / COUNT(DISTINCT s.sale_id) AS Average_Order_Value
FROM 
    dbo.sales s
JOIN 
    dbo.products p ON s.product_id = p.product_id;



CREATE VIEW vw_Monthly_Revenue_Growth AS
WITH MonthlyRevenue AS (
    SELECT 
        FORMAT(s.sale_date, 'yyyy-MM') AS YearMonth,
        SUM(p.price * s.quantity) AS Revenue
    FROM 
        dbo.sales s
    JOIN 
        dbo.products p ON s.product_id = p.product_id
    GROUP BY 
        FORMAT(s.sale_date, 'yyyy-MM')
)
SELECT 
    curr.YearMonth AS Current_Month,
    curr.Revenue AS Current_Revenue,
    prev.YearMonth AS Previous_Month,
    prev.Revenue AS Previous_Revenue,
    CASE 
        WHEN prev.Revenue IS NULL OR prev.Revenue = 0 THEN NULL
        ELSE ((curr.Revenue - prev.Revenue) / prev.Revenue) * 100 
    END AS Growth_Percentage
FROM 
    MonthlyRevenue curr
LEFT JOIN 
    MonthlyRevenue prev ON curr.YearMonth = FORMAT(DATEADD(MONTH, 1, CAST(prev.YearMonth + '-01' AS DATE)), 'yyyy-MM');


-- Example: Get top 5 revenue-generating categories
SELECT TOP 5 
    category_name, 
    Category_Revenue, 
    Revenue_Percentage
FROM 
    vw_Revenue_By_Category
ORDER BY 
    Category_Revenue DESC;



-- Sales Performance KPIs

CREATE VIEW vw_Total_Units_Sold AS
SELECT 
    SUM(s.quantity) AS Total_Units_Sold
FROM 
    dbo.sales s;


CREATE VIEW vw_Units_Sold_By_Category AS
SELECT 
    c.category_name,
    SUM(s.quantity) AS Units_Sold,
    (SUM(s.quantity) * 100.0 / (SELECT SUM(quantity) FROM dbo.sales)) AS Quantity_Percentage
FROM 
    dbo.sales s
JOIN 
    dbo.products p ON s.product_id = p.product_id
JOIN 
    dbo.category c ON p.category_id = c.category_id
GROUP BY 
    c.category_name;



CREATE VIEW vw_Units_Sold_By_Product AS
SELECT 
    p.product_name,
    SUM(s.quantity) AS Units_Sold,
    (SUM(s.quantity) * 100.0 / (SELECT SUM(quantity) FROM dbo.sales)) AS Quantity_Percentage
FROM 
    dbo.sales s
JOIN 
    dbo.products p ON s.product_id = p.product_id
GROUP BY 
    p.product_name;



CREATE VIEW vw_Units_Sold_By_Store AS
SELECT 
    st.Store_Name,
    st.City,
    st.Country,
    SUM(s.quantity) AS Units_Sold,
    (SUM(s.quantity) * 100.0 / (SELECT SUM(quantity) FROM dbo.sales)) AS Quantity_Percentage
FROM 
    dbo.sales s
JOIN 
    dbo.stores st ON s.store_id = st.Store_ID
GROUP BY 
    st.Store_Name, st.City, st.Country;



CREATE VIEW vw_Average_Units_Per_Transaction AS
SELECT 
    SUM(s.quantity) / COUNT(DISTINCT s.sale_id) AS Average_Units_Per_Transaction
FROM 
    dbo.sales s;



CREATE VIEW vw_Transaction_Per_Day_By_Store AS
SELECT 
    st.Store_Name,
    st.City,
    st.Country,
    CAST(s.sale_date AS DATE) AS Sale_Date,
    COUNT(DISTINCT s.sale_id) AS Daily_Transactions
FROM 
    dbo.sales s
JOIN 
    dbo.stores st ON s.store_id = st.Store_ID
GROUP BY 
    st.Store_Name, st.City, st.Country, CAST(s.sale_date AS DATE);


-- Best-Selling Months
CREATE VIEW vw_Monthly_Sales_Performance AS
SELECT 
    YEAR(s.sale_date) AS Year,
    MONTH(s.sale_date) AS Month,
    SUM(s.quantity) AS Units_Sold,
    COUNT(DISTINCT s.sale_id) AS Number_Of_Transactions,
    SUM(p.price * s.quantity) AS Revenue
FROM 
    dbo.sales s
JOIN 
    dbo.products p ON s.product_id = p.product_id
GROUP BY 
    YEAR(s.sale_date), MONTH(s.sale_date);

-- Best-Selling Days of Week
CREATE VIEW vw_DayOfWeek_Sales_Performance AS
SELECT 
    DATENAME(WEEKDAY, s.sale_date) AS Day_Of_Week,
    SUM(s.quantity) AS Units_Sold,
    COUNT(DISTINCT s.sale_id) AS Number_Of_Transactions,
    SUM(p.price * s.quantity) AS Revenue
FROM 
    dbo.sales s
JOIN 
    dbo.products p ON s.product_id = p.product_id
GROUP BY 
    DATENAME(WEEKDAY, s.sale_date);

-- Best-Selling Quarters
CREATE VIEW vw_Quarterly_Sales_Performance AS
SELECT 
    YEAR(s.sale_date) AS Year,
    DATEPART(QUARTER, s.sale_date) AS Quarter,
    SUM(s.quantity) AS Units_Sold,
    COUNT(DISTINCT s.sale_id) AS Number_Of_Transactions,
    SUM(p.price * s.quantity) AS Revenue
FROM 
    dbo.sales s
JOIN 
    dbo.products p ON s.product_id = p.product_id
GROUP BY 
    YEAR(s.sale_date), DATEPART(QUARTER, s.sale_date);


-- Product Performance KPIs
CREATE VIEW vw_Product_Popularity_Ranking AS
SELECT 
    p.product_name,
    SUM(s.quantity) AS Units_Sold,
    RANK() OVER (ORDER BY SUM(s.quantity) DESC) AS Popularity_Rank
FROM 
    dbo.sales s
JOIN 
    dbo.products p ON s.product_id = p.product_id
GROUP BY 
    p.product_name;


CREATE VIEW vw_Product_Profitability AS
SELECT 
    p.product_name,
    SUM(p.price * s.quantity) AS Revenue,
    SUM(p.price * s.quantity * 0.6) AS Estimated_Cost, -- Assuming 60% of price is cost
    SUM(p.price * s.quantity) - SUM(p.price * s.quantity * 0.6) AS Profit,
    ((SUM(p.price * s.quantity) - SUM(p.price * s.quantity * 0.6)) / SUM(p.price * s.quantity)) * 100 AS Profit_Margin_Percentage
FROM 
    dbo.sales s
JOIN 
    dbo.products p ON s.product_id = p.product_id
GROUP BY 
    p.product_name;




CREATE VIEW vw_New_Product_Performance AS
SELECT 
    p.product_name,
    p.category_id,
    p.launch_date,
    SUM(s.quantity) AS Units_Sold,
    COUNT(DISTINCT s.sale_id) AS Number_Of_Transactions,
    SUM(p.price * s.quantity) AS Revenue,
    DATEDIFF(DAY, p.launch_date, MAX(s.sale_date)) AS Days_Since_Launch
FROM 
    dbo.products p
LEFT JOIN 
    dbo.sales s ON p.product_id = s.product_id
WHERE 
    p.launch_date >= DATEADD(MONTH, -12, GETDATE())
GROUP BY 
    p.product_name, p.category_id, p.launch_date;


CREATE VIEW vw_Product_Price_Data_For_Elasticity AS
WITH MonthlyProductData AS (
    SELECT 
        p.product_id,
        p.product_name,
        YEAR(s.sale_date) AS Year,
        MONTH(s.sale_date) AS Month,
        AVG(p.price) AS Avg_Price,
        SUM(s.quantity) AS Monthly_Quantity
    FROM 
        dbo.sales s
    JOIN 
        dbo.products p ON s.product_id = p.product_id
    GROUP BY 
        p.product_id, p.product_name, YEAR(s.sale_date), MONTH(s.sale_date)
)
SELECT 
    curr.product_name,
    curr.Year AS Current_Year,
    curr.Month AS Current_Month,
    curr.Avg_Price AS Current_Price,
    curr.Monthly_Quantity AS Current_Quantity,
    prev.Avg_Price AS Previous_Price,
    prev.Monthly_Quantity AS Previous_Quantity,
    CASE 
        WHEN prev.Avg_Price = curr.Avg_Price THEN NULL
        ELSE ((curr.Monthly_Quantity - prev.Monthly_Quantity) / prev.Monthly_Quantity) / 
             ((curr.Avg_Price - prev.Avg_Price) / prev.Avg_Price)
    END AS Price_Elasticity
FROM 
    MonthlyProductData curr
JOIN 
    MonthlyProductData prev 
    ON curr.product_id = prev.product_id
    AND ((curr.Year = prev.Year AND curr.Month = prev.Month + 1)
         OR (curr.Year = prev.Year + 1 AND curr.Month = 1 AND prev.Month = 12))
WHERE 
    prev.Avg_Price != curr.Avg_Price;  -- Only include when price changed



CREATE VIEW vw_Product_Lifecycle_Performance AS
SELECT 
    p.product_name,
    p.launch_date,
    DATEDIFF(MONTH, p.launch_date, s.sale_date) AS Months_Since_Launch,
    COUNT(DISTINCT s.sale_id) AS Transaction_Count,
    SUM(s.quantity) AS Units_Sold,
    SUM(p.price * s.quantity) AS Revenue
FROM 
    dbo.products p
JOIN 
    dbo.sales s ON p.product_id = s.product_id
WHERE 
    s.sale_date >= p.launch_date
GROUP BY 
    p.product_name, p.launch_date, DATEDIFF(MONTH, p.launch_date, s.sale_date);





CREATE VIEW vw_Seasonal_Product_Performance AS
SELECT 
    p.product_name,
    YEAR(s.sale_date) AS Year,
    MONTH(s.sale_date) AS Month,
    SUM(s.quantity) AS Monthly_Units_Sold,
    SUM(p.price * s.quantity) AS Monthly_Revenue,
    AVG(SUM(s.quantity)) OVER (PARTITION BY p.product_id, MONTH(s.sale_date)) AS Avg_Monthly_Units
FROM 
    dbo.sales s
JOIN 
    dbo.products p ON s.product_id = p.product_id
GROUP BY 
    p.product_name, p.product_id, YEAR(s.sale_date), MONTH(s.sale_date);









-- Store Performance KPIs
CREATE VIEW vw_Store_Revenue_Ranking AS
SELECT 
    st.Store_Name,
    st.City,
    st.Country,
    SUM(p.price * s.quantity) AS Total_Revenue,
    RANK() OVER (ORDER BY SUM(p.price * s.quantity) DESC) AS Revenue_Rank
FROM 
    dbo.sales s
JOIN 
    dbo.products p ON s.product_id = p.product_id
JOIN 
    dbo.stores st ON s.store_id = st.Store_ID
GROUP BY 
    st.Store_Name, st.City, st.Country;


CREATE VIEW vw_Store_YoY_Growth AS
WITH YearlyStoreRevenue AS (
    SELECT 
        st.Store_ID,
        st.Store_Name,
        st.City,
        st.Country,
        YEAR(s.sale_date) AS Year,
        SUM(p.price * s.quantity) AS Annual_Revenue
    FROM 
        dbo.sales s
    JOIN 
        dbo.products p ON s.product_id = p.product_id
    JOIN 
        dbo.stores st ON s.store_id = st.Store_ID
    GROUP BY 
        st.Store_ID, st.Store_Name, st.City, st.Country, YEAR(s.sale_date)
)
SELECT 
    curr.Store_Name,
    curr.City,
    curr.Country,
    curr.Year AS Current_Year,
    curr.Annual_Revenue AS Current_Year_Revenue,
    prev.Year AS Previous_Year,
    prev.Annual_Revenue AS Previous_Year_Revenue,
    CASE 
        WHEN prev.Annual_Revenue IS NULL OR prev.Annual_Revenue = 0 THEN NULL
        ELSE ((curr.Annual_Revenue - prev.Annual_Revenue) / prev.Annual_Revenue) * 100 
    END AS Growth_Percentage
FROM 
    YearlyStoreRevenue curr
LEFT JOIN 
    YearlyStoreRevenue prev ON curr.Store_ID = prev.Store_ID AND curr.Year = prev.Year + 1;



CREATE VIEW vw_Store_Avg_Transaction_Value AS
SELECT 
    st.Store_Name,
    st.City,
    st.Country,
    COUNT(DISTINCT s.sale_id) AS Number_Of_Transactions,
    SUM(p.price * s.quantity) AS Total_Revenue,
    SUM(p.price * s.quantity) / COUNT(DISTINCT s.sale_id) AS Average_Transaction_Value
FROM 
    dbo.sales s
JOIN 
    dbo.products p ON s.product_id = p.product_id
JOIN 
    dbo.stores st ON s.store_id = st.Store_ID
GROUP BY 
    st.Store_Name, st.City, st.Country;




CREATE VIEW vw_Store_Revenue_Per_Staff AS
SELECT 
    st.Store_Name,
    st.City,
    st.Country,
    SUM(p.price * s.quantity) AS Total_Revenue,
    -- Assuming each store has a fixed number of staff for demonstration
    -- In reality, you would join to a staff table if available
    COUNT(DISTINCT DATEPART(MONTH, s.sale_date)) AS Operating_Months,
    SUM(p.price * s.quantity) / NULLIF(COUNT(DISTINCT DATEPART(MONTH, s.sale_date)), 0) AS Monthly_Revenue
FROM 
    dbo.sales s
JOIN 
    dbo.products p ON s.product_id = p.product_id
JOIN 
    dbo.stores st ON s.store_id = st.Store_ID
GROUP BY 
    st.Store_Name, st.City, st.Country;





CREATE VIEW vw_Store_Product_Mix AS
SELECT 
    st.Store_Name,
    st.City,
    st.Country,
    c.category_name,
    COUNT(DISTINCT s.sale_id) AS Transaction_Count,
    SUM(s.quantity) AS Units_Sold,
    SUM(p.price * s.quantity) AS Category_Revenue,
    (SUM(p.price * s.quantity) * 100.0 / SUM(SUM(p.price * s.quantity)) OVER (PARTITION BY st.Store_ID)) AS Category_Percentage
FROM 
    dbo.sales s
JOIN 
    dbo.products p ON s.product_id = p.product_id
JOIN 
    dbo.stores st ON s.store_id = st.Store_ID
JOIN 
    dbo.category c ON p.category_id = c.category_id
GROUP BY 
    st.Store_ID, st.Store_Name, st.City, st.Country, c.category_name;



CREATE VIEW vw_Store_Monthly_Consistency AS
SELECT 
    st.Store_Name,
    st.City,
    st.Country,
    COUNT(DISTINCT Monthly_Sales.YearMonth) AS Months_With_Sales,
    AVG(Monthly_Sales.Sale_Count) AS Avg_Monthly_Sales,
    STDEV(Monthly_Sales.Sale_Count) AS StDev_Monthly_Sales,
    (STDEV(Monthly_Sales.Sale_Count) / NULLIF(AVG(Monthly_Sales.Sale_Count), 0)) * 100 AS Coefficient_Of_Variation
FROM 
    dbo.stores st
CROSS APPLY (
    SELECT 
        FORMAT(s.sale_date, 'yyyy-MM') AS YearMonth,
        COUNT(DISTINCT s.sale_id) AS Sale_Count
    FROM 
        dbo.sales s
    WHERE 
        s.store_id = st.Store_ID
    GROUP BY 
        FORMAT(s.sale_date, 'yyyy-MM')
) AS Monthly_Sales
GROUP BY 
    st.Store_Name, st.City, st.Country;






--Warranty and Service KPIs

CREATE VIEW vw_Warranty_Claim_Rate AS
SELECT 
    COUNT(DISTINCT w.claim_id) AS Total_Claims,
    COUNT(DISTINCT s.sale_id) AS Total_Sales,
    (COUNT(DISTINCT w.claim_id) * 100.0 / NULLIF(COUNT(DISTINCT s.sale_id), 0)) AS Warranty_Claim_Rate
FROM 
    dbo.sales s
LEFT JOIN 
    dbo.warranty w ON s.sale_id = w.sale_id;



CREATE VIEW vw_Warranty_Claim_Rate_By_Product AS
SELECT 
    p.product_name,
    COUNT(DISTINCT w.claim_id) AS Product_Warranty_Claims,
    COUNT(DISTINCT s.sale_id) AS Product_Sales,
    (COUNT(DISTINCT w.claim_id) * 100.0 / NULLIF(COUNT(DISTINCT s.sale_id), 0)) AS Product_Warranty_Claim_Rate
FROM 
    dbo.products p
LEFT JOIN 
    dbo.sales s ON p.product_id = s.product_id
LEFT JOIN 
    dbo.warranty w ON s.sale_id = w.sale_id
GROUP BY 
    p.product_name;



CREATE VIEW vw_Warranty_Claim_Rate_By_Category AS
SELECT 
    c.category_name,
    COUNT(DISTINCT w.claim_id) AS Category_Warranty_Claims,
    COUNT(DISTINCT s.sale_id) AS Category_Sales,
    (COUNT(DISTINCT w.claim_id) * 100.0 / NULLIF(COUNT(DISTINCT s.sale_id), 0)) AS Category_Warranty_Claim_Rate
FROM 
    dbo.category c
LEFT JOIN 
    dbo.products p ON c.category_id = p.category_id
LEFT JOIN 
    dbo.sales s ON p.product_id = s.product_id
LEFT JOIN 
    dbo.warranty w ON s.sale_id = w.sale_id
GROUP BY 
    c.category_name;


CREATE VIEW vw_Average_Time_To_Claim AS
SELECT 
    AVG(DATEDIFF(DAY, s.sale_date, w.claim_date)) AS Average_Days_To_Claim
FROM 
    dbo.warranty w
JOIN 
    dbo.sales s ON w.sale_id = s.sale_id
WHERE 
    w.claim_date >= s.sale_date; -- To exclude potential data errors


CREATE VIEW vw_Average_Time_To_Claim_By_Product AS
SELECT 
    p.product_name,
    AVG(DATEDIFF(DAY, s.sale_date, w.claim_date)) AS Average_Days_To_Claim,
    COUNT(w.claim_id) AS Total_Claims
FROM 
    dbo.warranty w
JOIN 
    dbo.sales s ON w.sale_id = s.sale_id
JOIN 
    dbo.products p ON s.product_id = p.product_id
WHERE 
    w.claim_date >= s.sale_date -- To exclude potential data errors
GROUP BY 
    p.product_name;


CREATE VIEW vw_Repair_Status_Distribution AS
SELECT 
    repair_status,
    COUNT(*) AS Claim_Count,
    (COUNT(*) * 100.0 / (SELECT COUNT(*) FROM dbo.warranty)) AS Percentage
FROM 
    dbo.warranty
GROUP BY 
    repair_status;


CREATE VIEW vw_Warranty_Cost_Ratio AS
WITH WarrantyCosts AS (
    SELECT
        -- Estimating costs based on repair status
        CASE
            WHEN repair_status = 'Warranty Void' THEN 0
            WHEN repair_status = 'Paid Repaired' THEN 50  -- Estimated service cost
            WHEN repair_status = 'Free Replaced' THEN 
                (SELECT AVG(p.price * 0.7) FROM dbo.products p) -- Estimated replacement cost as 70% of avg price
            ELSE 25 -- Default cost for other statuses
        END AS Estimated_Cost,
        w.claim_id
    FROM
        dbo.warranty w
)
SELECT
    SUM(wc.Estimated_Cost) AS Total_Estimated_Warranty_Costs,
    (SELECT SUM(p.price * s.quantity) FROM dbo.sales s JOIN dbo.products p ON s.product_id = p.product_id) AS Total_Revenue,
    (SUM(wc.Estimated_Cost) * 100.0 / 
        NULLIF((SELECT SUM(p.price * s.quantity) FROM dbo.sales s JOIN dbo.products p ON s.product_id = p.product_id), 0)) 
        AS Warranty_Cost_Ratio
FROM
    WarrantyCosts wc;



CREATE VIEW vw_Repeat_Claim_Rate AS
WITH ClaimCounts AS (
    SELECT
        s.product_id,
        s.sale_id,
        COUNT(w.claim_id) AS Claims_Per_Sale
    FROM
        dbo.sales s
    LEFT JOIN
        dbo.warranty w ON s.sale_id = w.sale_id
    GROUP BY
        s.product_id, s.sale_id
)
SELECT
    COUNT(CASE WHEN Claims_Per_Sale > 1 THEN sale_id END) AS Sales_With_Multiple_Claims,
    COUNT(CASE WHEN Claims_Per_Sale > 0 THEN sale_id END) AS Sales_With_Claims,
    COUNT(sale_id) AS Total_Sales,
    (COUNT(CASE WHEN Claims_Per_Sale > 1 THEN sale_id END) * 100.0 / 
        NULLIF(COUNT(CASE WHEN Claims_Per_Sale > 0 THEN sale_id END), 0)) AS Repeat_Claim_Rate_Among_Claimed,
    (COUNT(CASE WHEN Claims_Per_Sale > 1 THEN sale_id END) * 100.0 / 
        NULLIF(COUNT(sale_id), 0)) AS Repeat_Claim_Rate_Among_All
FROM
    ClaimCounts;




-- Customer Behavior KPIs

CREATE VIEW vw_Transaction_Category_Distribution AS
WITH TransactionCategories AS (
    SELECT 
        s.sale_id,
        COUNT(DISTINCT p.category_id) AS Unique_Categories,
        SUM(p.price * s.quantity) AS Transaction_Value,
        COUNT(DISTINCT p.product_id) AS Unique_Products
    FROM 
        dbo.sales s
    JOIN 
        dbo.products p ON s.product_id = p.product_id
    GROUP BY 
        s.sale_id
)
SELECT 
    Unique_Categories,
    COUNT(*) AS Transaction_Count,
    (COUNT(*) * 100.0 / (SELECT COUNT(DISTINCT sale_id) FROM dbo.sales)) AS Percentage_Of_Transactions,
    AVG(Transaction_Value) AS Average_Transaction_Value,
    AVG(Unique_Products) AS Average_Products_Per_Transaction
FROM 
    TransactionCategories
GROUP BY 
    Unique_Categories;









CREATE VIEW vw_Transaction_Time_Patterns AS
SELECT 
    DATEPART(HOUR, s.sale_date) AS Hour_Of_Day,
    COUNT(DISTINCT s.sale_id) AS Transaction_Count,
    SUM(p.price * s.quantity) AS Total_Revenue,
    SUM(p.price * s.quantity) / COUNT(DISTINCT s.sale_id) AS Average_Transaction_Value
FROM 
    dbo.sales s
JOIN 
    dbo.products p ON s.product_id = p.product_id
GROUP BY 
    DATEPART(HOUR, s.sale_date);



CREATE VIEW vw_Product_Upgrade_Patterns AS
WITH ProductFamilies AS (
    -- Group products that might be in the same family (simplified approach)
    SELECT 
        product_id,
        product_name,
        -- Extract potential product family from name (e.g., "iPhone" from "iPhone 11")
        LEFT(product_name, CHARINDEX(' ', product_name + ' ')) AS Product_Family,
        launch_date
    FROM 
        dbo.products
)
SELECT 
    pf1.Product_Family,
    pf1.product_name AS Initial_Product,
    pf2.product_name AS Upgrade_Product,
    COUNT(*) AS Potential_Upgrade_Count,
    AVG(DATEDIFF(DAY, s1.sale_date, s2.sale_date)) AS Average_Days_Between_Purchases
FROM 
    dbo.sales s1
JOIN 
    ProductFamilies pf1 ON s1.product_id = pf1.product_id
JOIN 
    dbo.sales s2 ON s1.store_id = s2.store_id  -- Assuming same store might indicate same customer
JOIN 
    ProductFamilies pf2 ON s2.product_id = pf2.product_id
WHERE 
    pf1.Product_Family = pf2.Product_Family  -- Same product family
    AND pf1.product_id != pf2.product_id     -- Different products
    AND pf1.launch_date < pf2.launch_date    -- Newer product was launched later
    AND s1.sale_date < s2.sale_date          -- Purchased in chronological order
    AND DATEDIFF(DAY, s1.sale_date, s2.sale_date) BETWEEN 30 AND 730  -- Between 1 month and 2 years
GROUP BY 
    pf1.Product_Family, pf1.product_name, pf2.product_name
HAVING 
    COUNT(*) > 1;  -- Only show patterns that appear multiple times




CREATE VIEW vw_Post_Warranty_Purchase_Behavior AS
WITH StoreSalePatterns AS (
    -- Group sales by store to use as a proxy for customer
    SELECT 
        s.store_id,
        s.sale_id,
        s.sale_date,
        CASE WHEN w.claim_id IS NOT NULL THEN 1 ELSE 0 END AS Had_Warranty_Claim,
        w.repair_status,
        ROW_NUMBER() OVER (PARTITION BY s.store_id ORDER BY s.sale_date) AS Purchase_Sequence
    FROM 
        dbo.sales s
    LEFT JOIN 
        dbo.warranty w ON s.sale_id = w.sale_id
)
SELECT 
    ssp1.Had_Warranty_Claim,
    ssp1.repair_status,
    COUNT(DISTINCT ssp1.store_id) AS Store_Count,
    AVG(DATEDIFF(DAY, ssp1.sale_date, ssp2.sale_date)) AS Average_Days_To_Next_Purchase,
    COUNT(DISTINCT ssp2.sale_id) AS Follow_Up_Purchases,
    COUNT(DISTINCT ssp2.sale_id) * 1.0 / COUNT(DISTINCT ssp1.store_id) AS Purchases_Per_Store
FROM 
    StoreSalePatterns ssp1
JOIN 
    StoreSalePatterns ssp2 ON ssp1.store_id = ssp2.store_id
    AND ssp2.Purchase_Sequence = ssp1.Purchase_Sequence + 1
WHERE 
    ssp2.sale_date > ssp1.sale_date
GROUP BY 
    ssp1.Had_Warranty_Claim, ssp1.repair_status;



CREATE VIEW vw_Seasonal_Purchase_Behavior AS
SELECT 
    MONTH(s.sale_date) AS Month,
    COUNT(DISTINCT s.sale_id) AS Transaction_Count,
    SUM(s.quantity) AS Units_Sold,
    SUM(p.price * s.quantity) AS Total_Revenue,
    SUM(p.price * s.quantity) / COUNT(DISTINCT s.sale_id) AS Average_Transaction_Value,
    AVG(s.quantity) AS Average_Units_Per_Transaction
FROM 
    dbo.sales s
JOIN 
    dbo.products p ON s.product_id = p.product_id
GROUP BY 
    MONTH(s.sale_date);



-- Market Analysis KPIs

-- 1. Market Penetration by Region (simplified without external data)
CREATE VIEW vw_Market_Penetration_By_Region AS
SELECT 
    st.country,
    st.City,
    SUM(s.quantity * p.price) AS RegionalSales,
    COUNT(DISTINCT st.store_id) AS StoreCount,
    SUM(s.quantity * p.price) / NULLIF(COUNT(DISTINCT st.store_id), 0) AS SalesPerStore,
    -- Use store count as a proxy for market size
    RANK() OVER (ORDER BY SUM(s.quantity * p.price) / NULLIF(COUNT(DISTINCT st.store_id), 0) DESC) AS RegionalPerformanceRank
FROM 
    dbo.sales s
JOIN 
    dbo.stores st ON s.store_id = st.store_id
JOIN 
    dbo.products p ON s.product_id = p.product_id
GROUP BY 
    st.country, st.City;

-- 2. Category Market Share (simplified without external data)
CREATE VIEW vw_Category_Market_Share AS
SELECT 
    c.category_name,
    SUM(s.quantity * p.price) AS CategorySales,
    (SELECT SUM(s2.quantity * p2.price) FROM dbo.sales s2 JOIN dbo.products p2 ON s2.product_id = p2.product_id) AS TotalSales,
    (SUM(s.quantity * p.price) / 
     NULLIF((SELECT SUM(s2.quantity * p2.price) FROM dbo.sales s2 JOIN dbo.products p2 ON s2.product_id = p2.product_id), 0)) * 100 AS CategorySharePercentage
FROM 
    dbo.sales s
JOIN 
    dbo.products p ON s.product_id = p.product_id
	join category c 
	on c.category_id=p.category_id
GROUP BY 
    c.category_name;

-- 3. Price Point Analysis
CREATE VIEW vw_Price_Point_Analysis AS
SELECT 
    CASE 
        WHEN p.price < 500 THEN 'Under $500'
        WHEN p.price >= 500 AND p.price < 1000 THEN '$500-$999'
        WHEN p.price >= 1000 AND p.price < 1500 THEN '$1000-$1499'
        WHEN p.price >= 1500 AND p.price < 2000 THEN '$1500-$1999'
        ELSE '$2000+'
    END AS PriceRange,
    COUNT(s.sale_id) AS NumberOfTransactions,
    SUM(s.quantity) AS UnitsSold,
    SUM(s.quantity * p.price) AS TotalRevenue,
    COUNT(DISTINCT p.product_id) AS NumberOfProducts
FROM 
    dbo.sales s
JOIN 
    dbo.products p ON s.product_id = p.product_id
GROUP BY 
    CASE 
        WHEN p.price < 500 THEN 'Under $500'
        WHEN p.price >= 500 AND p.price < 1000 THEN '$500-$999'
        WHEN p.price >= 1000 AND p.price < 1500 THEN '$1000-$1499'
        WHEN p.price >= 1500 AND p.price < 2000 THEN '$1500-$1999'
        ELSE '$2000+'
    END;

-- 4. Competitive Position (simplified - using internal benchmarking)
CREATE VIEW vw_Competitive_Position AS
SELECT 
    c.category_name,
    SUM(s.quantity) AS UnitsSold,
    SUM(s.quantity * p.price) AS CategoryRevenue,
    AVG(p.price) AS AvgPricePoint,
    CASE
        WHEN SUM(s.quantity) > (SELECT AVG(CategoryUnits) FROM 
            (SELECT c2.category_name, SUM(s2.quantity) AS CategoryUnits 
             FROM dbo.sales s2 JOIN dbo.products p2 ON s2.product_id = p2.product_id 
			 	join category c2
	on c2.category_id=p2.category_id

             GROUP BY c2.category_name) AS CategorySummary) * 1.2
        THEN 'High Volume'
        ELSE 'Average/Low Volume'
    END AS VolumePosition,
    CASE
        WHEN AVG(p.price) > (SELECT AVG(AvgCategoryPrice) FROM 
            (SELECT c2.category_name, AVG(p2.price) AS AvgCategoryPrice 
             FROM dbo.products p2 
			 join category c2 
	on c2.category_id=p2.category_id
             GROUP BY c2.category_name) AS PriceSummary)
        THEN 'Premium'
        ELSE 'Value'
    END AS PricePosition
FROM 
    dbo.sales s
JOIN 
    dbo.products p ON s.product_id = p.product_id
	join category c 
	on c.category_id=p.category_id
GROUP BY 
    c.category_name;
-- 5. Geographic Expansion Opportunity
CREATE VIEW vw_Geographic_Expansion_Opportunity AS
WITH RegionPerformance AS (
    SELECT 
        st.City,
        st.country,
        COUNT(DISTINCT st.store_id) AS StoreCount,
        SUM(s.quantity) AS UnitsSold,
        SUM(s.quantity * p.price) AS TotalRevenue,
        CASE
            WHEN COUNT(DISTINCT st.store_id) = 0 THEN 0
            ELSE SUM(s.quantity * p.price) / NULLIF(COUNT(DISTINCT st.store_id), 0)
        END AS RevenuePerStore,
        COUNT(DISTINCT s.sale_id) AS TransactionCount
    FROM 
        dbo.sales s
    JOIN 
        dbo.stores st ON s.store_id = st.store_id
    JOIN 
        dbo.products p ON s.product_id = p.product_id
    GROUP BY 
        st.City, st.country
)
SELECT 
    rp.City,
    rp.country,
    rp.StoreCount,
    rp.UnitsSold,
    rp.TotalRevenue,
    rp.RevenuePerStore,
    rp.TransactionCount,
    (SELECT AVG(RevenuePerStore) FROM RegionPerformance WHERE RevenuePerStore > 0) AS AvgRevenuePerStore,
    CASE
        WHEN rp.RevenuePerStore < (SELECT AVG(RevenuePerStore) * 0.8 FROM RegionPerformance WHERE RevenuePerStore > 0) THEN 'High Priority'
        WHEN rp.RevenuePerStore < (SELECT AVG(RevenuePerStore) FROM RegionPerformance WHERE RevenuePerStore > 0) THEN 'Medium Priority'
        ELSE 'Low Priority'
    END AS ExpansionPriority,
    CASE
        WHEN rp.StoreCount < 3 THEN 'Underserved Market'
        ELSE 'Established Market'
    END AS MarketMaturity
FROM 
    RegionPerformance rp;




--Financial Performance KPIs

-- 1. Gross Profit Margin
-- Note: Assuming COGS is calculated as 60% of price for products
CREATE VIEW vw_Gross_Profit_Margin AS
SELECT
    YEAR(s.sale_date) AS Year,
    MONTH(s.sale_date) AS Month,
    SUM(s.quantity * p.price) AS Revenue,
    SUM(s.quantity * (p.price * 0.6)) AS COGS, -- Assuming COGS is 60% of price
    SUM(s.quantity * (p.price * 0.4)) AS GrossProfit, -- 40% gross margin
    (SUM(s.quantity * (p.price * 0.4)) / NULLIF(SUM(s.quantity * p.price), 0)) * 100 AS GrossProfitMarginPercentage
FROM
    dbo.sales s
JOIN
    dbo.products p ON s.product_id = p.product_id
GROUP BY
    YEAR(s.sale_date), MONTH(s.sale_date);

-- 2. Operating Expense Ratio
-- Note: Approximating operating expenses as store fixed costs + variable sales costs
CREATE VIEW vw_Operating_Expense_Ratio AS
SELECT
    YEAR(s.sale_date) AS Year,
    MONTH(s.sale_date) AS Month,
    SUM(s.quantity * p.price) AS Revenue,
    -- Estimating operating expenses: $10,000 fixed cost per store + 15% of revenue for variable costs
    (COUNT(DISTINCT s.store_id) * 10000) + (SUM(s.quantity * p.price) * 0.15) AS EstimatedOperatingExpenses,
    ((COUNT(DISTINCT s.store_id) * 10000) + (SUM(s.quantity * p.price) * 0.15)) / 
    NULLIF(SUM(s.quantity * p.price), 0) * 100 AS OperatingExpenseRatio
FROM
    dbo.sales s
JOIN
    dbo.products p ON s.product_id = p.product_id
JOIN
    dbo.stores st ON s.store_id = st.store_id
GROUP BY
    YEAR(s.sale_date), MONTH(s.sale_date);

-- 3. Inventory Turnover Rate
-- Assuming inventory table with average inventory values
-- For this example, estimating inventory from sales patterns
CREATE VIEW vw_Inventory_Turnover AS
WITH MonthlyCOGS AS (
    SELECT
        YEAR(s.sale_date) AS Year,
        MONTH(s.sale_date) AS Month,
        SUM(s.quantity * (p.price * 0.6)) AS COGS -- Estimating COGS as 60% of price
    FROM
        dbo.sales s
    JOIN
        dbo.products p ON s.product_id = p.product_id
    GROUP BY
        YEAR(s.sale_date), MONTH(s.sale_date)
),
EstimatedInventory AS (
    SELECT
        Year,
        Month,
        -- Estimating average monthly inventory as 2x the monthly COGS
        COGS * 2 AS EstimatedAverageInventory,
        COGS
    FROM
        MonthlyCOGS
)
SELECT
    Year,
    Month,
    COGS,
    EstimatedAverageInventory,
    COGS / NULLIF(EstimatedAverageInventory, 0) AS InventoryTurnoverRate,
    CASE
        WHEN COGS / NULLIF(EstimatedAverageInventory, 0) > 0 
        THEN 365 / (COGS / NULLIF(EstimatedAverageInventory, 0))
        ELSE NULL
    END AS DaysInventoryOutstanding
FROM
    EstimatedInventory;

-- 4. Return on Investment (ROI)
-- Assuming investment is store setup cost + inventory
CREATE VIEW vw_Return_On_Investment AS
WITH StoreInvestment AS (
    SELECT
        st.store_id,
        -- Assuming $500,000 initial investment per store
        500000 AS InitialInvestment
    FROM
        dbo.stores st
),
StorePerformance AS (
    SELECT
        s.store_id,
        YEAR(s.sale_date) AS Year,
        SUM(s.quantity * p.price) AS Revenue,
        SUM(s.quantity * (p.price * 0.6)) AS COGS,
        SUM(s.quantity * (p.price * 0.4)) AS GrossProfit,
        -- Estimating operating expenses as 15% of revenue
        SUM(s.quantity * p.price) * 0.15 AS OperatingExpenses,
        SUM(s.quantity * (p.price * 0.4)) - (SUM(s.quantity * p.price) * 0.15) AS NetProfit
    FROM
        dbo.sales s
    JOIN
        dbo.products p ON s.product_id = p.product_id
    GROUP BY
        s.store_id, YEAR(s.sale_date)
)
SELECT
    sp.store_id,
    sp.Year,
    si.InitialInvestment,
    sp.Revenue,
    sp.NetProfit,
    (sp.NetProfit / NULLIF(si.InitialInvestment, 0)) * 100 AS ROI
FROM
    StorePerformance sp
JOIN
    StoreInvestment si ON sp.store_id = si.store_id;

-- 5. Cash Conversion Cycle
-- Note: Approximating DIO, DSO, and DPO based on available data
CREATE VIEW vw_Cash_Conversion_Cycle AS
WITH MonthlySales AS (
    SELECT
        YEAR(s.sale_date) AS Year,
        MONTH(s.sale_date) AS Month,
        SUM(s.quantity * p.price) AS Revenue,
        SUM(s.quantity * (p.price * 0.6)) AS COGS
    FROM
        dbo.sales s
    JOIN
        dbo.products p ON s.product_id = p.product_id
    GROUP BY
        YEAR(s.sale_date), MONTH(s.sale_date)
)
SELECT
    Year,
    Month,
    Revenue,
    COGS,
    -- Estimating Days Inventory Outstanding (DIO)
    30 AS EstimatedDIO, -- Assuming monthly inventory turnover
    -- Estimating Days Sales Outstanding (DSO)
    15 AS EstimatedDSO, -- Assuming customers pay in 15 days on average
    -- Estimating Days Payable Outstanding (DPO)
    45 AS EstimatedDPO, -- Assuming suppliers are paid in 45 days on average
    -- Cash Conversion Cycle = DIO + DSO - DPO
    30 + 15 - 45 AS CashConversionCycle
FROM
    MonthlySales;

-- 6. Break-Even Analysis
CREATE VIEW vw_Break_Even_Analysis AS
WITH StoreCosts AS (
    SELECT
        s.store_id,
        -- Fixed costs: $10,000 per month per store
        10000 * 12 AS AnnualFixedCosts,
        -- Variable costs: 75% of revenue (COGS + operating expenses)
        SUM(s.quantity * p.price) * 0.75 AS AnnualVariableCosts,
        SUM(s.quantity * p.price) AS AnnualRevenue,
        SUM(s.quantity) AS AnnualUnits,
        AVG(p.price) AS AvgUnitPrice,
        (SUM(s.quantity * p.price) * 0.75) / NULLIF(SUM(s.quantity), 0) AS AvgVariableCostPerUnit
    FROM
        dbo.sales s
    JOIN
        dbo.products p ON s.product_id = p.product_id
    WHERE
        s.sale_date >= DATEADD(YEAR, -1, GETDATE())
    GROUP BY
        s.store_id
)
SELECT
    store_id,
    AnnualFixedCosts,
    AnnualVariableCosts,
    AnnualRevenue,
    AnnualUnits,
    AvgUnitPrice,
    AvgVariableCostPerUnit,
    -- Break-even in units = Fixed Costs / (Price - Variable Cost per Unit)
    AnnualFixedCosts / NULLIF((AvgUnitPrice - AvgVariableCostPerUnit), 0) AS BreakEvenUnits,
    -- Break-even in revenue = Break-even Units * Price
    (AnnualFixedCosts / NULLIF((AvgUnitPrice - AvgVariableCostPerUnit), 0)) * AvgUnitPrice AS BreakEvenRevenue
FROM
    StoreCosts;

-- 7. Economic Value Added (EVA)
-- Note: Approximating capital and WACC
CREATE VIEW vw_Economic_Value_Added AS
WITH CompanyFinancials AS (
    SELECT
        YEAR(s.sale_date) AS Year,
        -- Total Revenue
        SUM(s.quantity * p.price) AS Revenue,
        -- COGS
        SUM(s.quantity * (p.price * 0.6)) AS COGS,
        -- Gross Profit
        SUM(s.quantity * (p.price * 0.4)) AS GrossProfit,
        -- Operating Expenses (15% of revenue)
        SUM(s.quantity * p.price) * 0.15 AS OperatingExpenses,
        -- EBIT (Earnings Before Interest and Taxes)
        SUM(s.quantity * (p.price * 0.4)) - (SUM(s.quantity * p.price) * 0.15) AS EBIT,
        -- Taxes (25% of EBIT)
        (SUM(s.quantity * (p.price * 0.4)) - (SUM(s.quantity * p.price) * 0.15)) * 0.25 AS Taxes,
        -- NOPAT (Net Operating Profit After Taxes)
        (SUM(s.quantity * (p.price * 0.4)) - (SUM(s.quantity * p.price) * 0.15)) * 0.75 AS NOPAT,
        -- Estimating total capital employed as 2x annual revenue
        SUM(s.quantity * p.price) * 2 AS EstimatedCapital,
        -- WACC (Weighted Average Cost of Capital) - typically 8-12%
        10.0 AS AssumedWACC
    FROM
        dbo.sales s
    JOIN
        dbo.products p ON s.product_id = p.product_id
    GROUP BY
        YEAR(s.sale_date)
)
SELECT
    Year,
    Revenue,
    COGS,
    GrossProfit,
    OperatingExpenses,
    EBIT,
    Taxes,
    NOPAT,
    EstimatedCapital,
    AssumedWACC,
    -- EVA = NOPAT - (Capital * WACC)
    NOPAT - (EstimatedCapital * (AssumedWACC / 100)) AS EconomicValueAdded
FROM
    CompanyFinancials;


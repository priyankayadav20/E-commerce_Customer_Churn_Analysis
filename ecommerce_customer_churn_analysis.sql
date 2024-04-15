-------------------------- 1. Creating Database and Table and Importing Data into Table -----------------------------
DROP DATABASE IF EXISTS churn_db;
CREATE DATABASE churn_db;
USE churn_db;
DROP TABLE IF EXISTS churn_db.ecommerce_churn;
CREATE TABLE ecommerce_churn(
    CustomerID INT PRIMARY KEY,
    Churn INT ,
    Tenure INT,
    PreferredLoginDevice VARCHAR(40), 
    CityTier INT,
    WarehouseToHome INT,
    PreferredPaymentMode VARCHAR(40), 
    Gender VARCHAR(40), 
    HourSpendOnApp INT,
    NumberOfDeviceRegistered INT,
    PreferredOrderCat VARCHAR(40),
    SatisfactionScore INT,
    MaritalStatus VARCHAR(40), 
    NumberOfAddress INT,
    Complain INT,
    OrderAmountHikeFromlastYear INT,
    CouponUsed INT,
    OrderCount INT,
    DaysSinceLastOrder INT, 
    CashbackAmount INT);

LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\ecommerce_churn.csv'
INTO TABLE ecommerce_churn
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(CustomerID, Churn, @Tenure, PreferredLoginDevice, CityTier, @WarehouseToHome, PreferredPaymentMode, Gender, @HourSpendOnApp, 
NumberOfDeviceRegistered, PreferredOrderCat, SatisfactionScore, MaritalStatus, NumberOfAddress, Complain, 
@OrderAmountHikeFromlastYear, @CouponUsed, @OrderCount, @DaysSinceLastOrder, CashbackAmount)
SET Tenure = NULLIF(@Tenure, ''),
	WarehouseToHome= NULLIF(@WarehouseToHome, ''),
	HourSpendOnApp = NULLIF(@HourSpendOnApp, ''),
    OrderAmountHikeFromlastYear = NULLIF(@OrderAmountHikeFromlastYear, ''),
    CouponUsed = NULLIF(@CouponUsed, ''),
    OrderCount = NULLIF(@OrderCount, ''),
    DaysSinceLastOrder = NULLIF(@DaysSinceLastOrder, '');

 /**************************************************************************************************************************
                                                   2. DATA CLEANING 
 ****************************************************************************************************************************/                                                  
-------------------------------------- 2.1 Finding the total number of customers -------------------------------------------
SELECT DISTINCT COUNT(CustomerID) AS Total_Number_of_Customers FROM ecommerce_churn;

-------------------------------------- 2.2 Checking for duplicate rows -----------------------------------------------------
SELECT CustomerID, COUNT(CustomerID) AS Count FROM ecommerce_churn
GROUP BY CustomerID
HAVING COUNT(CustomerID) >1;   

-------------------------------------- 2.3 Checking for Null Values --------------------------------------------------------
SELECT 'Tenure' AS COLUMN_NAME, SUM(CASE WHEN  Tenure  IS NULL THEN 1 ELSE 0 END) AS NULL_COUNT FROM ecommerce_churn
UNION
SELECT 'WarehouseToHome' AS COLUMN_NAME, SUM(CASE WHEN WarehouseToHome IS NULL THEN 1 ELSE 0 END) AS NULL_COUNT FROM ecommerce_churn
UNION
SELECT 'HourSpendOnApp' AS COLUMN_NAME, SUM(CASE WHEN HourSpendOnApp IS NULL THEN 1 ELSE 0 END) AS NULL_COUNT FROM ecommerce_churn
UNION
SELECT 'OrderAmountHikeFromlastYear' AS COLUMN_NAME, SUM(CASE WHEN OrderAmountHikeFromlastYear IS NULL THEN 1 ELSE 0 END) AS NULL_COUNT FROM ecommerce_churn
UNION
SELECT 'CouponUsed' AS COLUMN_NAME, SUM(CASE WHEN CouponUsed IS NULL THEN 1 ELSE 0 END) AS NULL_COUNT FROM ecommerce_churn
UNION
SELECT 'OrderCount' AS COLUMN_NAME, SUM(CASE WHEN OrderCount IS NULL THEN 1 ELSE 0 END) AS NULL_COUNT FROM ecommerce_churn
UNION
SELECT 'DaysSinceLastOrder' AS COLUMN_NAME, SUM(CASE WHEN DaysSinceLastOrder IS NULL THEN 1 ELSE 0 END) AS NULL_COUNT FROM ecommerce_churn;
             
-------------------------------------- 2.4 Handling Null Values ------------------------------------------------------------
SET SQL_SAFE_UPDATES = 0;

UPDATE ecommerce_churn AS e1
JOIN (SELECT AVG(Tenure) AS Avg_Tenure FROM ecommerce_churn) AS e2
SET e1.Tenure = e2.Avg_Tenure
WHERE e1.Tenure IS NULL;

UPDATE ecommerce_churn AS e1
JOIN (SELECT AVG(WarehouseToHome) AS Avg_WarehouseToHome FROM ecommerce_churn) AS e2
SET e1.WarehouseToHome = e2.Avg_WarehouseToHome 
WHERE e1.WarehouseToHome  IS NULL;

UPDATE ecommerce_churn AS e1
JOIN (SELECT AVG(HourSpendOnApp) AS Avg_HourSpendOnApp FROM ecommerce_churn) AS e2
SET e1.HourSpendOnApp = e2.Avg_HourSpendOnApp
WHERE e1.HourSpendOnApp IS NULL;

UPDATE ecommerce_churn AS e1 
JOIN (SELECT AVG(OrderAmountHikeFromlastYear) AS Avg_OrderAmountHikeFromlastYear FROM ecommerce_churn) AS e2
SET e1.OrderAmountHikeFromlastYear = e2.Avg_OrderAmountHikeFromlastYear 
WHERE e1.OrderAmountHikeFromlastYear IS NULL;

UPDATE ecommerce_churn AS e1
JOIN (SELECT AVG(CouponUsed) AS Avg_CouponUsed FROM ecommerce_churn ) AS e2
SET e1.CouponUsed = e2.Avg_CouponUsed 
WHERE e1.CouponUsed IS NULL;

UPDATE ecommerce_churn AS e1
JOIN (SELECT AVG(OrderCount) AS Avg_OrderCount FROM ecommerce_churn) AS e2
SET e1.OrderCount = e2.Avg_OrderCount
WHERE e1.OrderCount IS NULL;

UPDATE ecommerce_churn AS e1
JOIN (SELECT AVG(DaysSinceLastOrder ) AS Avg_DaysSinceLastOrder FROM ecommerce_churn) AS e2
SET e1.DaysSinceLastOrder = e2.Avg_DaysSinceLastOrder 
WHERE e1.DaysSinceLastOrder IS NULL;

SET SQL_SAFE_UPDATES = 1;

-------------------------------- 2.5 Create a new column, "CustomerStatus," based on the "Churn" column  ----------------------------- 
-------------------------------- "Churn" contains binary values (0 for staying, 1 for churning)  -----------------------------------
---------- The new column will replace 0 with "Stayed" and 1 with "Churned" for clearer representation of customer status-----------
ALTER TABLE ecommerce_churn ADD CustomerStatus VARCHAR(50);
SET SQL_SAFE_UPDATES = 0;
UPDATE ecommerce_churn
SET CustomerStatus = 
CASE 
    WHEN Churn = 1 THEN 'Churned' 
    WHEN Churn = 0 THEN 'Stayed'
END ;
SET SQL_SAFE_UPDATES = 1;
SELECT DISTINCT CustomerStatus FROM ecommerce_churn;

----------------------------------- 2.6 Add a new column named 'complainreceived' based on the 'complain' column --------------------
---------------------------- In 'complain,' 0 means 'No' and 1 means 'Yes' --------------------------------------------------------
---------------- The new column will display 'Yes' for 1 and 'No' for 0, providing clearer representation -------------------------
ALTER TABLE ecommerce_churn ADD ComplainRecieved VARCHAR(10);
SET SQL_SAFE_UPDATES = 0;
UPDATE ecommerce_churn
SET ComplainRecieved =  
CASE 
    WHEN Complain = 1 THEN 'Yes'
    WHEN Complain = 0 THEN 'No'
END;
SET SQL_SAFE_UPDATES = 1;
SELECT DISTINCT ComplainRecieved FROM ecommerce_churn;

----------------------------- 2.7 Verify the accuracy and correctness of values in each column ----------------------------------
----------------------------------- 2.7.1 a) Fixing redundancy in 'PreferedLoginDevice' Column ------------------------------------
SELECT DISTINCT PreferredLoginDevice FROM ecommerce_churn;

-------------------- 2.7.1 b) Replace mobile phone with phone because 'Phone' and 'Mobile Phone' are same thing ------------------
SET SQL_SAFE_UPDATES = 0;
UPDATE ecommerce_churn
SET PreferredLoginDevice = 'Phone'
WHERE PreferredLoginDevice = 'Mobile Phone';
SET SQL_SAFE_UPDATES = 1;

----------------------------- 2.7.2 a) Fixing redundancy in 'PreferredOrderCat' Column ------------------------------------------
SELECT DISTINCT PreferredOrderCat FROM ecommerce_churn;

----------------------------- 2.7.2 b) Replace 'Mobile' with 'Mobile Phone'-----------------------------------------------------
SET SQL_SAFE_UPDATES = 0;
UPDATE ecommerce_churn
SET PreferredOrderCat = 'Mobile Phone'
WHERE PreferredOrderCat = 'Mobile';
SET SQL_SAFE_UPDATES = 1;

----------------------------- 2.7.3 a) Check distinct values for 'PreferredPaymentMode' column ----------------------------------
SELECT DISTINCT PreferredPaymentMode FROM ecommerce_churn;

----------------------------- 2.7.3 b) Replace 'COD' with 'Cash on Delivery' ---------------------------------------------------
SET SQL_SAFE_UPDATES = 0;
UPDATE ecommerce_churn
SET PreferredPaymentMode  = 'Cash on Delivery'
WHERE PreferredPaymentMode  = 'COD';
SET SQL_SAFE_UPDATES = 1;

----------------------------- 2.7.4 a) Check distinct value in 'WarehouseToHome' column -----------------------------------------
SELECT DISTINCT WarehouseToHome FROM ecommerce_churn;

-- I can see two values 126 and 127 that are outliers, it could be a data entry error, so I will correct it to 26 & 27 respectively--
----------------------------- 2.7.4 b) Replace value 127 with 27 and 126 with 26 -------------------------------------------------
SET SQL_SAFE_UPDATES = 0;
UPDATE ecommerce_churn
SET WarehouseToHome = 27
WHERE WarehouseToHome = 127;
UPDATE ecommerce_churn
SET WarehouseToHome = 26
WHERE WarehouseToHome = '126';
SET SQL_SAFE_UPDATES = 1;

/******************************************************************************************************************************
                                3. DATA EXPLORATION AND ANSWERING BUSINESS QUESTIONS
*******************************************************************************************************************************/
-------------------------------- 3.1. What is the overall customer churn rate? ------------------------------------------------
SELECT TotalNumberOfCustomers, TotalNumberOfChurnedCustomers,
       CAST((TotalNumberOfChurnedCustomers / TotalNumberOfCustomers)*100 AS DECIMAL(10,2)) AS ChurnRate
FROM (SELECT COUNT(*) AS TotalNumberOfCustomers FROM ecommerce_churn) AS Total,
(SELECT COUNT(*) AS TotalNumberOfChurnedCustomers FROM ecommerce_churn WHERE CustomerStatus = 'Churned') AS Churned;
-------------------------- Answer = The Churn rate is '16.84%' ----------------------------------------------------------------

----------------------------- 3.2. How does the churn rate vary based on the prefered login device? --------------------------
SELECT PreferredLoginDevice, COUNT(*) AS TotalCustomers, SUM(Churn) AS ChurnedCustomers, 
CAST(SUM(Churn)/COUNT(*)*100 AS DECIMAL(10, 2)) AS ChurnRate
FROM ecommerce_churn
GROUP BY PreferredLoginDevice;
-------------------- 'Computer' accounts for the highest churnrate with 19.83% and then 'Phone' with 15.62%. ------------------

----------------------------- 3.3 What is the distribution of customers across different city tiers? --------------------------
SELECT CityTier, COUNT(*) AS TotalCustomers, SUM(Churn) AS ChurnedCustomers,
CAST(SUM(Churn) / COUNT(*)*100 AS DECIMAL(10, 2)) AS ChurnRate
FROM ecommerce_churn
GROUP BY CityTier
ORDER BY Churnrate DESC;
----------- Answer = citytier3 has the highest churn rate with '21.37%', followed by citytier2 '19.83%' and then ---------------
----------------------------------- citytier1 has the least churn rate with '14.51%' -------------------------------------------

----------------------- 3.4. Is there any correlation between the warehouse-to-home distance and customer churn? ---------------
-- 3.4.a) Firstly, we will create a new column that provides a distance range based on the values in warehousetohome column ---
ALTER TABLE ecommerce_churn ADD WarehouseToHomeRange VARCHAR(50);
SET SQL_SAFE_UPDATES =0;
UPDATE ecommerce_churn
SET WarehouseToHomeRange =
CASE 
    WHEN WarehouseToHome <= 10 THEN 'Very Close Distance'
    WHEN WarehouseToHome > 10 AND WarehouseToHome <= 20 THEN 'Close Distance'
    WHEN WarehouseToHome > 20 AND WarehouseToHome <= 30 THEN 'Moderate Distance'
    WHEN WarehouseToHome > 30 THEN 'Far Distance'
END;
SET SQL_SAFE_UPDATES =1;
SELECT DISTINCT WarehouseToHomeRange FROM ecommerce_churn;

----------------------------------- 3.4.b) Finding correlation between warehousetohome and churnrate --------------------------
SELECT WarehouseToHomeRange, COUNT(*) AS TotalCustomers, SUM(Churn) AS ChurnedCustomers, 
CAST(SUM(Churn) / COUNT(*)*100 AS DECIMAL(10, 2)) AS ChurnRate
FROM ecommerce_churn
GROUP BY WarehouseToHomeRange
ORDER BY ChurnRate DESC;
-- Answer = The churn rate increases as the warehouse to home distance increases.

-------------------------------- 3.5. Which is the most prefered payment mode among churned customers? ------------------------
SELECT PreferredPaymentMode, COUNT(*) AS TotalCustomers, SUM(Churn) AS ChurnedCustomers, 
CAST(SUM(Churn) / COUNT(*)*100 AS DECIMAL(10, 2)) AS ChurnRate
FROM ecommerce_churn
GROUP BY PreferredPaymentMode
ORDER BY ChurnRate DESC;
-------------------------- Answer = The most prefered payment mode among churned customers is Cash on Delivery -----------------

----------------------------------- 3.6. What is the typical tenure for churned customers? -----------------------------------
-------- 3.6.a) Firstly, we will create a new column that provides a tenure range based on the values in tenure column---------
ALTER TABLE ecommerce_churn ADD TenureRange VARCHAR(50);
SET SQL_SAFE_UPDATES =0;
UPDATE ecommerce_churn
SET TenureRange =
CASE 
WHEN Tenure <= 6 THEN '6 Months'
WHEN Tenure > 6 AND Tenure <= 12 THEN '1 Year'
WHEN Tenure > 12 AND Tenure<= 24 THEN '2 Years'
WHEN Tenure > 24 THEN 'More Than 2 Years'
END;
SET SQL_SAFE_UPDATES =1;
SELECT DISTINCT TenureRange FROM ecommerce_churn;

----------------------------------- 3.6.b) Finding typical tenure for churned customers -------------------------------------
SELECT TenureRange, COUNT(*) AS TotalCustomers, SUM(Churn) AS ChurnedCustomers,
CAST(SUM(Churn) /COUNT(*) * 100 AS DECIMAL(10,2)) AS ChurnRate
FROM ecommerce_churn
GROUP BY TenureRange
ORDER BY ChurnRate DESC;
----------------------------- Answer = Most customers churned within a 6 months tenure period -----------------------------

-------------------- 3.7 Is there any difference in churn rate between male and female customers? -------------------------
SELECT Gender, COUNT(*) AS TotalCustomers, SUM(Churn) AS ChurnedCustomers,
CAST(SUM(Churn) * 1.0 /COUNT(*) * 100 AS DECIMAL(10,2)) AS ChurnRate
FROM ecommerce_churn
GROUP BY Gender
ORDER BY ChurnRate DESC;
----------------------- Answer = More men churned in comaprison to wowen but difference is minimal ------------------------

----------------- 3.8 How does the average time spent on the app differ for churned and non-churned customers? -------------
SELECT CustomerStatus, avg(HourSpendOnApp) AS AverageHourSpentOnApp
FROM ecommerce_churn
GROUP BY CustomerStatus;
----- Answer = There is no difference between the average time spent on the app for churned and non-churned customers -------

-------------------------- 3.9 Does the number of registered devices impact the likelihood of churn? ------------------------
SELECT NumberOfDeviceRegistered, COUNT(*) AS TotalCustomers, SUM(Churn) AS ChurnedCustomers,
CAST(SUM(Churn) / COUNT(*) * 100 AS DECIMAL(10,2)) AS ChurnRate
FROM ecommerce_churn
GROUP BY NumberOfDeviceRegistered
ORDER BY ChurnRate DESC;
-------------------- Answer = As the number of registered devices increseas the churn rate increases. -----------------------

----------------------------- 3.10. Which order category is most preferred among churned customers? --------------------------
SELECT PreferredOrderCat,  COUNT(*) AS TotalCustomers, SUM(Churn) AS ChurnedCustomers,
CAST(SUM(Churn)/COUNT(*) * 100 AS DECIMAL(10,2)) AS ChurnRate
FROM ecommerce_churn
GROUP BY PreferredOrderCat
ORDER BY ChurnRate DESC;
----------- Answer = 'Mobile Phone' category has the highest churn rate and 'Grocery' has the least churn rate ---------------

----------------------- 3.11. Is there any relationship between customer satisfaction scores and churn? ----------------------
SELECT SatisfactionScore, COUNT(*) AS TotalCustomers, SUM(Churn) AS ChurnedCustomers,
CAST(SUM(Churn)/COUNT(*) * 100 AS DECIMAL(10,2)) AS ChurnRate
FROM ecommerce_churn
GROUP BY SatisfactionScore
ORDER BY ChurnRate DESC;
--  Answer = Customer satisfaction score of 5 has the highest churn rate, satisfaction score of 1 has the least churn rate --

----------------------- 3.12. Does the marital status of customers influence churn behavior? ---------------------------------
SELECT MaritalStatus, COUNT(*) AS TotalCustomers, SUM(Churn) AS ChurnedCustomer,
CAST(SUM(Churn) /COUNT(*) * 100 AS DECIMAL(10,2)) AS ChurnRate
FROM ecommerce_churn
GROUP BY MaritalStatus
ORDER BY ChurnRate DESC;
-------- Answer = Single customers have the highest churn rate while married customers have the least churn rate ----------------

----------------------------- 3.13. How many addresses do churned customers have on average? ------------------------------------
SELECT ROUND(AVG(NumberOfAddress), 0) AS Avg_Num_of_Churned_Customers_Address
FROM ecommerce_churn
WHERE CustomerStatus = 'Churned';
----------------------------- Answer = On average, churned customers have 4 addresses -------------------------------------------

-------------------------------- 3.14. Does customer complaints influence churned behavior? -------------------------------------
SELECT ComplainRecieved, COUNT(*) AS TotalCustomer, SUM(Churn) AS ChurnedCustomer,
CAST(SUM(Churn)/COUNT(*) * 100 AS DECIMAL(10,2)) AS ChurnRate
FROM ecommerce_churn
GROUP BY ComplainRecieved
ORDER BY ChurnRate DESC;
----------------------- Answer = Customers with complains had the highest churn rate 31.67% -------------------------------------

-------------------- 3.15. How does the usage of coupons differ between churned and non-churned customers? ---------------------
SELECT CustomerStatus, SUM(CouponUsed) AS SumofCouponUsed
FROM ecommerce_churn
GROUP BY CustomerStatus;
-------------------------- Churned customers used less coupons in comparison to non-churned customers ---------------------------

----------------------- 3.16. What is the average number of days since the last order for churned customers? ---------------------
SELECT ROUND(AVG(DaysSinceLastOrder)) AS AverageNumofDaysSinceLastOrder
FROM ecommerce_churn
WHERE CustomerStatus = 'Churned';
----------------------- Answer = The average number of days since last order for churned customer is 3 ---------------------------

-------------------------- 3.17. Is there any correlation between cashback amount and churn rate? --------------------------------
-- 3.17.a) Firstly, we will create a new column that provides a cashback amount range based on the values in cashback amount column --
ALTER TABLE ecommerce_churn ADD CashbackAmountRange VARCHAR(50);
SET SQL_SAFE_UPDATES = 0;
UPDATE ecommerce_churn
SET CashbackAmountRange =
CASE 
    WHEN CashbackAmount <= 100 THEN 'Low Cashback Amount'
    WHEN CashbackAmount > 100 AND CashbackAmount <= 200 THEN 'Moderate Cashback Amount'
    WHEN CashbackAmount > 200 AND CashbackAmount <= 300 THEN 'High Cashback Amount'
    WHEN CashbackAmount > 300 THEN 'Very High Cashback Amount'
END;
SET SQL_SAFE_UPDATES = 1;
SELECT DISTINCT CashbackAmountRange FROM ecommerce_churn;
----------------------- 3.17.b) Finding correlation between cashbackamountrange and churned rate ------------------------------
SELECT CashbackAmountRange, COUNT(*) AS TotalCustomers, SUM(Churn) AS ChurnedCustomer,
CAST(SUM(Churn) * 1.0 /COUNT(*) * 100 AS DECIMAL(10,2)) AS ChurnRate
FROM ecommerce_churn
GROUP BY CashbackAmountRange
ORDER BY ChurnRate DESC;
-------- Answer = Customers with a Moderate Cashback Amount (Between 100 and 200) have the highest churn rate, follwed by-------
----------------- High cashback amount, then very high cashback amount and finally low cashback amount -------------------------

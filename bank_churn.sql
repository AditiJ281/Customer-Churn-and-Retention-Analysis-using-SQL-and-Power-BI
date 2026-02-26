select *
from churn_bank;

alter table churn_bank
modify surname varchar(100);

-- Create Staging Table for Customer Info
CREATE TABLE stg_customer_info (
    CustomerId INT PRIMARY KEY,
    Surname VARCHAR(100),
    CreditScore INT,
    Geography VARCHAR(50),
    Gender VARCHAR(20),
    Age FLOAT,
    Tenure INT,
    EstimatedSalary VARCHAR(50) -- Kept as string to handle '€' symbols
);

-- Create Staging Table for Account Info
CREATE TABLE stg_account_info (
    CustomerId INT,
    Balance VARCHAR(50), -- Kept as string to handle '€' symbols
    NumOfProducts INT,
    HasCrCard VARCHAR(10), -- 'Yes'/'No'
    Tenure INT,
    IsActiveMember VARCHAR(10), -- 'Yes'/'No'
    Exited INT
);

INSERT INTO stg_customer_info (CustomerId, Surname, CreditScore, Geography, Gender, Age, Tenure, EstimatedSalary)
SELECT CustomerId, Surname, CreditScore, Geography, Gender, Age, Tenure, EstimatedSalary
FROM churn_bank;

INSERT INTO stg_account_info (CustomerId, Balance, NumOfProducts, HasCrCard, Tenure, IsActiveMember, Exited)
SELECT CustomerId, Balance, NumOfProducts, HasCrCard, Tenure, IsActiveMember, Exited
FROM churn_bank;

-- ETL STEP
CREATE TABLE fact_customer_retention AS
WITH clean_customers AS (
    SELECT 
        CustomerId,
        Surname,
        CreditScore,
        CASE 
            WHEN Geography IN ('FRA', 'French') THEN 'France'
            ELSE Geography 
        END AS Country,
        Gender,
        COALESCE(Age, 38) AS Age,
        -- MySQL uses DECIMAL instead of NUMERIC
        CAST(REPLACE(REPLACE(EstimatedSalary, '€', ''), ',', '') AS DECIMAL(15,2)) AS EstimatedSalary
    FROM stg_customer_info
),
clean_accounts AS (
    -- MySQL doesn't have "DISTINCT ON", so we use ROW_NUMBER()
    SELECT * FROM (
        SELECT 
            CustomerId,
            CAST(REPLACE(REPLACE(Balance, '€', ''), ',', '') AS DECIMAL(15,2)) AS Balance,
            NumOfProducts,
            CASE WHEN HasCrCard = 'Yes' THEN 1 ELSE 0 END AS HasCrCard,
            CASE WHEN IsActiveMember = 'Yes' THEN 1 ELSE 0 END AS IsActiveMember,
            Exited,
            ROW_NUMBER() OVER (PARTITION BY CustomerId ORDER BY CustomerId) as rn
        FROM stg_account_info
    ) t
    WHERE rn = 1
)
SELECT 
    c.CustomerId, c.Surname, c.CreditScore, c.Country, c.Gender, c.Age, c.EstimatedSalary,
    a.Balance, a.NumOfProducts, a.HasCrCard, a.IsActiveMember, a.Exited
FROM clean_customers c
JOIN clean_accounts a ON c.CustomerId = a.CustomerId;

-- View for the "Revenue at Risk" Dashboard
CREATE VIEW view_revenue_risk AS
SELECT 
    Country,
    CASE 
        WHEN Age < 30 THEN 'Young'
        WHEN Age BETWEEN 30 AND 50 THEN 'Middle-Aged'
        ELSE 'Senior'
    END AS Age_Group,
    IsActiveMember,
    SUM(Balance) AS Total_Balance,
    AVG(Exited) AS Churn_Rate
FROM fact_customer_retention
GROUP BY 1, 2, 3;

WITH duplicate_cte as(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY CustomerId, Surname, CreditScore, Geography, Gender, Age, Tenure, Balance, NumOfProducts, HasCrCard, IsActiveMember, EstimatedSalary, Exited
order by CustomerId) as rn
FROM churn_bank
)
select*
from duplicate_cte
where rn >1;

ALTER TABLE churn_bank
ADD PRIMARY KEY(CustomerId);

select distinct *
from churn_bank;


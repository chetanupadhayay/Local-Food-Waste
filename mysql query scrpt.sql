CREATE DATABASE food_waste;
USE food_waste;

CREATE TABLE providers (
    Provider_ID INT PRIMARY KEY,
    Name VARCHAR(255) NOT NULL,
    Type ENUM('Restaurant','Grocery Store','Supermarket','Catering Service'), 
    Address TEXT,
    City VARCHAR(100),
    Contact VARCHAR(100)
);

CREATE TABLE receivers (
    Receiver_ID INT PRIMARY KEY,
    Name VARCHAR(255) NOT NULL,
    Type ENUM('NGO','Charity','Individual','Shelter'), 
    City VARCHAR(100),
    Contact VARCHAR(100)
);

CREATE TABLE food_listings (
    Food_ID INT PRIMARY KEY,
    Food_Name VARCHAR(255) NOT NULL,
    Quantity INT CHECK(Quantity >= 0),
    Expiry_Date DATE NOT NULL,
    Provider_ID INT NOT NULL,
    Provider_Type ENUM('Restaurant','Grocery Store','Supermarket','Catering Service'), 
    Location VARCHAR(100),
    Food_Type ENUM('Vegetarian','Non-Vegetarian','Vegan'),
    Meal_Type ENUM('Breakfast','Lunch','Dinner','Snacks'),
    FOREIGN KEY (Provider_ID) REFERENCES providers(Provider_ID) ON DELETE CASCADE
);

CREATE TABLE claims (
    Claim_ID INT PRIMARY KEY,
    Food_ID INT NOT NULL,
    Receiver_ID INT NOT NULL,
    Status ENUM('Pending','Completed','Cancelled'),
    Timestamp DATETIME,
    FOREIGN KEY (Food_ID) REFERENCES food_listings(Food_ID) ON DELETE CASCADE,
    FOREIGN KEY (Receiver_ID) REFERENCES receivers(Receiver_ID) ON DELETE CASCADE
);

-- LOAD DATA LOCAL INFILE 'C:/Users/hp/Downloads/providers_data.csv'
-- INTO TABLE providers
-- FIELDS TERMINATED BY ',' ENCLOSED BY '"'
-- LINES TERMINATED BY '\n'
-- IGNORE 1 ROWS
-- (Provider_ID, Name, Type, Address, City, Contact);

-- LOAD DATA LOCAL INFILE 'C:/Users/hp/Downloads/receivers_data.csv'
-- INTO TABLE receivers
-- FIELDS TERMINATED BY ',' ENCLOSED BY '"'
-- LINES TERMINATED BY '\n'
-- IGNORE 1 ROWS
-- (Receiver_ID, Name, Type, City, Contact);

-- LOAD DATA LOCAL INFILE 'C:/Users/hp/Downloads/food_listings_data.csv'
-- INTO TABLE food_listings
-- FIELDS TERMINATED BY ','
-- ENCLOSED BY '"'
-- LINES TERMINATED BY '\n'
-- IGNORE 1 ROWS
-- (Food_ID, Food_Name, Quantity, @Expiry_Date, Provider_ID, Provider_Type, Location, Food_Type, @Meal_Type)
-- SET
--     Expiry_Date = STR_TO_DATE(@Expiry_Date, '%c/%e/%Y'),
--     Meal_Type   = TRIM(REPLACE(REPLACE(@Meal_Type, '\r', ''), '\n', ''));

-- LOAD DATA LOCAL INFILE 'C:/Users/hp/Downloads/claims_data.csv'
-- INTO TABLE claims
-- FIELDS TERMINATED BY ','
-- ENCLOSED BY '"'
-- LINES TERMINATED BY '\n'
-- IGNORE 1 ROWS
-- (Claim_ID, Food_ID, Receiver_ID, Status, @Timestamp)
-- SET
--     Timestamp = STR_TO_DATE(@Timestamp, '%c/%e/%Y %H:%i');

-- Count rows
SELECT COUNT(*) FROM providers;
SELECT COUNT(*) FROM receivers;
SELECT COUNT(*) FROM food_listings;
SELECT COUNT(*) FROM claims;

-- Check foreign keys
SELECT COUNT(*) FROM food_listings WHERE Provider_ID NOT IN (SELECT Provider_ID FROM providers);
SELECT COUNT(*) FROM claims WHERE Food_ID NOT IN (SELECT Food_ID FROM food_listings);
SELECT COUNT(*) FROM claims WHERE Receiver_ID NOT IN (SELECT Receiver_ID FROM receivers);

-- Providers & Receivers
SELECT City, COUNT(*) AS Provider_Count
FROM providers
GROUP BY City;

SELECT Type, COUNT(*) AS Total_Providers
FROM providers
GROUP BY Type
ORDER BY Total_Providers DESC;

SELECT r.Name, COUNT(c.Claim_ID) AS Total_Claims
FROM receivers r
JOIN claims c ON r.Receiver_ID = c.Receiver_ID
GROUP BY r.Name
ORDER BY Total_Claims DESC
LIMIT 10;

SELECT Location, COUNT(*) AS Food_Count
FROM food_listings
GROUP BY Location
ORDER BY Food_Count DESC
LIMIT 1;

SELECT Type, COUNT(*) AS Receiver_Count
FROM receivers
GROUP BY Type;

SELECT Name, Type, Contact
FROM providers
WHERE City = 'Markport';

-- Food Listings & Availability
SELECT SUM(Quantity) AS Total_Quantity
FROM food_listings;

SELECT Food_Type, COUNT(*) AS Count_Type
FROM food_listings
GROUP BY Food_Type
ORDER BY Count_Type DESC;

SELECT Meal_Type, COUNT(*) AS Count_Meals
FROM food_listings
GROUP BY Meal_Type;

SELECT Food_ID, Food_Name, Expiry_Date
FROM food_listings
WHERE Expiry_Date <= DATE_ADD(CURDATE(), INTERVAL 3 DAY);

-- Claims & Distribution
SELECT f.Food_Name, COUNT(c.Claim_ID) AS Claim_Count
FROM claims c
JOIN food_listings f ON c.Food_ID = f.Food_ID
GROUP BY f.Food_Name
ORDER BY Claim_Count DESC;

SELECT p.Name, COUNT(c.Claim_ID) AS Successful_Claims
FROM claims c
JOIN food_listings f ON c.Food_ID = f.Food_ID
JOIN providers p ON f.Provider_ID = p.Provider_ID
WHERE c.Status = 'Completed'
GROUP BY p.Name
ORDER BY Successful_Claims DESC
LIMIT 1;

SELECT Status, COUNT(*) AS Status_Count
FROM claims
GROUP BY Status;

SELECT r.Name, AVG(f.Quantity) AS Avg_Quantity_Claimed
FROM claims c
JOIN receivers r ON c.Receiver_ID = r.Receiver_ID
JOIN food_listings f ON c.Food_ID = f.Food_ID
GROUP BY r.Name
ORDER BY Avg_Quantity_Claimed DESC
LIMIT 10;

SELECT f.Meal_Type, COUNT(*) AS Claim_Count
FROM claims c
JOIN food_listings f ON c.Food_ID = f.Food_ID
GROUP BY f.Meal_Type
ORDER BY Claim_Count DESC;

-- Trends & Insights
SELECT p.Name, SUM(f.Quantity) AS Total_Donated
FROM providers p
JOIN food_listings f ON p.Provider_ID = f.Provider_ID
GROUP BY p.Name
ORDER BY Total_Donated DESC
LIMIT 10;

SELECT DATE_FORMAT(Timestamp, '%Y-%m') AS Month, COUNT(*) AS Claims
FROM claims
GROUP BY Month
ORDER BY Month;

SELECT f.Location, COUNT(*) AS Completed_Claims
FROM claims c
JOIN food_listings f ON c.Food_ID = f.Food_ID
WHERE c.Status = 'Completed'
GROUP BY f.Location
ORDER BY Completed_Claims DESC
LIMIT 5;

-- ✅ Fixed Query 19
SELECT f.Food_ID, f.Food_Name,
       TIMESTAMPDIFF(HOUR, c.Timestamp, f.Expiry_Date) AS Hours_Until_Expiry
FROM claims c
JOIN food_listings f ON c.Food_ID = f.Food_ID
WHERE c.Status = 'Completed'
LIMIT 10;

SELECT p.Name, COUNT(f.Food_ID) AS Unclaimed_Food
FROM providers p
JOIN food_listings f ON p.Provider_ID = f.Provider_ID
WHERE NOT EXISTS (SELECT 1 FROM claims c WHERE c.Food_ID = f.Food_ID)
GROUP BY p.Name
ORDER BY Unclaimed_Food DESC
LIMIT 10;



-- SQL Project - Data Cleaning

SELECT *
FROM world_layoffs.layoffs;

-- Create a staging table 'layoffs_staging' based on 'layoffs' for cleaning
CREATE TABLE world_layoffs.layoffs_staging
LIKE world_layoffs.layoffs;

INSERT world_layoffs.layoffs_staging
SELECT *
FROM world_layoffs.layoffs;

-- Now when we are data cleaning we usually follow a few steps
-- 1. Check for duplicates and remove them
-- 2. Standardize data and fix errors
-- 3. Look at null values or blank values
-- 4. Remove any columns and rows that are not necessary - few ways

-- 1. Remove Duplicates
# First let's check for duplicates

SELECT *
FROM world_layoffs.layoffs_staging;

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM world_layoffs.layoffs_staging;

-- Identify and handle duplicates in 'layoffs_staging'
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage,
country, funds_raised_millions) AS row_num
FROM world_layoffs.layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- Select records for 'Casper' to review
SELECT *
FROM world_layoffs.layoffs_staging
WHERE company = 'Casper';

-- Create 'layoffs_staging2' with cleaned data
CREATE TABLE `world_layoffs`.`layoffs_staging2` (
  `company` text,
  `location`text,
  `industry`text,
  `total_laid_off` INT,
  `percentage_laid_off` text,
  `date` text,
  `stage`text,
  `country` text,
  `funds_raised_millions` int,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Populate 'layoffs_staging2' with data and assign row numbers for duplicates
INSERT INTO world_layoffs.layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage,
country, funds_raised_millions) AS row_num
FROM world_layoffs.layoffs_staging;

-- Now that we have this we can delete rows were row_num is greater than 1
DELETE
FROM world_layoffs.layoffs_staging2
WHERE row_num > 1;


-- 2. Standardize Data

SELECT *
FROM world_layoffs.layoffs_staging2;

SELECT company, TRIM(company)
FROM world_layoffs.layoffs_staging2;

-- Trim leading and trailing spaces in company names
UPDATE world_layoffs.layoffs_staging2
SET company = TRIM(company);

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry LIKE 'Crypto%';

-- Consolidate 'Crypto' industry terms
UPDATE world_layoffs.layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Check distinct 'industry' values after standardization
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM world_layoffs.layoffs_staging2
ORDER BY 1;

-- Clean up 'country' column by removing trailing '.' from 'United States'
UPDATE world_layoffs.layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Let's also fix the date columns:
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM world_layoffs.layoffs_staging2;

-- Convert date strings to proper DATE format
UPDATE world_layoffs.layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Alter 'date' column to DATE type for consistency
ALTER TABLE world_layoffs.layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM world_layoffs.layoffs_staging2;


-- 3. Null or blank values

-- Let's take a look at these
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company = 'Airbnb';

-- It looks like Airbnb is a travel, but this one just isn't populated.
-- We can sure that it's the same for the others. So we write a query that
-- if there is another row with the same company name and it will update it to
-- the non-null industry values makes it easy so if there were thousands
-- we wouldn't have to manually check them all


-- We should set the blanks to nulls since those are typically easier to work with
UPDATE world_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Now if we check those are all null
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NUll
OR industry = ''
ORDER BY industry;


SELECT t1.industry, t2.industry
FROM world_layoffs.layoffs_staging2 t1
JOIN world_layoffs.layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

-- Now we need to populate those nulls if possible
UPDATE world_layoffs.layoffs_staging2 t1
JOIN world_layoffs.layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;


-- 4. Remove any columns and rows that are not necessary

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NUll
AND percentage_laid_off IS NULL;

-- Remove rows with null values
DELETE
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NUll
AND percentage_laid_off IS NULL;

SELECT *
FROM world_layoffs.layoffs_staging2;

-- Remove staging table columns
ALTER TABLE world_layoffs.layoffs_staging2
DROP COLUMN row_num;

SELECT *
FROM world_layoffs.layoffs_staging2;
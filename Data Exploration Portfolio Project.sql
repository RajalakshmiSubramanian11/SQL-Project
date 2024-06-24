-- EDA

-- Here we are just going to explore the data and find trends or patterns like outliers

-- Perform Exploratory Data Analysis on layoffs_staging2 table
-- Retrieve all data from layoffs_staging2

SELECT *
FROM world_layoffs.layoffs_staging2;


-- Find maximum total laid off and percentage laid off
SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM world_layoffs.layoffs_staging2;

-- Identify records where 100% of employees were laid off, sorted by funds raised descending
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

-- Calculate total layoffs per company, sorted by highest layoffs
SELECT company, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

-- Determine the earliest and latest dates in the dataset
SELECT MIN(`date`), MAX(`date`)
FROM world_layoffs.layoffs_staging2;

-- Summarize total layoffs by industry, ordered by highest layoffs
SELECT industry, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

-- Similarly we analyse the total layoffs by country, ordered by highest layoffs
SELECT country, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

-- Analyze yearly trends of layoffs, sorted by latest year
SELECT YEAR(`date`), SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

-- Verify layoffs by stage of the company, ordered by highest layoffs
SELECT stage, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

-- Calculate monthly layoffs and display in chronological order
SELECT SUBSTRING(`date`, 1,7) AS `MONTH`, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
WHERE SUBSTRING(`date`, 1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1;

-- Calculate rolling total of layoffs by month
WITH Rolling_Total AS
(
SELECT SUBSTRING(`date`, 1,7) AS `MONTH`, SUM(total_laid_off) AS total_off
FROM world_layoffs.layoffs_staging2
WHERE SUBSTRING(`date`, 1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1
)
SELECT `MONTH`, total_off
,SUM(total_off) OVER(ORDER BY `MONTH`) AS rolling_total
FROM Rolling_Total;

-- Review company layoffs annually, identifying top performers
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;

-- Top companies ranked by yearly layoffs
WITH Company_Year (company, years, total_laid_off) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY company, YEAR(`date`)
), Company_Year_Rank AS (
SELECT *,
DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_Year
WHERE years IS NOT NULL
)
SELECT *
FROM Company_Year_Rank
WHERE Ranking <= 5;
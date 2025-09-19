-- =============================================
--  DATA CLEANING & PREPARATION
-- =============================================
-- Create staging table
CREATE TABLE layoffs_staging LIKE layoffs;

INSERT INTO layoffs_staging 
SELECT *
FROM layoffs;

-- Identify duplicates using ROW_NUMBER
WITH duplicate_cte AS (
    SELECT *,
           ROW_NUMBER() OVER(
               PARTITION BY company, location, industry, total_laid_off, 
                            percentage_laid_off, `date`, stage, country, 
                            funds_raised_millions
           ) AS row_num
    FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- Create second staging table with row_num column
CREATE TABLE layoffs_staging2 (
  company TEXT,
  location TEXT,
  industry TEXT,
  total_laid_off INT DEFAULT NULL,
  percentage_laid_off TEXT,
  date TEXT,
  stage TEXT,
  country TEXT,
  funds_raised_millions DOUBLE DEFAULT NULL,
  row_num INT
);

INSERT INTO layoffs_staging2
SELECT *,
       ROW_NUMBER() OVER(
           PARTITION BY company, location, industry, total_laid_off, 
                        percentage_laid_off, `date`, stage, country, 
                        funds_raised_millions
       ) AS row_num
FROM layoffs_staging;

-- Remove duplicates
DELETE FROM layoffs_staging2
WHERE row_num > 1;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- ======================================================
--  STANDARDIZING DATA
-- ======================================================

-- Trim spaces in company names
UPDATE layoffs_staging2
SET company = TRIM(company);

-- Standardize industry (e.g., all "Crypto..." → "Crypto")
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Clean up country names
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Convert text dates into proper DATE format
UPDATE layoffs_staging2
SET date = STR_TO_DATE(date, '%m/%d/%Y');

-- Fill missing industries using self-join
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
  ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
  AND t2.industry IS NOT NULL;

-- Remove records with no layoff information
DELETE FROM layoffs_staging2
WHERE total_laid_off IS NULL
  AND percentage_laid_off IS NULL;

-- Final Cleaned Data
SELECT *
FROM layoffs_staging2;

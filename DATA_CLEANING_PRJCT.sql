-- PROJECT1 DATA CLEANING
SELECT*
FROM layoffs;

-- WHAT WE ARE GOING TO DO IN THIS PROJECT 
-- 1. REMOVE DUPLICATES
-- 2. STANDARDIZE THE DATA 
-- 3. NULL VALUES AND BLANK VALUES 
-- 4. REMOVE ANY COLUMNS

-- 1. REMOVE DUPLICATES
CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT*
FROM layoffs_staging;

INSERT INTO layoffs_staging
SELECT*
FROM layoffs;

SELECT*,
ROW_NUMBER() 
OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage,
 country, funds_raised_millions ) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS 
(
SELECT*,
ROW_NUMBER() 
OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage,
 country, funds_raised_millions ) AS row_num
FROM layoffs_staging
)
SELECT*
FROM duplicate_cte
WHERE row_num > 1;

SELECT*
FROM layoffs_staging
WHERE company = 'Casper';

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
  
  SELECT*
  FROM layoffs_staging2;
  
  INSERT INTO layoffs_staging2
  SELECT*,
  ROW_NUMBER() 
OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage,
 country, funds_raised_millions ) AS row_num
FROM layoffs_staging;

SET SQL_SAFE_UPDATES = 0;
  
DELETE
FROM layoffs_staging2
WHERE row_num > 1;
  
 SELECT*
FROM layoffs_staging2
WHERE company = 'Airbnb';

-- 2. STANDARDIZE THE DATA 

SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

SELECT*
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
where industry like 'Crypto%';

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
where country like 'United States%'; 

SELECT `date`
FROM layoffs_staging2;


SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y' )
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `DATE` = STR_TO_DATE(`date`, '%m/%d/%Y' );
 

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- 3. NULL VALUES AND BLANK VALUES
SELECT*
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging
SET industry = NULL
where industry = '';

SELECT *
FROM layoffs_staging
WHERE industry IS NULL ;

SELECT*
FROM layoffs_staging
WHERE company = 'Airbnb';

SELECT*, row_number() OVER(PARTITION BY t1.company)
FROM layoffs_staging t1
	JOIN layoffs_staging t2
    ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL )
AND t2.industry IS NOT NULL;

SELECT t1.industry, t2.industry, row_number() OVER(PARTITION BY t1.company)
FROM layoffs_staging t1
	JOIN layoffs_staging t2
    ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;
    
UPDATE layoffs_staging t1
	JOIN layoffs_staging t2
    ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

SELECT*
FROM layoffs_staging
WHERE company = 'Airbnb';

SELECT*
FROM layoffs_staging
WHERE company LIKE 'BaLL%';

-- 4. REMOVE ANY COLUMNS

SELECT*
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;





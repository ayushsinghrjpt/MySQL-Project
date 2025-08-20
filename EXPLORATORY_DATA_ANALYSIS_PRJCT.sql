-- EXPLORATORY DATA ANALYSIS

SELECT*
FROM layoffs_staging;

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging;

SELECT*
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;

SELECT*
FROM layoffs_staging
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

SELECT company, SUM(total_laid_off)
FROM layoffs_staging
GROUP BY company
ORDER BY 2 DESC;


SET SQL_SAFE_UPDATES = 0;


UPDATE layoffs_staging
SET `date` = str_to_date(`date`, '%m/%d/%Y');

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging;

SELECT industry, SUM(total_laid_off)
FROM layoffs_staging
GROUP BY industry
ORDER BY 2 DESC;

SELECT country, SUM(total_laid_off)
FROM layoffs_staging
GROUP BY country
ORDER BY 2 DESC;

SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

SELECT stage, SUM(total_laid_off)
FROM layoffs_staging
GROUP BY stage
ORDER BY 1 DESC;

SELECT stage, SUM(total_laid_off)
FROM layoffs_staging
GROUP BY stage
ORDER BY 2 DESC;

SELECT company, AVG(percentage_laid_off)
FROM layoffs_staging
GROUP BY company
ORDER BY 2 DESC 
;

SELECT SUBSTRING(`date`,1,7 ) AS `MONTH`, SUM(total_laid_off) AS total_off
FROM layoffs_staging
WHERE SUBSTRING(`date`,1,7 ) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC;

WITH Rolling_Total AS (
		SELECT SUBSTRING(`date`,1,7 ) AS `MONTH`, SUM(total_laid_off) AS total_off
		FROM layoffs_staging
		WHERE SUBSTRING(`date`,1,7 ) IS NOT NULL
		GROUP BY `MONTH`
		ORDER BY 1 ASC
)
SELECT `MONTH`, total_off,SUM(total_off) OVER( ORDER BY `MONTH` ) AS ROLLING_TOTAL
FROM Rolling_Total;

SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;

WITH Company_Year (Company, `Year`, Total_Laid_Off) AS(
SELECT company, YEAR(`date`), SUM(total_laid_off) AS total_off
FROM layoffs_staging
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC
), Company_Rank AS (
SELECT*, DENSE_RANK() OVER(PARTITION BY `Year` ORDER BY Total_Laid_Off DESC) AS Ranking
FROM Company_Year
WHERE Total_Laid_Off IS NOT NULL AND `Year` IS NOT NULL 
) 
SELECT*
FROM Company_Rank
WHERE Ranking <= 5
;






























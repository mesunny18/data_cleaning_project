#Creating a table FROM Raw data to apply operatiONs to clean data

CREATE TABLE layoff_staging LIKE layoffs;
INSERT layoff_staging SELECT * FROM layoffs;

#checking if table has duplicate values
WITH CTE AS
(
SELECT *, ROW_NUMBER() OVER(PARTITION BY company, locatiON, industry, `date`, stage, country) AS row_num FROM layoff_staging
)
DELETE FROM CTE WHERE row_num >1;

#Creating a new table includes row_num because CTE is not updatable in MySQL
CREATE TABLE `layoff_staging2` (
  `company` text,
  `locatiON` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_milliONs` int DEFAULT NULL,
  `row_num` int
  
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoff_staging2
SELECT *, ROW_NUMBER() OVER(PARTITION BY company, locatiON, industry,total_laid_off,percentage_laid_off, `date`, stage, country,funds_raised_milliONs ) as row_num FROM layoff_staging;

SELECT * FROM layoff_staging2 WHERE row_num >1;

DELETE FROM layoff_staging2 WHERE row_num >1;

#standartisatiON 
UPDATE layoff_staging2
SET company= trim(company);


UPDATE layoff_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT COUNTRY , trim(TRAILING '.' FROM COUNTRY) FROM layoff_staging2
WHERE country LIKE 'United States%';


SELECT * FROM layoff_staging2
WHERE industry IS NULL;

SELECT t1.industry , t2.industry FROM layoff_staging2 t1
JOIN layoff_staging2 t2 ON t1.company=t2.company
WHERE t1.industry IS NULL and t2.industry IS NOT NULL;

UPDATE layoff_staging2 t1
JOIN layoff_staging2 t2 ON t1.company=t2.company
SET t1.industry=t2.industry
WHERE t1.industry IS NULL and t2.industry IS NOT NULL;


UPDATE layoff_staging2
SET `date`=str_to_date(`date`,'%m/%d/%Y'); 

ALTER TABLE layoff_staging2
MODIFY COLUMN `date` DATE;

DELETE FROM layoff_staging2 
WHERE total_laid_off IS NULL and percentage_laid_off IS NULL;

SELECT * FROM layoff_staging2
-- SQL Project - Data Cleaning

USE world_layoffs;

SELECT *
FROM layoffs;


-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary - few ways

-- first thing we want to do is create a staging table. This is the one we will work in and clean the data. We want a table with the raw data in case something happens

CREATE TABLE world_layoffs.layoffs_staging 
LIKE world_layoffs.layoffs;

INSERT layoffs_staging 
SELECT * FROM world_layoffs.layoffs;

select *
FROM layoffs_staging;

-- 1. REMOVE DUPLICATES

with dublicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`,
stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
select *
FROM dublicate_cte
WHERE row_num > 1;

SELECT *
FROM layoffs
WHERE company = 'Casper';

-- we copied the table layoffs_staging2, we can add 1 new coloumn 'row_num' as a int

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` text,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` text,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT layoffs_staging2 
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`,
stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

select *
FROM layoffs_staging2
where row_num>1;

-- Now we can delete the duplicates

delete
FROM layoffs_staging2
where row_num>1;

-- 2. STANDARDIZE THE DATA

select company, TRIM(company)
FROM layoffs_staging2;

-- We can take off the space on company coloumn

update layoffs_staging2
SET company = TRIM(company);

select distinct industry
FROM layoffs_staging2
order by 1;

select *
FROM layoffs_staging2
where industry like'%Crypto%';

update layoffs_staging2
set industry='Crypto'
where industry like'%Crypto%';

select distinct country
FROM layoffs_staging2
order by 1;

update layoffs_staging2
set country= TRIM ( TRAILING '.' from country)
where country like'United States%';

update layoffs_staging2
set country= 'United States'
where country like'United States%';

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

update layoffs_staging2
set `date`= '6/27/2022'
where `date` like  '%NULL%';

select `date` from layoffs_staging2;

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- 3. NULL VALUES OR BLANK VALUES

select  *
from layoffs_staging2
WHERE industry = 'NULL'
or industry= ''
or industry is null;

select  *
from layoffs_staging2
WHERE company like "%Airbnb%";

-- We can see the industry that is NULL by using JOIN 

SELECT  t1.company ,t1.industry, t2.industry
FROM layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE ( t1.industry IS NULL OR t1.industry= '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry= '';


UPDATE layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

select  *
from layoffs_staging2
WHERE company like "%Bally%";

-- 4. REMOVE ANY COLUMNS AND ROWS

select  *
from layoffs_staging2
WHERE (total_laid_off = 'NULL' or total_laid_off= '')
AND (percentage_laid_off = 'NULL' or percentage_laid_off= '');

select  *
from layoffs_staging2
WHERE (total_laid_off = 'NULL')
AND (percentage_laid_off = 'NULL');


DELETE
from layoffs_staging2
WHERE (total_laid_off = 'NULL')
AND (percentage_laid_off = 'NULL');

select  *
from layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;


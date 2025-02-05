-- Exploratory Data Analysis

select *
FROM layoffs_staging2;

-- Firstly we convert total_laid_off column from text to int 

select *
FROM layoffs_staging2
where total_laid_off= 'NULL';


UPDATE layoffs_staging2 
SET total_laid_off = NULL
WHERE total_laid_off= 'NULL'; 

UPDATE layoffs_staging2 
SET percentage_laid_off = NULL
WHERE percentage_laid_off = 'NULL'; 

ALTER TABLE layoffs_staging2
MODIFY COLUMN total_laid_off int;

-- EASIER QUERIES 

-- Looking at Percentage to see how big these layoffs were

select max(total_laid_off), max(percentage_laid_off)
FROM layoffs_staging2;

select *
FROM layoffs_staging2
where percentage_laid_off=1
order by total_laid_off desc;

-- Companies with the biggest single Layoff

select company, sum(total_laid_off)
FROM layoffs_staging2
group by company
order by 2 desc;

select  min(`date`), max(`date`)
FROM layoffs_staging2;

-- by industry

select industry, sum(total_laid_off)
FROM layoffs_staging2
group by industry
order by 2 desc;

-- by year
select YEAR(`date`), sum(total_laid_off)
FROM layoffs_staging2
group by YEAR(`date`)
order by 1 desc;

-- by month 
select substring(`date`,1,7) as `Month`, sum(total_laid_off)
FROM layoffs_staging2
group by `Month`
order by 1 asc;


WITH Rolling_Total AS
(
SELECT substring(`date`,1,7) AS `Month`, sum(total_laid_off) AS total_off
FROM layoffs_staging2
GROUP BY `Month`
ORDER BY 1 asc
)
SELECT `Month`,total_off,
sum(total_off) OVER(ORDER BY `Month`) AS rolling_total
FROM  Rolling_Total;

-- Companies with the most Layoffs

SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 desc;


WITH Company_Year (company,years, total_laid_off) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
)
SELECT *, DENSE_RANK() OVER(partition by YEARS ORDER BY total_laid_off DESC) as Ranking 
FROM Company_Year
WHERE years is not null
order by Ranking asc;


WITH Company_Year (company,years, total_laid_off) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
), Company_Year_Rank AS
(SELECT *, DENSE_RANK() OVER(partition by YEARS ORDER BY total_laid_off DESC) as Ranking 
FROM Company_Year
WHERE years is not null
)
SELECT *
FROM Company_Year_Rank
WHERE Ranking <= 5;



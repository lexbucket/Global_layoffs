-- DATA CLEANING LAYOFFS

-- 1. Remove duplicates (CTEs, Copy/create tables)
-- 2. Normalise data (Trim, Trim Trailing, working with dates, use of Distinct and LIKE, Update records)
-- 3. Dealing with Null/Empty values (convert empty to null values, Inner Joins to populate null values)
-- 4. Remove columns/rows (Alter... Drop, Delete records)

SELECT *
FROM layoffs;

-- Good practice to work with a 'copy' of the data so nothing is modified from the original/raw data
CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT *
FROM layoffs_staging;

------------------------------------------------------------------------------

-- 1. REMOVE DUPLICATES
-- 1.1 See the duplicates based on the columns in PARTITION BY 
WITH duplicate_cte AS (
SELECT *, ROW_NUMBER() OVER (PARTITION BY company, industry, percentage_laid_off, date, stage, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT * 
FROM duplicate_cte
WHERE row_num > 1 -- main condition, if it's >1 it's a duplicate
;

-- 1.2 To delete those registers we can create another table that includes already the row number previously calculated so we can remove those that are duplicates (>1)
-- REMEMBER, this query only creates the table but there's no data in it yet
CREATE TABLE `layoffs_staging2`(
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int -- adding this column to work with the row_numbers for duplicates
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- 1.3 populate the newly created table with the same query used for the CTE in 1.1
INSERT INTO layoffs_staging2
SELECT *, ROW_NUMBER() OVER (PARTITION BY company, industry, percentage_laid_off, date, stage, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- 1.4 confirm table is populated and also that duplicates can be seen
SELECT *
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

-- 1.5 Delete those duplicated records
DELETE
FROM layoffs_staging2
WHERE row_num > 1;

--------------------------------------------------------------------------------------------------

-- 2. NORMALISE DATA
-- Reviewing issues with the data (extra blank spaces, unwanted characters, data types). Explore columns.

-- *****************************
-- 2.1 Column 'company' -> TRIMING BLANK SPACES. 
-- Using TRIM and updating just the specific column on the table
UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT *
FROM layoffs_staging2;

-- ********************************
-- 2.2 If there's a column with dates always check the data type is correct so it can be manipulated properly
DESCRIBE layoffs_staging2;
-- column 'date' is text type. 

-- 2.2.1 check how it should look like
SELECT date, STR_TO_DATE(`date`, '%m/%d/%Y') -- column name and current format so MySQL understands it
FROM layoffs_staging2;

-- 2.2.2 update the column with the correct date FORMAT (not type yet)
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- 2.2.3 Change data TYPE. Confirm with the panel or with DESCRIBE the data type changed
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- *********************************
-- 2.3 Normalise names that are misspelled or clearly should be one only. 
-- 2.3.1 Check distinct different columns first. 
SELECT distinct industry -- Crypto vs cryptocurrency
FROM layoffs_staging2
ORDER BY 1;

SELECT distinct location -- Found a couple of cities misspelled: Dusseldorf and Malmo
FROM layoffs_staging2
ORDER BY 1;

SELECT distinct country -- Found a DOT in 'United States.' 
FROM layoffs_staging2
ORDER BY 1;

SELECT distinct stage -- Seems normal
FROM layoffs_staging2
ORDER BY 1;

-- 2.3.2 Show all the found records with similar name in the respective column to make sure what will be changed is correct
-- industry column in this case
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

-- 2.3.3 Update the table 
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- 2.3.4 REPEAT PROCESS FOR THE OTHER COLUMNS
-- location
SELECT *
FROM layoffs_staging2
WHERE location LIKE '%sseldorf';

UPDATE layoffs_staging2
SET location = 'Dusseldorf'
WHERE location LIKE '%sseldorf';

SELECT *
FROM layoffs_staging2
WHERE location LIKE 'Mal%';

UPDATE layoffs_staging2
SET location = 'Malmo'
WHERE location LIKE 'Mal%';

-- 2.3.5 country. There are two options: doing what has been done for the previous columns or using TRIM TRAIL as it only reqwuires to remove a character at the end
-- OPTION 1: Repeating previous process:
/* SELECT *
FROM layoffs_staging2
WHERE country LIKE '%states.';

UPDATE layoffs_staging2
SET country = 'United States'
WHERE country LIKE '%states.';
*/
-- OPTION 2: Using Trim Trail
SELECT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

--------------------------------------------------------------------------------------------------------------------

-- 3. DEALING WITH NULL/EMPTY VALUES
-- 3.1 Check if there are empty or NULL cells
SELECT distinct industry
FROM layoffs_staging2
ORDER BY industry;

-- 3.2 In some occasions there will be NULL values and empty cells. To deal with them in an easier way sometimes it's better to convert empty values to null.
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL OR industry = "" -- Companies are: Airbnb, Bally's interactive, Carvana, Juul
;

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = "";

-- 3.3 Back to checking if the null values can be populated with info existing in the table.
-- 'industry' for the previous companies could be populated with the info of the same 'company' and 'location' in other records.
SELECT *
FROM layoffs_staging2 t1
WHERE company = 'Juul';

-- An inner Join with the same table can help to verify that information. It is known they should match 'company' and 'location' so we can populate 'industry'.
-- In this particular example only the company "Bally's interactive" has no other records that match the criteria so it won't be updated
SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE t1.industry IS NULL
	AND t2.industry IS NOT NULL -- This is important as we only want to see the registers that match with t2 that are not null.
;

-- 3.4 Now we can update the table based on this results:
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry -- this one in particular adds the missing value if it exists
WHERE t1.industry IS NULL
	AND t2.industry IS NOT NULL
;

-- Explore again to confirm
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL -- Companie Bally's interactiveis the only one in this search, so it's correct
;

------------------------------------------------------------------
-- 4. Deleting columns and rows we don't need
-- 4.1 Columns, like the previously created for row_num are not necessary anymore

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging2;

-- 4.2 Delete rows we may not need, like null values from both 'total_laid_off' and 'percentage_laid_off'
-- WARNING: always make sure it's ok to delete the data, what percentage of the data are you deleting? is it acceptable? can I need it later?

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
	AND percentage_laid_off IS NULL;

------------------------------------------------------------------------

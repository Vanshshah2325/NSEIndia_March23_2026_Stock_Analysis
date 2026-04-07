create	database nse_Project;
use nse_project;

CREATE TABLE daily_prices (
    SYMBOL VARCHAR(20),
    SECURITY VARCHAR(100),
    PREV_CL_PR FLOAT,
    OPEN_PRICE FLOAT,
    HIGH_PRICE FLOAT,
    LOW_PRICE FLOAT,
    CLOSE_PRICE FLOAT,
    NET_TRDVAL FLOAT,
    NET_TRDQTY BIGINT,
    TRADES BIGINT,
    HI_52_WK FLOAT,
    LO_52_WK FLOAT,
    SERIES VARCHAR(10)
);

CREATE TABLE market_cap (
    id INT AUTO_INCREMENT PRIMARY KEY,
    TRADE_DATE DATE,
    SYMBOL VARCHAR(20),
    SERIES VARCHAR(10),
    SECURITY_NAME VARCHAR(150),
    CATEGORY VARCHAR(50),
    LAST_TRADE_DATE DATE,
    FACE_VALUE DECIMAL(10,2),
    ISSUE_SIZE BIGINT,
    CLOSE_PRICE DOUBLE,
    MARKET_CAP DOUBLE
);

-- ------------------------------------------------Load The Dataset-----------------------------------------------------
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/nse_market_cap_23032026.csv'
INTO TABLE market_cap
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@trade_date, symbol, series, security_name, category, @last_trade_date, face_value, issue_size, close_price, market_cap)
SET 
trade_date =
  CASE
    WHEN @trade_date LIKE '__-__-____' THEN STR_TO_DATE(@trade_date, '%d-%m-%Y')
    WHEN @trade_date LIKE '__-___-__' THEN STR_TO_DATE(@trade_date, '%d-%b-%y')
    ELSE @trade_date
  END,

last_trade_date =
  CASE
    WHEN TRIM(@last_trade_date) = '' THEN NULL
    WHEN TRIM(@last_trade_date) = 'Not Traded' THEN NULL
    WHEN @last_trade_date LIKE '__-__-____' THEN STR_TO_DATE(@last_trade_date, '%d-%m-%Y')
    WHEN @last_trade_date LIKE '__-___-__' THEN STR_TO_DATE(@last_trade_date, '%d-%b-%y')
    ELSE @last_trade_date
  END;
  
  set sql_safe_updates = 0;
  select count(*) as total_row from daily_prices ;
  select count(*) as total_row from market_cap ;
  
-- --------------------------------------------------Data Cleaning - daily_prices Table------------------------------------
select * from daily_prices where SYMBOl=" ";
delete from daily_prices where SYMBOl=" ";

select count(*) from daily_prices
where SERIES != 'EQ';

select count(*) from daily_prices where SERIES != 'EQ';
delete from daily_prices where SERIES != 'EQ';

-- Find zero trading quantity
select * from daily_prices
where NET_TRDQTY = 0;

delete from daily_prices
where NET_TRDQTY = 0;

select count(*) from daily_prices
where SYMBOL like 'NIFTY%';
delete from daily_prices
where SYMBOL like 'NIFTY%';

update daily_prices set SYMBOL = trim(SYMBOl);

--  Check negative price values
select * from daily_prices
where OPEN_PRICE < 0 
   OR HIGH_PRICE < 0 
   OR LOW_PRICE < 0 
   OR CLOSE_PRICE < 0;

-- 14. Min, Max, Avg price
select 
MIN(CLOSE_PRICE), 
MAX(CLOSE_PRICE), 
AVG(CLOSE_PRICE)
from daily_prices;

select * from daily_prices;
select count(*) from daily_prices;

-- --------------------------------------------------Data Cleaning - market_cap Table------------------------------------

select * from market_cap where SYMBOl=" ";

update market_cap set SYMBOL = TRIM(SYMBOL);

-- Find duplicate SYMBOL
SELECT SYMBOL, COUNT(*) 
FROM market_cap
GROUP BY SYMBOL
HAVING COUNT(*) > 1;

select * from market_cap
WHERE MARKET_CAP IS NULL;

select * from market_cap;
select count(*) from daily_prices;


commit;

-- --------------------------------------------- Working with Clean Data --------------------------------------------

select count(*) from nse_clean;
select * from nse_clean;

alter table nse_clean rename column SECURITY to  COMPANY_NAME ; 
select * from nse_clean;


-- Top 10 stocks by Market Cap
select * from nse_clean order by MARKET_CAP desc limit 10;

-- Top 10 by Trading Volume
select SYMBOL , NET_TRDQTY from nse_clean order by NET_TRDQTY desc limit 10 ;

-- Top 10 by Trades
select SYMBOL , NET_TRDQTY from nse_clean order by TRADES desc limit 10 ;

-- Top 10 by Trades
select SYMBOL , OPEN_PRICE , CLOSE_PRICE , MARKET_CAP  from nse_clean where CLOSE_PRICE>OPEN_PRICE;

-- Daily Return Calculation 
select SYMBOL , ((CLOSE_PRICE - PREV_CL_PR)/PREV_CL_PR)*100 as Daily_Return from nse_clean;

-- Top 10 Gainers
select SYMBOL , ((CLOSE_PRICE - PREV_CL_PR)/PREV_CL_PR)*100 as Daily_Return from nse_clean order by Daily_Return desc limit 10;

-- Top 10 losers 
select SYMBOL , ((CLOSE_PRICE - PREV_CL_PR)/PREV_CL_PR)*100 as Daily_Return from nse_clean order by Daily_Return limit 10;

-- Highest Price Range
select Symbol , HIGH_PRICE , LOW_PRICE , (HIGH_PRICE-LOW_PRICE) as PRICE_RANGE from nse_clean order by PRICE_RANGE desc limit 10 ;
select Symbol , HIGH_PRICE , LOW_PRICE , (HIGH_PRICE-LOW_PRICE) as PRICE_RANGE from (select * from nse_clean where OPEN_PRICE<50) as nse_clean order by PRICE_RANGE desc limit 10 ;


select * from nse_clean;

-- Cap Category
select SYMBOL , case 
when MARKET_CAP > 200000000000 then "Large Cap"
when MARKET_CAP > 50000000000 then "Mid Cap"
else "Small Cap"
end as CAP_CATEGORY from  nse_clean;

-- count based on Cap_category
select case 
when MARKET_CAP > 200000000000 then "Large Cap" -- 20 abj
when MARKET_CAP > 50000000000 then "Mid Cap"	-- 05 abj
else "Small Cap"
end as CAP_CATEGORY , count(*) as totalstock from  nse_clean group by CAP_CATEGORY;

select * from nse_clean;

-- Avg Close Price per Category
select case 
when MARKET_CAP > 200000000000 then "Large Cap" -- 20 abj
when MARKET_CAP > 50000000000 then "Mid Cap"	-- 05 abj
else "Small Cap"
end as CAP_CATEGORY , avg(CLOSE_PRICE) as AVG_PRICE from  nse_clean group by CAP_CATEGORY;

-- Top 5 Large Cap by Volume
select SYMBOL , NET_TRDQTY , MARKET_CAP from nse_clean where MARKET_CAP > 200000000000 order by NET_TRDQTY desc limit 5;

-- Rank by Market Cap
select SYMBOL , MARKET_CAP , RANK() OVER (order by MARKET_CAP desc) as rank_mcap from nse_clean ;

-- Rank by Market Cap
select SYMBOL , MARKET_CAP , 
((CLOSE_PRICE - PREV_CL_PR)/PREV_CL_PR)*100 as Daily_return ,
RANK() OVER (order by (((CLOSE_PRICE - PREV_CL_PR)/PREV_CL_PR)*100) desc) as rank_return from nse_clean ;


-- Top 3 per Cap Category
select * from 
(select SYMBOL , MARKET_CAP , case 
when MARKET_CAP > 200000000000 then "Large Cap" -- 20 abj
when MARKET_CAP > 50000000000 then "Mid Cap"	-- 05 abj
else "Small Cap"
end as CAP_CATEGORY,
RANK() OVER(partition by case 
when MARKET_CAP > 200000000000 then "Large Cap" -- 20 abj
when MARKET_CAP > 50000000000 then "Mid Cap"	-- 05 abj
else "Small Cap"
end order by MARKET_CAP desc ) as rnk 
from nse_clean) t where rnk<=3 ;



commit ;


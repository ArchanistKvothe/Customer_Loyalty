########################################################################################################################
#                            Section 1 - Loading data
########################################################################################################################


create database Loyalty; -- To create my database, Loyalty
use Loyalty; -- To make sure I'm using the database, Loyalty

drop table if exists location; -- To make sure I wasn't making duplicate tables
create table Location ( -- To create the table, making sure the column headers match the raw data
    store_location int,
    region         varchar(5),
    province       varchar(30),
    city           varchar(30),
    postal_code    varchar(10),
    banner         varchar(10),
    store_num      int
);

describe location; -- To make sure the table is created correctly
truncate location; -- In case the data was loaded incorrectly

show global variables like 'local_infile'; -- To check if I could load csv files
set global local_infile  = true; -- To make sure I could load csv files

load data local infile 'C:\\Users\\dozie\\OneDrive\\SQL folders\\Charles Ohiri Sql Loyalty Test\\quiz_data\\location.csv'
into table Location
fields terminated by ','
    enclosed by '"' -- To make sure when loading the data, the correct deliminators are recognized
lines terminated by '\r\n' -- To recognize when data in each row stops and the next row begins
ignore 1 lines
;

select * from Location; -- To make sure my data is loaded correctly
select max(region) from location;

drop table if exists products;

create table Products ( -- To create the table, making sure the column headers match the raw data
    product_key    bigint,
    sku            tinyint,
    upc           bigint,
    item_name      bigint,
    item_description varchar(20),
    department      varchar(20),
    category        varchar(20)
);


describe products;

truncate products;

load data local infile 'C:\\Users\\dozie\\OneDrive\\SQL folders\\Charles Ohiri Sql Loyalty Test\\quiz_data\\product.csv'
into table products
fields terminated by ',' -- columns separated by ','
    enclosed by '"' -- referring to values in fields with '"'
lines terminated by '\r\n' -- To recognize when data in each row stops and the next row begins
ignore 1 lines
;

select count(*) from products; -- To make sure my data is loaded correctly

select
    item_name,
    department,
    category,
    count(*) CountDups
from products
group by 1,2,3
order by CountDups desc
;                             -- Checking for duplicates

select *,
       row_number() over (partition by item_name ) as CntDup
        from products;

drop table if exists txn1;

create table txn1 ( -- To create the table, making sure the column headers match the raw data
    store_location_key bigint,
    product_key bigint,
  collector_key bigint,
  trans_dt date,
  sales decimal(10, 2),
  units int,
  trans_key numeric(30, 0)
);
drop table txn1;


load data local infile 'C:\\Users\\dozie\\OneDrive\\SQL folders\\Charles Ohiri Sql Loyalty Test\\quiz_data\\trans_fact_1.csv'
into table txn1
fields terminated by ','
    enclosed by '"' -- To make sure when loading the data, the correct deliminators are recognized
lines terminated by '\r\n' -- To recognize when data in each row stops and the next row begins
ignore 1 lines
;

select year(trans_dt)
from txn1; -- To make sure the date format is recognized

drop table txn2;

create table txn2 (
    store_location_key bigint,
    product_key bigint,
  collector_key bigint,
  trans_dt date,
  sales decimal(10, 2),
  units int,
  trans_key numeric(30, 0)
);

load data local infile 'C:\\Users\\dozie\\OneDrive\\SQL folders\\Charles Ohiri Sql Loyalty Test\\quiz_data\\trans_fact_2.csv'
into table txn2
fields terminated by ','
    enclosed by '"' -- To make sure when loading the data, the correct deliminators are recognized
lines terminated by '\r\n' -- To recognize when data in each row stops and the next row begins
ignore 1 lines
;
select * from txn2;
select year(trans_dt)
from txn2;

drop table txn3;
create table txn3 (
    store_location_key bigint,
    product_key bigint,
  collector_key bigint,
  trans_dt date,
  sales decimal(10, 2),
  units int,
  trans_key numeric(30, 0)
);

load data local infile 'C:\\Users\\dozie\\OneDrive\\SQL folders\\Charles Ohiri Sql Loyalty Test\\quiz_data\\trans_fact_3.csv'
into table txn3
fields terminated by ','
    enclosed by '"' -- To make sure when loading the data, the correct deliminators are recognized
lines terminated by '\r\n' -- To recognize when data in each row stops and the next row begins
ignore 1 lines -- Ignore the first line of field information
;

select * from txn3;
select year(trans_dt)
from txn3;

drop table txn4;
create table txn4 (
    collector_key bigint,
    trans_dt date,
    store_location_key bigint,
    product_key bigint,
  sales decimal(10, 2),
  units int,
  trans_key numeric(30, 0)
);

load data local infile 'C:\\Users\\dozie\\OneDrive\\SQL folders\\Charles Ohiri Sql Loyalty Test\\quiz_data\\trans_fact_4.csv'
into table txn4
fields terminated by '\,'
lines terminated by '\r\n'
ignore 1 lines  -- Ignore the first line of field information
(collector_key, @trans_dt, store_location_key, product_key, sales, units, trans_key)
set trans_dt = str_to_date(@trans_dt, '%m/%d/%Y'); -- set the right format for the datetime object

select * from txn4;
select year(trans_dt)
from txn4;

drop table txn5;

create table txn5 (
    collector_key bigint,
    trans_dt date,
    store_location_key bigint,
    product_key bigint,
  sales decimal(10, 2),
  units int,
  trans_key numeric(30, 0)
);

load data local infile 'C:\\Users\\dozie\\OneDrive\\SQL folders\\Charles Ohiri Sql Loyalty Test\\quiz_data\\trans_fact_5.csv'
into table txn5
fields terminated by ','
    enclosed by '"'
lines terminated by '\r\n'
ignore 1 lines
;

select * from txn5;
select year(trans_dt)
from txn5;
select count(*) from txn5;
drop table txn6;

create table txn6 (
    collector_key bigint,
    trans_dt date,
    store_location_key bigint,
    product_key bigint,
  sales decimal(10, 2),
  units int,
  trans_key numeric(30, 0)
);

load data local infile 'C:\\Users\\dozie\\OneDrive\\SQL folders\\Charles Ohiri Sql Loyalty Test\\quiz_data\\trans_fact_6.csv'
into table txn6
fields terminated by ','
    enclosed by '"'
lines terminated by '\r\n'
ignore 1 lines
(collector_key, @trans_dt, store_location_key, product_key, sales, units, trans_key)
set trans_dt = str_to_date(@trans_dt, '%m/%d/%Y'); -- set the right format for the datetime object
;
truncate txn6;

select count(*) from txn6;
select year(trans_dt)
from txn6;
drop table txn7;

create table txn7 (
    collector_key bigint,
    trans_dt date,
    store_location_key bigint,
    product_key bigint,
  sales decimal(10, 2),
  units int,
  trans_key numeric(30, 0)
);

load data local infile 'C:\\Users\\dozie\\OneDrive\\SQL folders\\Charles Ohiri Sql Loyalty Test\\quiz_data\\trans_fact_7.csv'
into table txn7
fields terminated by ','
    enclosed by '"'
lines terminated by '\r\n'
ignore 1 lines
;

select * from txn7;
select year(trans_dt)
from txn7;
drop table txn8;

create table txn8 (
     product_key bigint,
    collector_key bigint,
    trans_dt date,
    store_location_key bigint,
  sales decimal(10, 2),
  units int,
  trans_key numeric(30, 0)
);

load data local infile 'C:\\Users\\dozie\\OneDrive\\SQL folders\\Charles Ohiri Sql Loyalty Test\\quiz_data\\trans_fact_8.csv'
into table txn8
fields terminated by ','
    enclosed by '"'
lines terminated by '\r\n'
ignore 1 lines
(product_key, collector_key, @trans_dt, store_location_key, sales, units, trans_key)
set trans_dt = str_to_date(@trans_dt, '%m/%d/%Y') -- set the right format for the datetime object
;

select count(*) from txn8;
select year(trans_dt)
from txn8
where year(trans_dt) is not null;

truncate txn8;
drop table txn9;

create table txn9 (
     product_key bigint,
    collector_key bigint,
    trans_dt date,
    store_location_key bigint,
  sales decimal(10, 2),
  units int,
  trans_key numeric(30, 0)
);

load data local infile 'C:\\Users\\dozie\\OneDrive\\SQL folders\\Charles Ohiri Sql Loyalty Test\\quiz_data\\trans_fact_9.csv'
into table txn9
fields terminated by ','
    enclosed by '"'
lines terminated by '\r\n'
ignore 1 lines;

select * from txn9;
select year(trans_dt)
from txn9;
drop table txn10;

create table txn10 (
     product_key bigint,
    collector_key bigint,
    trans_dt date,
    store_location_key bigint,
  sales decimal(10, 2),
  units int,
  trans_key numeric(30, 0)
);

load data local infile 'C:\\Users\\dozie\\OneDrive\\SQL folders\\Charles Ohiri Sql Loyalty Test\\quiz_data\\trans_fact_10.csv'
into table txn10
fields terminated by ','
    enclosed by '"'
lines terminated by '\r\n'
ignore 1 lines;

select * from txn10;
select year(trans_dt)
from txn10;

drop table transaction
;
create table transaction as ( -- Creating table transaction using the 10 txn tables created eliminating duplicates using 'UNION'
    select store_location_key,
    product_key,
  collector_key,
  trans_dt,
  sales,
  units,
  trans_key
 from txn1
union
select store_location_key,
    product_key,
  collector_key,
  trans_dt,
  sales,
  units,
  trans_key
from txn2
union
select store_location_key,
    product_key,
  collector_key,
  trans_dt,
  sales,
  units,
  trans_key
from txn3
union
select store_location_key,
    product_key,
  collector_key,
  trans_dt,
  sales,
  units,
  trans_key
from txn4
union
select store_location_key,
    product_key,
  collector_key,
  trans_dt,
  sales,
  units,
  trans_key
from txn5
union
select store_location_key,
    product_key,
  collector_key,
  trans_dt,
  sales,
  units,
  trans_key
from txn6
union
select store_location_key,
    product_key,
  collector_key,
  trans_dt,
  sales,
  units,
  trans_key
from txn7
union
select store_location_key,
    product_key,
  collector_key,
  trans_dt,
  sales,
  units,
  trans_key
from txn8
union
select store_location_key,
    product_key,
  collector_key,
  trans_dt,
  sales,
  units,
  trans_key
from txn9
union
select store_location_key,
    product_key,
  collector_key,
  trans_dt,
  sales,
  units,
  trans_key
from txn10
);

select * from transaction;
select count(*) from transaction;


select count(CntDup) -- Checking for duplicates in the transaction table
from(select *
from(select *,
       row_number() over (partition by trans_key order by  ) as CntDup
        from transaction) as sub1
where CntDup > 1
order by CntDup desc) as sub2;

########################################################################################################################
#                            Section 2 - General SQL Queries
########################################################################################################################

# 1. How many sales are exactly 0?

select * -- To inspect my table
from transaction
order by DESC;

select count(*) -- Counting how many rows in my tables have sales = 0
from transaction
where sales = 0;

-- There are 17747 rows with sales = 0

# 2. How many transactions have negative sales and negative units?

select * -- To inspect my table
from transaction;

select count(*) -- Counting how many rows in my tables have negative sales AND negative units
from transaction
where sales < 0
and units < 0;

-- There are 347 transactions where sales and units are negative

# 3. How many years does the transaction table span (start and end year)?

select * -- To inspect my table
from transaction;

select count(distinct year(trans_dt)) yrs_span
from transaction;

-- The transaction table spans 2 years

# 4. Create a table to show the total sales year-over-year growth

select * -- To inspect my table
from transaction;

select sum(sales) total_sales, -- To sum up sales for the entire tables
       year(trans_dt) trans_yrs
from transaction
group by 2 -- To group the total sales by year
order by trans_yrs; -- TO make sure the data is ordered by transaction years in ascending order

-- total_sales|trans_yrs
-- 1531348.36|2015
-- 159276.67|2016

# 5. Create a table to show the total sales for each month of each year

select * -- To inspect my table
from transaction;

select  year(trans_dt) trans_yrs, -- to get the year values from the transaction date column
       month(trans_dt) trans_mths, -- -- to get the month values from the transaction date column
       sum(sales) total_sales -- To sum up sales for the entire tables
from transaction
group by 1,2 -- To group the total sales by year, then month
order by 1,2; -- To order by the year, then month

/* #|trans_yrs|trans_mths|total_sales
1|2015|3|7215.24
2|2015|4|245145.24
3|2015|5|153227.30
4|2015|6|167331.69
5|2015|7|140963.12
6|2015|8|170344.32 */

# 6. Create a **pivot table** to show the total sales for each month of each year

select * -- To inspect my table
from transaction;

select year(trans_dt) Yrs_trans, -- To create a pivot table with aggregate sales by month
    sum(case when month(trans_dt) = 1 then sales else 0 end) as Jan_sales,
    sum(case when month(trans_dt) = 2 then sales else 0 end) as Feb_sales,
    sum(case when month(trans_dt) = 3 then sales else 0 end) as Mar_sales,
    sum(case when month(trans_dt) = 4 then sales else 0 end) as April_sales,
    sum(case when month(trans_dt) = 5 then sales else 0 end) as May_sales,
    sum(case when month(trans_dt) = 6 then sales else 0 end) as June_sales,
    sum(case when month(trans_dt) = 7 then sales else 0 end) as July_sales,
    sum(case when month(trans_dt) = 8 then sales else 0 end) as Aug_sales,
    sum(case when month(trans_dt) = 9 then sales else 0 end) as Sept_sales,
    sum(case when month(trans_dt) = 10 then sales else 0 end) as Oct_sales,
    sum(case when month(trans_dt) = 11 then sales else 0 end) as Nov_sales,
    sum(case when month(trans_dt) = 12 then sales else 0 end) as Dec_sales
from transaction
group by 1; -- To group the sales by year

/* Yrs_trans|Jan_sales|Feb_sales|Mar_sales|April_sales|May_sales|June_sales|July_sales|Aug_sales|Sept_sales|Oct_sales|Nov_sales|Dec_sales
   2015|0.00|0.00|7215.24|245145.24|153227.30|167331.69|140963.12|170344.32|152907.68|166976.90|160353.01|166883.86
   2016|100656.91|7062.93|10450.95|6955.41|7700.80|4864.47|4827.61|5366.84|6572.28|4818.47|0.00|0.00 */

# 7. Which month of which year had the largest total sales?

select * -- To inspect my table
from transaction;

select  year(trans_dt) trans_yrs, -- to get the year values from the transaction date column
       month(trans_dt) trans_mths, -- -- to get the month values from the transaction date column
       sum(sales) total_sales -- To sum up sales for the entire tables
from transaction
group by 1,2 -- To group the total sales by year, then month
order by total_sales desc -- -- To order by total sales in descending order
limit 1;

/* trans_yrs|trans_months|total_sales
    2015|       4|       245145.24 */

# 8. Create a table to show total sales for each province

select * -- To inspect my table
from transaction;

select * -- to inspect the table i'll be joining with my transactions table
from location;

select distinct province -- To find the provinces present in the table
from location;

select -- To create a pivot tables showing total sales for each province
       sum(case when province = 'ontario' then sales else 0 end) as Ontario,
        sum(case when province = 'alberta' then sales else 0 end) as Alberta,
         sum(case when province = 'british columbia' then sales else 0 end) as British_Columbia,
          sum(case when province = 'Saskatchewan' then sales else 0 end) as Saskatchewan,
           sum(case when province = 'manitoba' then sales else 0 end) as Manitoba,
            sum(case when province = 'New brunswick' then sales else 0 end) as New_brunswick,
             sum(case when province = 'nova scotia' then sales else 0 end) as Nova_Scotia,
              sum(case when province = 'prince edward island' then sales else 0 end) as Prince_Edward_Island,
               sum(case when province = 'Newfoundland' then sales else 0 end) as Newfoundland,
                sum(case when province = 'northwest territory' then sales else 0 end) as Northwest_Territory,
                 sum(case when province = 'quebec' then sales else 0 end) as Quebec
from transaction t
left join location l on t.store_location_key = l.store_location; -- To join the locations table to the transactions table
                                                                    -- I use the left join because sales primarily.

/* Ontario|Alberta|British_Columbia|Saskatchewan|Manitoba|New_brunswick|Nova_Scotia|Prince_Edward_Island|Newfoundland|Northwest_Territory|Quebec
935780.56| 484575.38|   168061.45|       51680.61|50527.03| 0.00|           0.00|           0.00|             0.00|       0.00|            0.00 */

# 9. Create a table to show total sales for each product key

select * -- To inspect my table
from transaction;

select product_key,
       sum(sales) Total_sales -- To sum up the total sales in the transactions table
from transaction
group by 1 -- To group the sales by each product key
order by 1;

/* product_key|Total_sales
    283911|40.92
    30000068|5.32
    79050000|1.71
    79050001|1.71 */

# 10. Create a table to show the average sales for customers in the loyalty program vs customers that are not.
# Hints: Collector key = -1 (non-loyal) - Collector Key > -1 (loyal)

select * -- To inspect my table
from transaction;

select Loyalty_program,
       avg(sales) Avg_sales -- To calculate the average sales for the table
from (select *,
       case when collector_key = -1 then 'Non loyal' -- To create a column grading the loyal vs non-loyal customers
            when collector_key > -1 then 'Loyal' else null end as Loyalty_program
        from transaction) as sub1
        group by 1 -- To group the average sales based on loyalty
        ;
show tables;

/*Loyalty_program|Avg_sales
    Non loyal|20.735224
    Loyal|18.104968 */

# 11. Calculate the year-over-year growth in sales for each province

select * -- To inspect my table
from transaction;
select * -- to inspect the table i'll be joining with my transactions table
from location;

select province,
       year(trans_dt) Yr_trans,
       sum(sales) Total_sales
from transaction t inner join location l -- To join transactions table with location table, I used inner join as both tables are equally important
on t.store_location_key = l.store_location
group by 1,2; -- Grouping the total sales by province and year


select *, -- To get the percentage change year over year
       case when PrevSales is null then 0 else ((Yearly_sales-PrevSales)/TotalProvSales) * 100 end as Perc_chg
from (select *, -- To get the cumulative sum of Yearly_sales grouped by province
        sum(Yearly_sales) over (partition by province rows between unbounded preceding and current row) CumSumSales_by_prov,
        sum(Yearly_sales) over (partition by province) TotalProvSales, -- Yearly_sales by province
        lag(Yearly_sales) over (partition by province) PrevSales -- Previous year's sales by province
        from (select province,
                       year(trans_dt) Yr_trans,
                       sum(sales) Yearly_sales
                from transaction t inner join location l
                    on t.store_location_key = l.store_location
                    group by 1,2) as sub1)
                                        as sub2;

/* province|Yr_trans|Yearly_sales|CumSumSales_by_prov|TotalProvSales|PrevSales|Perc_chg
ALBERTA|2015|469022.81|469022.81|484575.38||0.000000
ALBERTA|2016|15552.57|484575.38|484575.38|469022.81|-93.580949
BRITISH COLUMBIA|2015|92684.52|92684.52|168061.45||0.000000
BRITISH COLUMBIA|2016|75376.93|168061.45|168061.45|92684.52|-10.298370
MANITOBA|2015|49449.79|49449.79|50527.03||0.000000

*/

# 12. Calculate the month-over-month growth in sales for each province

select * -- To inspect my table
from transaction;
select * -- to inspect the table i'll be joining with my transactions table
from location;

select province,
       month(trans_dt) mth_trans,
       sum(sales) Provincial_sales
from transaction t inner join location l -- To join transactions table with location table, I used inner join as both tables are equally important
on t.store_location_key = l.store_location
group by 1,2; -- Grouping the total sales by province and month


select *, -- To get the percentage change in sales by month
       case when PrevSales is null then 0 else ((Monthly_sales-PrevSales)/TotalProvSales) * 100 end as Perc_chg
from (select *, -- To get the cumulative sum of Monthly_sales grouped by province
            sum(Monthly_sales) over (partition by province rows between unbounded preceding and current row) CumSumSales_by_prov,
            sum(Monthly_sales) over (partition by province) TotalProvSales, -- Monthly_sales by province
            lag(Monthly_sales) over (partition by province) PrevSales -- Previous month's sales by province
            from (select province,
                        month(trans_dt) mth_trans,
                        sum(sales) Monthly_sales
                    from transaction t inner join location l
                        on t.store_location_key = l.store_location
                        group by 1,2 -- To order the table by province then the months of transaction in those months
                        order by 1,2 ) as sub1)
                                            as sub2;


/* province|mth_trans|Monthly_sales|CumSumSales_by_prov|TotalProvSales|PrevSales|Perc_chg
ALBERTA|1|15552.57|15552.57|484575.38||0.000000
ALBERTA|3|1917.14|17469.71|484575.38|15552.57|-2.813892
ALBERTA|4|104820.57|122290.28|484575.38|1917.14|21.235794
ALBERTA|5|37549.33|159839.61|484575.38|104820.57|-13.882513
ALBERTA|6|60348.35|220187.96|484575.38|37549.33|4.704948
 */

########################################################################################################################
#                            Section 3 - Business Questions
########################################################################################################################

# 1. The president of the company wants to understand which provinces and stores are performing well (above the average
# of each province), and how much are the top store in each province performing compared with the province average.
# How many stores are performing above average in each province and how many are performing below average?
# (find the average store sales of each province, sales of each store, and the difference)

select * -- To inspect my table
from transaction;
select * -- to inspect the table i'll be joining with my transactions table
from location;

select province,
       store_location,
       sum(sales) store_sales
from transaction t inner join location l -- To join transactions table with location table, I used inner join as both tables are equally important
on t.store_location_key = l.store_location
group by 1,2
order by 1,3 desc; -- Grouping the total sales by province and store sales highest to lowest

select province,
       sum(OverachieverCnt) Sum_overachiever, -- This shows the sum of stores that are overachievers for each province
       sum(UnderachieverCnt) Sum_underachiever -- This shows the sum of stores that are underachievers for each province
from (select province,
       store_location, -- Code below is to show the count of stores overachieving vs underachieving
       count(case when Performance_Fctr = 'over-performing' then 1 else 0 end) OverachieverCnt,
       count(case when Performance_Fctr = 'under-performing' then 1 else 0 end) UnderachieverCnt
        from (select *,
                    case when store_sales > Avg_sales then 'Over-performing' -- To show stores that are doing well or not in comparison to the average sales of each province
                    when store_sales < Avg_sales then 'Under-performing' end as Performance_Fctr
                from (select *,
                            avg(store_sales) over (partition by province) Avg_sales -- To show the avg sales by province
                        from (select province,
                                    store_location,
                                    sum(sales) store_sales
                                from transaction t inner join location l -- To join transactions table with location table, I used inner join as both tables are equally important
                                    on t.store_location_key = l.store_location
                                        group by 1,2
                                        order by 1,3 desc) as sub1) as sub2) as sub3
                                            group by 1,2) as sub4
                                                group by 1;



/* province|store_location|store_sales|Avg_sales|Performance_Fctr
ALBERTA|9807|217507.09|37275.029231|Over-performing
ALBERTA|7296|179697.70|37275.029231|Over-performing
ALBERTA|9802|54094.98|37275.029231|Over-performing
ALBERTA|7247|10261.62|37275.029231|Under-performing
ALBERTA|7226|9986.44|37275.029231|Under-performing

province|Sum_overachiever|Sum_underachiever
ALBERTA|13|13
BRITISH COLUMBIA|6|6
MANITOBA|3|3
ONTARIO|22|22
SASKATCHEWAN|3|3
 */

# 2. The president further wants to know how customers in the loyalty program are performing compared to non-loyalty
# customers and what category of products is contributing to most of the sales from each group.

select * -- To inspect my table
from transaction;

select *
from products;

select *
from location;

select category,
        sum(sales) CategorySales,-- To compare customer sales in the loyalty program vs not
        sum(case when Loyalty_program = 'Non loyal' then sales else 0 end) as NonLoyalCusSales,
        sum(case when Loyalty_program = 'loyal' then sales else 0 end) as LoyalCusSales
from (select category,
             sales,
            case when collector_key = -1 then 'Non loyal' -- To create a column grading the loyal vs non-loyal customers
                when collector_key > -1 then 'Loyal' else null end as Loyalty_program
        from transaction t
            inner join products p on t.product_key = p.product_key) as sub1
                group by 1 -- To group our Total sales by category
                order by 2 desc; -- To show the highest sales pre product category

/* category|CategorySales|NonLoyalCusSales|LoyalCusSales
fe148072|25800.76|16677.00|9123.76
ffcec4a7|23010.97|12400.62|10610.35
e49d14f1|17338.28|12027.72|5310.56
687ed9e3|16005.97|10060.93|5945.04
65d731c8|14596.64|8045.22|6551.42
d5a0a65d|13366.74|8909.79|4456.95
 */

# 3. Determine the top 5 stores by province and top 10 product categories by the department from these top-performing stores.

select * -- To inspect my table
from transaction;

select * -- To inspect the table I'll be joining
from products;

select *
from location;

select *,
--      Ranking departments based off sales
       dense_rank() over (partition by province, store_location_key order by Total_sales desc) Dept_rank
from (select *,
            sum(Total_sales) over (partition by department,store_location_key) StoreSales
        from (Select province,
                    store_location_key,
                    department,
                    category,
                    sum(sales) Total_sales
                from transaction t -- To join all three tables using inner join
                    inner join products p on t.product_key = p.product_key
                        inner join location l on t.store_location_key = l.store_location
                        group by 1,2,4,3  -- Total sales by province, store and dept
                            order by Total_sales desc) as sub1) as sub2
--                          To find stores with the highest sales

select *
from (select province, store_location_key, department, category, sum(sales) Total_sales,
       dense_rank() over (partition by province order by sum(sales) desc) Store_Rank,
       dense_rank() over (partition by province, store_location_key order by sum(sales) desc) Dept_rank
from transaction t
inner join products p on t.product_key = p.product_key
inner join location l on t.store_location_key = l.store_location
group by province, store_location_key, department, category) as sub1
where Store_Rank <= 5
and Dept_rank <= 10
;




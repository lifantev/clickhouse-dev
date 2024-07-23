select avg(volume) FROM
s3('https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/crypto_prices.parquet')
where crypto_name = 'Bitcoin'
limit 100;

select trim(crypto_name) as name, count() FROM
s3('https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/crypto_prices.parquet')
where name = 'zzz.finance'
group by name
order by name asc;

DESCRIBE s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/pypi/2023/pypi_0_7_34.snappy.parquet');

select count()
from s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/pypi/2023/pypi_0_7_34.snappy.parquet')
limit 10;

create table pypi(
    TIMESTAMP DateTime64(3),
    COUNTRY_CODE String,
    PROJECT String,
    URL String
)
ENGINE = MergeTree
primary key TIMESTAMP;

DESCRIBE pypi;

drop table pypi;

INSERT INTO pypi
select * from s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/pypi/2023/pypi_0_7_34.snappy.parquet');

select PROJECT, count()
from pypi
group by PROJECT
order by 2 desc
limit 100;

select PROJECT, count()
from pypi2
where toStartOfMonth(TIMESTAMP) = '2023-04-01'::Date
group by PROJECT
order by 2 desc
limit 100;

SELECT PROJECT, count() from pypi
where PROJECT like 'boto%'
group by PROJECT
order by 2 desc;

CREATE TABLE pypi2 (
    TIMESTAMP DateTime64(3),
    COUNTRY_CODE String,
    PROJECT String,
    URL String
)
ENGINE = MergeTree
PRIMARY KEY (PROJECT, TIMESTAMP);

drop table pypi2;

INSERT INTO pypi2
    SELECT *
    FROM pypi;

SELECT PROJECT, count() AS c
FROM pypi2
WHERE PROJECT like 'boto%'
GROUP BY PROJECT
ORDER BY c DESC;

select count(distinct PROJECT) from pypi;

SELECT
    table,
    formatReadableSize(sum(data_compressed_bytes)) AS compressed_size,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed_size,
    count() AS num_of_active_parts
FROM system.parts
WHERE (active = 1) AND (table like '%pypi%')
group by table;

select uniqExact(COUNTRY_CODE), uniqExact(PROJECT), uniqExact(URL) from pypi;

create table pypi3(
    TIMESTAMP DateTime64(3),
    COUNTRY_CODE LowCardinality(String),
    PROJECT LowCardinality(String),
    URL String
)
ENGINE = MergeTree
primary key (PROJECT, TIMESTAMP);

drop table pypi3;

insert into pypi3 select * from pypi;

select uniqExact(COUNTRY_CODE), uniqExact(PROJECT), uniqExact(URL) from pypi3;

create table crypto_prices (
    trade_date Date,
    crypto_name LowCardinality(String),
    volume Float32,
    price Float32,
    market_cap Float32,
    change_1_day Float32
)
ENGINE = MergeTree
primary key (crypto_name, trade_date);

insert into crypto_prices select * from s3('https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/crypto_prices.parquet');

select count() from crypto_prices;

select count() from crypto_prices where volume >= 1_000_000;

select avg(price) from crypto_prices where crypto_name like 'B%';

SELECT 
   count() AS count,
   by
FROM hackernews
GROUP BY by
ORDER BY count DESC;

desc s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/uk_property_prices.snappy.parquet');

create table uk_price_paid (
    price	UInt32,
date	Date,
postcode1	LowCardinality(String),
postcode2	LowCardinality(String),
type	Enum('terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4, 'other' = 0),
is_new	UInt8,
duration	Enum('freehold' = 1, 'leasehold' = 2, 'unknown' = 0),
addr1	String,
addr2	String,
street	LowCardinality(String),
locality	LowCardinality(String),
town	LowCardinality(String),
district	LowCardinality(String),
county	LowCardinality(String),
)
ENGINE = MergeTree
primary key (postcode1, postcode2, date);

insert into uk_price_paid select * from s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/uk_property_prices.snappy.parquet');

select count() from uk_price_paid;

select avg(price) from uk_price_paid where postcode1 = 'LU1' and postcode2 = '5FT';

select avg(price) from uk_price_paid where postcode2 = '5FT';

select avg(price) from uk_price_paid where town = 'YORK';

SET format_csv_delimiter = '~';

DESC s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/operating_budget.csv')
SETTINGS 
format_csv_delimiter='~';

SELECT sum(toUInt32OrZero(approved_amount))
FROM s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/operating_budget.csv')
SETTINGS 
format_csv_delimiter='~';

create table operating_budget (
fiscal_year	LowCardinality(String),
service	LowCardinality(String),
department	LowCardinality(String),
program	LowCardinality(String),
description	String,
item_category	LowCardinality(String),
approved_amount	UInt32,
recommended_amount	UInt32,
actual_amount	Decimal(12,2),
fund	LowCardinality(String),
fund_type	Enum('GENERAL FUNDS' = 1, 'FEDERAL FUNDS' = 2, 'OTHER FUNDS' = 3),
program_code LowCardinality(String),
)
ENGINE = MergeTree
PRIMARY KEY (fiscal_year, program);

insert into operating_budget
with splitByChar('(', c4) AS result
SELECT
    c1 AS fiscal_year,
    c2 AS service,
    c3 AS department,
    result[1] AS program,
    c5 AS description,
    c6 AS item_category,
    toUInt32OrZero(c7) AS approved_amount,
    toUInt32OrZero(c8) AS recommended_amount,
    toDecimal64(c9, 2) AS actual_amount,
    c10 AS fund,
    c11 AS fund_type,
    splitByChar(')',result[2])[1] AS program_code
FROM s3(
'https://learn-clickhouse.s3.us-east-2.amazonaws.com/operating_budget.csv', 
'CSV',
'c1 String,
c2 String,
c3 String,
c4 String,
c5 String,
c6 String,
c7 String,
c8 String,
c9 String,
c10 String,
c11 String')
SETTINGS 
format_csv_delimiter='~',
input_format_csv_skip_first_lines=1;

select * from operating_budget;

select sum(approved_amount) from operating_budget where fiscal_year = '2022';

select program from operating_budget where program_code = '031';

select sum(actual_amount) from operating_budget where fiscal_year = '2022' and program = 'AGRICULTURE & ENVIRONMENTAL SERVICES ';

select * from uk_price_paid where price > 100_000_000 order by price desc;

select count() from uk_price_paid where price > 1_000_000 and toYear(date) = '2022';

select uniqExact(town) from uk_price_paid;

select argMax(town, price) from uk_price_paid limit 1;

select topKIf(10)(town, town != 'LONDON') from uk_price_paid;

select town, formatReadableQuantity(avg(price)) as ap
from uk_price_paid
group by town
order by ap desc
limit 10;

select addr1, addr2, street, town
from uk_price_paid
order by price desc
limit 1;

select type, avg(price)
from uk_price_paid
group by type;

select sumIf(price, county IN ['AVON','ESSEX','DEVON','KENT','CORNWALL'] and toYear(date) = '2020')
from uk_price_paid;

select toYYYYMM(date) as d, avg(price)
from uk_price_paid
where toYear(date) between '2005' and '2010'
group by d
order by d desc;

select toDayOfYear(date), countIf(price, town = 'LIVERPOOL' and toYear(date) = '2020')
from uk_price_paid
group by 1
order by 1;

with (
    select max(price) 
    from uk_price_paid 
) as most_exp
select town, max(price) / most_exp
from uk_price_paid
group by town
order by 2 desc;

SELECT toYYYYMM(toDate('2024-04-15'));

create view london_properties_view as 
select date, price, addr1, addr2, street
from uk_price_paid
where town = 'LONDON';

select avg(price) from london_properties_view where toYear(date) = '2022';

select count() from london_properties_view;

SELECT count() 
FROM uk_price_paid
WHERE town = 'LONDON';

EXPLAIN SELECT count() 
FROM london_properties_view;

EXPLAIN SELECT count() 
FROM uk_price_paid
WHERE town = 'LONDON';

create or replace view properties_by_town_view as 
select date, price, addr1, addr2, street
from uk_price_paid
where town = {town_filter:String};

select formatReadableQuantity(price), street 
from properties_by_town_view(town_filter='LIVERPOOL') 
order by price desc 
limit 1;

select count(), avg(price) from uk_price_paid where toYear(date) = '2020';

select toYear(date) as d, count(), avg(price) 
from uk_price_paid 
group by d
order by d desc;

desc uk_price_paid;

create or replace table prices_by_year_dest (
price	UInt32,
date	Date,
addr1	String,
addr2	String,
street	LowCardinality(String),
town	LowCardinality(String),
district	LowCardinality(String),
county	LowCardinality(String),
)
ENGINE = MergeTree
primary key (town, date)
-- partition by toYear(date)
;

drop table prices_by_year_dest;

show create table prices_by_year_dest;

create materialized view prices_by_year_view to prices_by_year_dest
as 
    SELECT date, price, addr1, addr2, street, town, district, county
    from uk_price_paid;

drop view prices_by_year_view;

select min(toYear(date)), max(toYear(date)) from uk_price_paid;

insert into prices_by_year_dest 
select date, price, addr1, addr2, street, town, district, county 
from uk_price_paid;

select count() from prices_by_year_dest;

SELECT * FROM system.parts
WHERE table='prices_by_year_dest';

select count(), avg(price) from prices_by_year_dest where toYear(date) = '2020';

select count(), max(price), avg(price), quantile(0.90)(price) 
from prices_by_year_dest
where toYYYYMM(date) = '200506'
and county = 'STAFFORDSHIRE';

INSERT INTO uk_price_paid VALUES
    (125000, '2024-03-07', 'B77', '4JT', 'semi-detached', 0, 'freehold', 10,'',	'CRIGDON','WILNECOTE','TAMWORTH','TAMWORTH','STAFFORDSHIRE'),
    (440000000, '2024-07-29', 'WC1B', '4JB', 'other', 0, 'freehold', 'VICTORIA HOUSE', '', 'SOUTHAMPTON ROW', '','LONDON','CAMDEN', 'GREATER LONDON'),
    (2000000, '2024-01-22','BS40', '5QL', 'detached', 0, 'freehold', 'WEBBSBROOK HOUSE','', 'SILVER STREET', 'WRINGTON', 'BRISTOL', 'NORTH SOMERSET', 'NORTH SOMERSET');

SELECT * FROM system.parts
WHERE table='prices_by_year_dest';

SELECT 
    town,
    sum(price) AS sum_price,
    formatReadableQuantity(sum_price)
FROM uk_price_paid
GROUP BY town
ORDER BY sum_price DESC;

desc uk_price_paid;

create table prices_sum_dest (
    town LowCardinality(String),
    sum_price SimpleAggregateFunction(sum, UInt64)
)
engine = SummingMergeTree
primary key town;

create MATERIALIZED view prices_sum_view to prices_sum_dest
as 
    SELECT
        town,
        maxSimpleState(price) as sum_price
    from uk_price_paid
group by town;

insert into prices_sum_dest
select town, price from uk_price_paid;

select * from prices_sum_dest;

SELECT 
    town,
    sum(price) AS sum_price,
    formatReadableQuantity(sum_price)
FROM uk_price_paid
WHERE town = 'LONDON'
GROUP BY town;

SELECT
    town,
    sum(sum_price) AS sum,
    formatReadableQuantity(sum)
FROM prices_sum_dest
WHERE town = 'LONDON'
group by town;

INSERT INTO uk_price_paid (price, date, town, street)
VALUES
    (4294967295, toDate('2024-01-01'), 'LONDON', 'My Street1');

select town, sum(sum_price) as s
from prices_sum_dest
group by town
order by s desc
limit 10;

WITH
    toStartOfMonth(date) AS month
SELECT 
    month,
    min(price) AS min_price,
    max(price) AS max_price
FROM uk_price_paid
GROUP BY month 
ORDER BY month DESC;

WITH
    toStartOfMonth(date) AS month
SELECT 
    month,
    avg(price)
FROM uk_price_paid
GROUP BY month 
ORDER BY month DESC;

WITH
    toStartOfMonth(date) AS month
SELECT 
    month,
    count()
FROM uk_price_paid
GROUP BY month 
ORDER BY month DESC;

create or replace table uk_prices_aggs_dest (
    month Date,
    volume AggregateFunction(count, UInt64),
    min_price SimpleAggregateFunction(min, UInt64),
    max_price SimpleAggregateFunction(max, UInt64),
    avg_price AggregateFunction(avg, UInt32)
)
Engine AggregatingMergeTree
primary key month;

create MATERIALIZED view uk_prices_aggs_view to uk_prices_aggs_dest
as
    SELECT
        toStartOfMonth(date) as month,
        countState() as volume,
        minSimpleState(price) as min_price,
        maxSimpleState(price) as max_price,
        avgState(price) as avg_price
    from uk_price_paid
    group by month;

insert into uk_prices_aggs_dest 
SELECT
        toStartOfMonth(date) as month,
        countState() as volume,
        minSimpleState(price) as min_price,
        maxSimpleState(price) as max_price,
        avgState(price) as avg_price
    from uk_price_paid
    where month < '2024-01-01'
    group by month;

SELECT * FROM uk_prices_aggs_dest;

SELECT
    month,
    min(min_price),
    max(max_price)
from uk_prices_aggs_dest
where month > toStartOfMonth(now()) - interval 22 MONTH
group by month
order by month desc;

SELECT
    month,
    avgMerge(avg_price)
from uk_prices_aggs_dest
where month > toStartOfMonth(now()) - interval 2 YEAR
group by month
order by month desc;

SELECT
    countMerge(volume)
from uk_prices_aggs_dest
where toYear(month) = '2020';

INSERT INTO uk_price_paid (date, price, town) VALUES
    ('2024-08-01', 10000, 'Little Whinging'),
    ('2024-08-01', 1, 'Little Whinging');

SELECT 
    month,
    countMerge(volume),
    min(min_price),
    max(max_price)
FROM uk_prices_aggs_dest
WHERE toYYYYMM(month) = '202408'
GROUP BY month;

SELECT
    cluster,
    shard_num,
    replica_num,
    database_shard_name,
    database_replica_name
FROM system.clusters;

SELECT event_time, query
FROM system.query_log
ORDER BY event_time DESC
LIMIT 20;

SELECT
    event_time, query
FROM clusterAllReplicas(default, system.query_log)
ORDER BY  event_time DESC
LIMIT 20;

SELECT
    count()
FROM clusterAllReplicas(default, system.query_log)
where query ilike 'insert%';

SELECT count()
FROM system.parts;

SELECT count()
FROM clusterAllReplicas(default, system.parts);

SELECT
    instance,
    * EXCEPT instance APPLY formatReadableSize
FROM (
    SELECT
        hostname() AS instance,
        sum(primary_key_size),
        sum(primary_key_bytes_in_memory),
        sum(primary_key_bytes_in_memory_allocated)
    FROM clusterAllReplicas(default, system.parts)
    GROUP BY instance
);

SELECT 
    PROJECT,
    count()
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/pypi/2023/pypi_0_0_*.snappy.parquet')
GROUP BY PROJECT
ORDER BY 2 DESC
LIMIT 20;

SELECT
    PROJECT,
    count()
FROM s3Cluster(default, 'https://datasets-documentation.s3.eu-west-3.amazonaws.com/pypi/2023/pypi_0_0_*.snappy.parquet')
GROUP BY PROJECT
ORDER BY 2 DESC
LIMIT 20;

select * from s3('https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/mortgage_rates.csv') limit 10;

create dictionary uk_mortgage_rates (
    date DateTime64,
    variable Decimal32(2),
    fixed Decimal32(2),
    bank Decimal32(2)
)
primary key date
SOURCE (
    HTTP(
        url 'https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/mortgage_rates.csv'
        format 'CSV'
    )
)
layout(COMPLEX_KEY_HASHED())
LIFETIME(2628000000);

select * from uk_mortgage_rates;

WITH
    toStartOfMonth(uk_price_paid.date) AS month
SELECT
    month,
    count(),
    any(variable),
FROM uk_price_paid
JOIN uk_mortgage_rates
ON month = toStartOfMonth(uk_mortgage_rates.date)
GROUP BY month;

WITH
    toStartOfMonth(uk_price_paid.date) AS month
SELECT
    month,
    count(),
    any(variable),
FROM uk_price_paid
JOIN uk_mortgage_rates
ON month = toStartOfMonth(uk_mortgage_rates.date)
GROUP BY month
order by 2 desc;

select corr(toFloat32(vol), toFloat32(var)) from(
    WITH
    toStartOfMonth(uk_price_paid.date) AS month
SELECT
    month,
    count() as vol,
    any(variable) as var
FROM uk_price_paid
JOIN uk_mortgage_rates
ON month = toStartOfMonth(uk_mortgage_rates.date)
GROUP BY month
);

select corr(toFloat32(vol), toFloat32(var)) from(
    WITH
    toStartOfMonth(uk_price_paid.date) AS month
SELECT
    month,
    count() as vol,
    any(fixed) as var
FROM uk_price_paid
JOIN uk_mortgage_rates
ON month = toStartOfMonth(uk_mortgage_rates.date)
GROUP BY month
);

create table rates_monthly (
    month DateTime64,
    variable Decimal32(2),
    fixed Decimal32(2),
    bank Decimal32(2)
)
engine = ReplacingMergeTree
primary key month;

INSERT INTO rates_monthly
    SELECT 
        toDate(date) AS month,
        variable,
        fixed,
        bank
    FROM s3(
        'https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/mortgage_rates.csv',
        'CSVWithNames');

select * from rates_monthly;

SELECT * 
FROM rates_monthly 
WHERE month = '2022-05-31';

insert into rates_monthly VALUES
    ('2022-05-31', 3.2, 3.0, 1.1);

SELECT * 
FROM rates_monthly FINAL
WHERE month = '2022-05-31';

create table rates_monthly2 (
    month DateTime64,
    variable Decimal32(2),
    fixed Decimal32(2),
    bank Decimal32(2),
    version UInt32
)
engine = ReplacingMergeTree(version)
primary key month;

INSERT INTO rates_monthly2
    SELECT 
        toDate(date) AS month,
        variable,
        fixed,
        bank,
        1
    FROM s3(
        'https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/mortgage_rates.csv',
        'CSVWithNames');

INSERT INTO rates_monthly2 VALUES 
    ('2022-04-30', 3.1, 2.6, 1.1, 10);

INSERT INTO rates_monthly2 VALUES 
    ('2022-04-30', 2.9, 2.4, 0.9, 5);

SELECT * 
FROM rates_monthly2 FINAL
WHERE month = '2022-04-30';

OPTIMIZE TABLE rates_monthly2;

SELECT * 
FROM rates_monthly2
order by month desc;

create table messages (
    id UInt32,
    day Date,
    message String,
    sign Int8
)
engine = CollapsingMergeTree(sign)
primary key id;

INSERT INTO messages VALUES 
   (1, '2024-07-04', 'Hello', 1),
   (2, '2024-07-04', 'Hi', 1),
   (3, '2024-07-04', 'Bonjour', 1);

SELECT * FROM messages;

insert into messages values 
(2, '2024-07-05', 'Goodbye', -1),
(2, '2024-07-05', 'Goodbye', 1);

SELECT * FROM messages final;
SELECT * FROM messages;

INSERT INTO messages (id,sign) VALUES
    (3,-1);

INSERT INTO messages VALUES 
   (1, '2024-07-03', 'Adios', 1);

SELECT * FROM messages FINAL;

SELECT
    formatReadableSize(sum(data_uncompressed_bytes) AS u) AS uncompressed,
    formatReadableSize(sum(data_compressed_bytes) AS c) AS compressed,
    round(u / c, 2) AS compression_ratio,
    count() AS num_of_parts
FROM system.parts
WHERE table = 'uk_price_paid' AND active = 1;

SELECT
    column,
    formatReadableSize(sum(column_data_uncompressed_bytes) AS u) AS uncompressed,
    formatReadableSize(sum(column_data_compressed_bytes) AS c) AS compressed,
    round(u / c, 2) AS compression_ratio
FROM system.parts_columns
WHERE table = 'uk_price_paid' AND active = 1
GROUP BY column;

CREATE TABLE prices_1
(
    `price` UInt32,
    `date` Date,
    `postcode1` LowCardinality(String) ,
    `postcode2` LowCardinality(String),
    `type` Enum8('other' = 0, 'terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4),
    `is_new` UInt8,
    `duration` Enum8('unknown' = 0, 'freehold' = 1, 'leasehold' = 2),
    `addr1` String,
    `addr2` String,
    `street` LowCardinality(String),
    `locality` LowCardinality(String),
    `town` LowCardinality(String),
    `district` LowCardinality(String),
    `county` LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (postcode1, postcode2, date)
SETTINGS min_rows_for_wide_part=0,min_bytes_for_wide_part=0;

INSERT INTO prices_1
    SELECT * FROM uk_price_paid;

SELECT
    column,
    formatReadableSize(sum(column_data_uncompressed_bytes) AS u) AS uncompressed,
    formatReadableSize(sum(column_data_compressed_bytes) AS c) AS compressed,
    round(u / c, 2) AS compression_ratio
FROM system.parts_columns
WHERE table = 'prices_1' AND active = 1
GROUP BY column;

CREATE OR REPLACE TABLE prices_2
(
    `price` UInt32 CODEC(T64, LZ4),
    `date` Date CODEC(DoubleDelta, ZSTD),
    `postcode1` String,
    `postcode2` String,
    `is_new` UInt8 CODEC(LZ4HC)
)
ENGINE = MergeTree
ORDER BY date
SETTINGS min_rows_for_wide_part=0,min_bytes_for_wide_part=0;

insert into prices_2 select price, date, postcode1, postcode2, is_new from uk_price_paid;

select
    column,
    formatReadableSize(sum(column_data_uncompressed_bytes) AS u) AS uncompressed,
    formatReadableSize(sum(column_data_compressed_bytes) AS c) AS compressed,
    round(u / c, 2) AS compression_ratio
FROM system.parts_columns
WHERE table = 'prices_2' AND active = 1
GROUP BY column;

create table ttl_demo (
    key UInt32,
    value String,
    timestamp DateTime,

)
engine = MergeTree
primary key key
TTL timestamp + interval 60 second;

INSERT INTO ttl_demo VALUES 
    (1, 'row1', now()),
    (2, 'row2', now());

SELECT * FROM ttl_demo;

alter TABLE ttl_demo materialize ttl;

OPTIMIZE TABLE ttl_demo final;

alter table ttl_demo modify column
value String TTL timestamp + interval 5 second;

alter table prices_1 modify ttl date + interval 5 years;

ALTER TABLE prices_1
MATERIALIZE TTL;

select min(date) from prices_1;

select uniq(county) from uk_price_paid;

explain indexes=1
SELECT
    formatReadableQuantity(count()),
    avg(price)
FROM uk_price_paid
WHERE county = 'GREATER LONDON' AND date < toDate('2024-01-01');

alter table uk_price_paid add index county_index county type set(10) granularity 5;

alter table uk_price_paid materialize index county_index;

select * from system.mutations;

SELECT
    table,
    formatReadableSize(data_compressed_bytes) as data_compressed,
    formatReadableSize(secondary_indices_compressed_bytes) as index_compressed,
    formatReadableSize(primary_key_size) as primary_key
FROM
    system.parts
ORDER BY secondary_indices_uncompressed_bytes DESC
LIMIT 5;

alter table uk_price_paid drop index county_index;

alter table uk_price_paid add index county_index county type set(10) granularity 1;

alter table uk_price_paid materialize index county_index;

select * from system.mutations order by create_time desc;

SELECT 
    toYear(date) AS year,
    count(),
    avg(price),
    max(price),
    formatReadableQuantity(sum(price))
FROM uk_price_paid
WHERE town = 'LIVERPOOL'
GROUP BY year
ORDER BY year DESC;

SELECT
    formatReadableSize(sum(bytes_on_disk)),
    count() AS num_of_parts
FROM system.parts
WHERE table = 'uk_price_paid' AND active = 1;

alter table uk_price_paid
    add projection uk_price_paid_town_projection (
        select town, date, price
        order by town, date
    );

alter table uk_price_paid materialize projection uk_price_paid_town_projection;

ALTER TABLE uk_price_paid
    ADD PROJECTION handy_aggs_projection (
        SELECT
            avg(price),
            max(price),
            sum(price)
        GROUP BY town
    );

ALTER TABLE uk_price_paid
    MATERIALIZE PROJECTION handy_aggs_projection;

explain
SELECT
    avg(price),
    max(price),
    formatReadableQuantity(sum(price))
FROM uk_price_paid
WHERE town = 'LIVERPOOL';








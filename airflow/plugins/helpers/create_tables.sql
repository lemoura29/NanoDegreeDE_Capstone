CREATE TABLE IF NOT EXISTS public.dim_country (
	country_code varchar PRIMARY KEY,
	country_name varchar,
	region varchar,
	sub_region varchar
);


CREATE TABLE IF NOT EXISTS public.dim_indicator (
	code varchar PRIMARY KEY,
	name varchar,
    unit_measure varchar,
    periodicity varchar,
	aggregation_method varchar
);

CREATE TABLE IF NOT EXISTS public.fact_score_staging (
	country_code varchar NOT NULL,
	time_year integer NOT NULL,
	indicator_code varchar NOT NULL,
	value numeric(32,8)
);


CREATE TABLE IF NOT EXISTS public.fact_score (
	score_id bigint IDENTITY(1,1),
	country_code varchar NOT NULL,
    country_name varchar,
	time_year integer NOT NULL,
	indicator_code varchar NOT NULL,
    indicator_name varchar,
	value numeric(32,8),
    primary key(score_id)
);
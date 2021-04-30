# Data Engineering Capstone Project

## Project Summary

The purpose of the project is to build an ETL pipeline that extracts data from S3, stages it in Redshift, and save data into a dimensional tables for analysis and insights. It can be grouped by time period (year) or by different granularity of locality (countries, regions, sub-regions).  It be possible to separate by type of indicators (personal perceptions, development indicators, economic indicators). For example, we could try to track the value of a country's external debt over the years.


## Sources:

- [Countries with Regional Codes](https://github.com/lukes/ISO-3166-Countries-with-Regional-Codes/blob/master/all/all.json)
- [Indicators e WDI data] (https://datatopics.worldbank.org/world-development-indicators/#archives)

### Datasets

- all.json: contains informations about the countries (codes, name, region, sub-region, ...)
- Indicator: contains data of the indicators (name, description, measure, frequency ..)
- WDIData.csv: contains world development indicators per country, year and indicator. [More information](https://www.worldbank.org/en/who-we-are)

## Data Model

 A star schema with a fact table score with the dimensions location and indicators as shown in the image below:

 ![diagram](diagrama.png)


## Tools used

ETL was performed using spark, airflow was used to automate this process and Redshift was used to save the data.

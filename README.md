# Data-Ingestion-Shellscript
Practice project for automating ETL using the northwind database.


Order of shellscripts to be placed into Oozie Workflow
RawData.sh --> hiveTable.sh --> cleansing.sh --> Flatten.sh --> DenormalizedTable.sh --> Fact.sh

Practice Business Intelligence Questions I Made Up To Consider
1.Which country has the most customers? Which city has the most customers?
2.Which country has the most orders? Which city has the most orders?
3.What are the top ten products in revenue? 
4.Which product is the most expensive? How much revenue has this product generated in the 1997 year?
5.How many products are available to purchase at this time? Which products are the least purchased?
6.How many products are no longer available to puchase at this time?
7.What shipping company provides the most shipping service to us in 1997?
8.What is the longest time after ordering an item to receive it? Shortest? 
9.What are the most popular products by country?
10.What product category is most popular? 
11.What product category is most popular by country?
12.What is the average delivery time for a product?
13.Who are the employees that process most orders? 


Outline:
Sqoop import
Create hive table
Cleanse Data
Flatten Tables
Setup Star Schema
Prepare Fact Table
Dimension Tables

Oozie to schedule scripts


business model standardization
(dimensional modeling) include partitioning/bucketing
fact and dimensions
enrichment?
conversion?






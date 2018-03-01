# Data-Ingestion-Shellscript
Practice project for automating ETL.


Order of shellscripts to be placed into Oozie Workflow
RawData.sh --> Flatten.sh --> DenormalizedTable.sh --> BusinessIntelligence.sh



Practice Business Intelligence Questions I Made Up
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
Flattened Products,Orders,OrderDetails and Customers
SH1
Prepped business intelligence questions
Spark to answer them. Kept queries. ask about new hive db?
script for each answer
Oozie to schedule scripts

Some script files are make once never change. Have a config file to change parameters within the script
Script for cleansing.

When making the flattened table, there were duplicate columns within the tables. Using equi-joins solved tables with only one common column, but if a table had two duplicate columns, there was a need to specifically exclude it when joining. 
The address column value has a comma within it, causing mismatched columns when the hive table reads the file. Tried changing field delimiter for hive table, and also changed fields terminated by for sqoop import. Still has not fixed the table.

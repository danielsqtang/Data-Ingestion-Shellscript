#!/bin/bash
#Sqoop import Products, Customers, Orders tables from northwind database. 
sqoop import --connect jdbc:mysql://52.207.2.108/northwind --username temp --password temp --target-dir /user/daniel/projectscript/NorthwindProducts --m 1 --table Products
sqoop import --connect jdbc:mysql://52.207.2.108/northwind --username temp --password temp --target-dir /user/daniel/projectscript/NorthwindCustomers --m 1 --table Customers
sqoop import --connect jdbc:mysql://52.207.2.108/northwind --username temp --password temp --target-dir /user/daniel/projectscript/NorthwindOrderDetails --m 1 --table 'Order Details'
sqoop import --connect jdbc:mysql://52.207.2.108/northwind --username temp --password temp --target-dir /user/daniel/projectscript/NorthwindOrders --m 1 --table Orders
#Use variables & loop
tableNames = ['Products','Customers','Orders',`Order Details`]
for i in range(len(tableNames)): sqoop import --connect jdbc:mysql://52.207.2.108/northwind --username temp --password temp --target-dir /user/daniel/projectscript/Northwind$tableNames[i] --m 1 --table $tableNames[i]

#Create external hive table for each imported table
beeline <<EOF 
!connect jdbc:hive2://ec2-52-207-2-108.compute-1.amazonaws.com:10000/default;principal=hive/ec2-52-207-2-108.compute-1.amazonaws.com@SOLRS.NET;

USE daniel_db;
CREATE EXTERNAL TABLE IF NOT EXISTS NorthwindProducts (
ProductID int,
ProductName string,
SupplierID int,
CategoryID int,
QuantityPerUnit int,
UnitPrice int,
UnitsInStock int,
UnitsOnOrder int,
ReorderLevel int,
Discontinued int
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/daniel/projectscript/NorthwindProducts';drop table NorthwindCustomers;
CREATE EXTERNAL TABLE IF NOT EXISTS NorthwindOrderDetails (
OrderID int,
ProductID int,
UnitPrice int,
Quantity int,
Discount double
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/daniel/projectscript/NorthwindOrderDetails';

CREATE EXTERNAL TABLE IF NOT EXISTS NorthwindCustomers (
CustomerID string,
CompanyName string,
ContactName string,
ContactTitle string,
Address string,
City string,
Region string,
PostalCode int,
Country string,
Phone int,
Fax int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ("separatorChar" = ",")
STORED AS TEXTFILE
LOCATION '/user/daniel/projectscript/NorthwindCustomers';

CREATE EXTERNAL TABLE IF NOT EXISTS NorthwindOrderDetails (
OrderID int,
ProductID int,
UnitPrice int,
Quantity int,
Discount double
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/daniel/projectscript/NorthwindOrderDetails';

CREATE EXTERNAL TABLE IF NOT EXISTS NorthwindOrders (
CustomerID string,
EmployeeID int,
OrderDate string,
RequiredDate string,
ShippedDate string,
ShipVia int,
Freight float,
ShipName string,
ShipAddress string,
ShipCity string,
ShipRegion string,
ShipPostalCode int,
ShipCountry string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/daniel/projectscript/NorthwindOrders';

EOF



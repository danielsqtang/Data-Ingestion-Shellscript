#!/bin/bash
#Create external hive table for each imported table
beeline <<EOF 
!connect jdbc:hive2://ec2-52-207-2-108.compute-1.amazonaws.com:10000/default;principal=hive/ec2-52-207-2-108.compute-1.amazonaws.com@SOLRS.NET;

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
LOCATION '/user/daniel/projectscript/NorthwindProducts';

CREATE EXTERNAL TABLE IF NOT EXISTS NorthwindCustomers (
CustomerID string,
CompanyName string,
ContactName string,
ContactTitle string,
Address string,
City string,
Region string,
PostalCode string,
Country string,
Phone string,
Fax string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
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
OrderID int,
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
ShipPostalCode string,
ShipCountry string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/daniel/projectscript/NorthwindOrders';

EOF

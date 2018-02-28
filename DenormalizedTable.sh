#!/bin/bash

beeline <<EOF
!connect jdbc:hive2://ec2-52-207-2-108.compute-1.amazonaws.com:10000/default;principal=hive/ec2-52-207-2-108.compute-1.amazonaws.com@SOLRS.NET;

CREATE TABLE IF NOT EXISTS NorthwindFlattened (
ProductID int,
UnitPrice int,
CustomerID string,
OrderID int,
EmployeeID int,
OrderDate string,
RequiredDate string,
ShippedDate string,
ShipVia int,
Freight float,
ShipName string,
ShipAddress string,
ShipCity String,
ShipRegion string,
ShipPostalCode int,
ShipCountry string,
Quantity int,
Discount double,
CompanyName string,
ContactName string,
ContactTitle string,
Address string,
City string,
Region string,
PostalCode int,
Country string,
Phone int,
Fax int,
ProductName string,
SupplierID int,
CategoryID int,
QuantityPerUnit int,
UnitsInStock int,
UnitsOnOrder int,
ReorderLevel int,
Discontinued int
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
LOCATION '/user/daniel/projectscript/NorthwindFlattened'
TBLPROPERTIES ("skip.header.line.count"="1");

select * from northwindflattened limit 5;

EOF

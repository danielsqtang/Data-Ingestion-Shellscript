#!/bin/bash
#Sqoop import Products, Customers, Orders tables from northwind database. 
sqoop import --connect jdbc:mysql://52.207.2.108/northwind --username temp --password temp --target-dir /user/daniel/projectscript/NorthwindProducts --m 1 --table Products
sqoop import --connect jdbc:mysql://52.207.2.108/northwind --username temp --password temp --target-dir /user/daniel/projectscript/NorthwindCustomers --m 1 \
--query "select CustomerID, CompanyName, ContactName, ContactTitle, Replace(Address, ',', ''), City, Region, PostalCode, Country, Phone, Fax from Customers where \$CONDITIONS"
sqoop import --connect jdbc:mysql://52.207.2.108/northwind --username temp --password temp --target-dir /user/daniel/projectscript/NorthwindOrderDetails --m 1 --table 'OrderDetails'
sqoop import --connect jdbc:mysql://52.207.2.108/northwind --username temp --password temp --target-dir /user/daniel/projectscript/NorthwindOrders --m 1 --table Orders
#Use variables & loop
source config.sh
for i in range(len($TABLENAMES)): sqoop import --connect jdbc:$MYSQL/$DATABASE --username $USERNAME --password $PASSWORD --target-dir /user/daniel/projectscript/$DATABASE/$TABLENAMES[i] --m 1 --table $TABLENAMES[i]

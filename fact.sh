#!/bin/bash
source config.sh

pyspark2 <<EOF
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("NorthwindFact")
sc = SparkContext(conf=conf)
from pyspark.sql import HiveContext
hive_context = HiveContext(sc)

NorthwindFlattened = hive_context.table("NorthwindFlattened")
NorthwindFact=NorthwindFlattened.select('OrderID','ProductID','CustomerID','EmployeeID','SupplierID','CategoryID','UnitPrice','Quantity','Discount')

#NorthwindFact.write.save('/user/daniel/projectscript/NorthwindFact',format='csv',mode='overwrite')
$FACTTABLENAME.write.save('/user/daniel/projectscript/$FACTTABLENAME', format='csv', mode='overwrite')

EOF

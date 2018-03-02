#!/bin/bash
#Pyspark2 method on flattening hive tables.
source config.sh
#Initialize pyspark2
pyspark2 <<EOF

#Setup SparkContext and SparkConf
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("NorthwindDenormalizer")
sc = SparkContext(conf=conf)

#Setup HiveContext for importing Hive tables
from pyspark.sql import HiveContext
hive_context = HiveContext(sc)

${TABLENAMES[i]} = hive_context.table("$DATABASE${TABLENAMES[i]}")
#Commands to run in pyspark
Orders = hive_context.table("NorthwindOrders")
OrderDetails = hive_context.table("NorthwindOrderDetails")
Customers = hive_context.table("NorthwindCustomers")
Products = hive_context.table("NorthwindProducts")
#Join Tables
Flattened = Orders.join(OrderDetails, ["OrderID"]).join(Customers, ["CustomerID"]).join(Products,["ProductID", "UnitPrice"])

#Cleanse duplicate row data
Flattened.createOrReplaceTempView("tempFlattened") 
FlattenedFinal = sqlContext.sql("select distinct * from tempFlattened")

#save to hdfs as csv
FlattenedFinal.write.save('/user/daniel/projectscript/NorthwindFlattened', format='csv', mode='overwrite')

#Create hive table from DF (Permission error here)
#FlattenedFinal.createOrReplaceTempView("tempFlattenedFinal") 
#sqlContext.sql("create table NorthwindFlattened as select * from tempFlattenedFinal");

EOF

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
#Flattened = Orders.join(OrderDetails, ["OrderID"]).join(Customers, ["CustomerID"]).join(Products,["ProductID", "UnitPrice"])

$FLATTENEDTABLENAME = ($TABLENAME[0])\
.join($TABLENAMES[1], ["$EQUIJOINCOLUMNS[0]"])\
.join($TABLENAMES[2], ["$EQUIJOINCOLUMNS[1]"])\
.join($TABLENAMES[3], ["$EQUIJOINCOLUMNS[2], $EQUIJOINCOLUMNS[3]"])

#Cleanse duplicate row data
$FLATTENEDTABLENAME.dropDuplicates()

#Run Queries on temp table.
$FLATTENEDTABLENAME.createOrReplaceTempView("$TEMPFLATTENEDTABLENAME")
Query = sqlContext.sql("select * from $TEMPFLATTENEDTABLENAME")\

#save to hdfs as csv
$FLATTENEDTABLENAME.write.save('/user/daniel/projectscript/$FLATTENEDTABLENAME', format='csv', mode='overwrite')

#Create hive table from DF (Permission error here)
#FlattenedFinal.createOrReplaceTempView("tempFlattenedFinal") 
#sqlContext.sql("create table NorthwindFlattened as select * from tempFlattenedFinal");

EOF

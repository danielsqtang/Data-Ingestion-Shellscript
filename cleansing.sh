#!/bin/bash
source config.sh

pyspark2 <<EOF
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("Cleanser")
sc = SparkContext(conf=conf)
from pyspark.sql import HiveContext
hive_context = HiveContext(sc)

#load from hive tables
#drop rows with all null values
#drop rows with more than 5 null fields
#drop columns with all null values
#Normalize datatypes - within sqoop, Replace(Address, ',', '')
#Dropped duplicate columns by equi joining on multiple columns
#create temp table and save to hive table

for i in range(len($TABLENAMES)):\
$TABLENAMES[i] = hive_context.table("$DATABASE$TABLENAMES[i]")\
$TABLENAMES[i] = $TABLENAMES[i].dropna(how='all')\
$TABLENAMES[i] = $TABLENAMES[i].dropna(thresh=5)\
$TABLENAMES[i] = $TABLENAMES[i].dropna(axis=1, how='all')\
$TABLENAMES[i].createOrReplaceTempView("$TABLENAMES[i]")\
$TABLENAMES[i].write().mode("overwrite").saveAsTable("$DATABASE$TABLENAMES[i]")
EOF

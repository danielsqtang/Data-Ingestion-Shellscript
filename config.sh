MYSQL="mysql://52.207.2.108"
DATABASE="northwind"
USERNAME="temp"
PASSWORD="temp"
TABLENAMES=("Orders" "OrderDetails" "Customers" "Products")
EQUIJOINCOLUMNS=("OrderID" "CustomerID" "ProductID" "UnitPrice")
FLATTENEDTABLENAME="NorthwindFlattened"

//ONGOING
flatten.sh
Flattened = Orders.join(OrderDetails, ["OrderID"]).join(Customers, ["CustomerID"]).join(Products,["ProductID", "UnitPrice"])

$FLATTENEDTABLENAME = $TABLENAME[0]\
.join($TABLENAMES[1], ["$EQUIJOINCOLUMNS[0]"])\
.join($TABLENAMES[2], ["$EQUIJOINCOLUMNS[1]"])\
.join($TABLENAMES[3], ["$EQUIJOINCOLUMNS[2], $EQUIJOINCOLUMNS[3]"])


Flattened.createOrReplaceTempView("tempFlattened")
FlattenedFinal = sqlContext.sql("select distinct * from tempFlattened")

FlattenedFinal.write.save('/user/daniel/projectscript/NorthwindFlattened', format='csv', mode='overwrite')

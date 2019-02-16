// this was a practice question I solved before sitting for my CCA Spark Certification. The question is given here http://nn02.itversity.com/cca175/problem7.html
//"List the names of the Top 5 products by revenue ordered on '2013-07-26'. Revenue is considered only for COMPLETE and CLOSED orders."
//the data was given in three different tables which indicates I have to do joins later on.

// starting spark

spark-shell --master yarn

// importing the textfile data from HDFS to RDD


sqlContext.setConf("spark.sql.shuffle.partitions", "4")
 val ordersRDD= sc.textFile("/public/retail_db/orders")
 val orderItemsRDD= sc.textFile("/public/retail_db/order_items")
 val productsRDD= sc.textFile("/public/retail_db/products")




// mapping the RDD as needed to get the desired output
val ordersDF= ordersRDD.map(x=> {
	val o= x.split(",")
	( o(0).toInt, o(1).substring(0,10), o(3))
}).toDF("order_id", "order_date", "order_status")
// registering the DF so that it can be used in Spark SQL
ordersDF.registerTempTable("orders")



// filtering data where order_status= [COMPLETE,CLOSED] and order_date is 2013-07-26

val ordersDFfilter= sqlContext.sql("select * from ordersDF "+
               "where order_status in ('COMPLETE','CLOSED') and order_date like '2013-07-26%' ")


 ordersDFfilter.registerTempTable("orders")


val orderItemsDF= orderItemsRDD.map(x=> {
	val oi=x.split(",")
	(oi(1).toInt, oi(2).toInt, oi(4).toFloat)
}).toDF("order_item_order_id", "order_item_product_id", "order_item_subtotal")

orderItemsDF.registerTempTable("orderitems")


val productsDF= productsRDD.map(x=> {
	val p= x.split(",")
	(p(0).toInt, p(1).toInt, p(2))
}).toDF("product_id", "product_category_id", "product_name")

productsDF.registerTempTable("products")

                    //now joining all the DFs in SparkSQL
  val result= sqlContext.sql("select order_date,product_name,product_category_id,round(sum(order_item_subtotal),2) as order_revenue from orders o "+
                 " inner join orderitems oi ON o.order_id=oi.order_item_order_id "+
                 " inner join products p ON oi.order_item_product_id= p.product_id "+
                 " group by order_date,product_category_id,product_name  order by order_revenue desc limit 5")

// saving the output DF as Textfile with : as delimiter
result.rdd.map(x=> x.mkString(":")).coalesce(1).saveAsTextFile("/user/rizbiece/problem7/solution/")



//read the data for validation
sc.textFile(/user/rizbiece/problem7/solution/").collect.foreach(println)
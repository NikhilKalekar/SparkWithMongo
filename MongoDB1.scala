// Databricks notebook source
import com.mongodb.spark._

val restaurants = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("database", "restaurants").option("collection", "demo").load()

// COMMAND ----------

display(restaurants)

// COMMAND ----------

// Q3 ) Find out how many documents are there in the restaurants collection:

restaurants.count()

// COMMAND ----------

//  Q4 ) Display the first restaurant in the collection:

restaurants.first()

// COMMAND ----------

// Q5 ) Find all the restaurants where the cuisine is Italian :



// If you want all info for each restaurant:
val q5 = restaurants.filter($"cuisine"==="Italian")
display(q5)


// COMMAND ----------

// If you want only name, cuisine for each restaurant:
val q52 = restaurants.filter($"cuisine"==="Italian").select("name","cuisine","borough")
display(q52)

// COMMAND ----------

// 5. Find the restaurants that satisfy the following criteria:
// It should be in Manhattan burrough and the cuisine should be Italian
// also find the count:


// All info for the restaurants that are in Manhattan and have italian cuisine:
val q6 = restaurants.filter($"borough"==="Manhattan").filter($"cuisine"==="Italian")
display(q6)

// COMMAND ----------

// name, borough and cuisine for the restaurants that are in Manhattan and have italian cuisine:
val q62 = restaurants.filter($"borough"==="Manhattan").filter($"cuisine"==="Italian").select("name","borough","cuisine")
display(q62)

// COMMAND ----------

//  count:

q6.count()


// COMMAND ----------

// Q6.) Display just the names of those restaurants where the cuisine is Indian and who have received the grade A at least once.

// COMMAND ----------

val Q62 = restaurants.filter($"cuisine"==="Indian").select("name","cuisine","grades.grade")

// COMMAND ----------

import org.apache.spark.sql.functions._

 val value = udf((arr: Seq[String]) => arr.mkString(","))

 val newDf = Q62.withColumn("grade", value($"grade"))

// COMMAND ----------

newDf.show

newDf.count()

// COMMAND ----------

val Q63 = newDf.filter($"grade".contains("A"))

Q63.count()

// COMMAND ----------

Q63.show()

// COMMAND ----------


// Q7. How many different types of cuisines are there in the collection, also display their names


val q7 = restaurants.select("cuisine").distinct() 
q7.show(100,false)
q7.count()


// COMMAND ----------

// Q8 ) Find the count of restaurants grouped by borough:

val q8 = restaurants.groupBy("borough").count()

// COMMAND ----------

q8.show()

// COMMAND ----------

display(restaurants)

// COMMAND ----------

//Q9 Find the count of Chinese restaurants grouped by zipcode

val q91 = restaurants.filter($"cuisine"==="Chinese").select("cuisine","address.zipcode")


// COMMAND ----------

q91.show()

// COMMAND ----------

val q92 = q91.groupBy("zipcode").count()

// COMMAND ----------

q92.show()

// COMMAND ----------

// 10. Find the count of Pizza restaurants in Brooklyn grouped by zipcode


val q10 = restaurants.filter($"borough"==="Brooklyn").filter($"cuisine"==="Pizza").select("name","cuisine","borough","address.zipcode")

// COMMAND ----------

q10.show()

// COMMAND ----------

val result = q10.groupBy("zipcode").count()

// COMMAND ----------

display(result)

// COMMAND ----------



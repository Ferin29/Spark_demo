import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
//val spark: SparkSession = SparkSession.builder.master("local").getOrCreate
//val sqlContext = new org.apache.spark.sql.SQLContext(spark)
// Convenience function for turning JSON strings into DataFrames.
val test = spark.read.json("liste_des_prenoms_ 2004_a_2012.json")
test.show()

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext

val spark: SparkSession = SparkSession.builder.master("local").getOrCreate
val sqlContext = new org.apache.spark.sql.SQLContext(spark)
val test = spark.read.json("liste_des_prenoms_ 2004_a_2012.json").select("fields")

val nameData = sqlContext.sql("SELECT fields.nombre, fields.prenoms, fields.annee, fields.sexe FROM df")
df.select("fields").orderBy($"nombre".desc)
nameData.printSchema
val dfFields = df.select(explode(df("fields")))
val test = df.groupBy("nombre").count()
val fields = df.registerTempTable("fields")
df.show()
//df.printSchema


//df.select("fields").show()
dfFields.orderBy(desc("nombre")).show()
dfFields.show()
//df.select("fields").show()

//df.select("fields").show()

dfFields.select("fields").printSchema

import org.apache.spark.SparkContext._

val textFile = sc.textFile("stendhal.txt")

val counts = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _).map(item => item.swap).sortByKey(false,1).map(item => item.swap)

counts.saveAsTextFile("democount")

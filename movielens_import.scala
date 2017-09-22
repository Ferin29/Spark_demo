import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator, CrossValidatorModel}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
val sqlContext = new org.apache.spark.sql.SQLContext(sc); import sqlContext.implicits._
import sys.process._

//  Chargement en mémoire RDD
val cvModel = ALSModel.load("cvModel_Movielenstri")
val ratings_raw = sc.textFile("./ml-1m/ratings.dat")
ratings_raw.takeSample(false,10, seed=0).foreach(println)


// Création d'une class Ratings qui aura pour but de mapper les lignes du fichier en Ratings (DataFrame)
case class Rating(userId: Int, movieId: Int, rating: Float)
val ratings = ratings_raw.map(x => x.split("::")).map(r => Rating(r(0).toInt, r(1).toInt, r(2).toFloat)).toDF().na.drop()
println("Number of ratings = " + ratings_raw.count())

ratings.sample(false, 0.0001, seed=0).show(10)
val grouped_ratings = ratings.groupBy("userId").count().withColumnRenamed("count", "No. of ratings")
grouped_ratings.show(10)

println("Number of users = " + grouped_ratings.count())

val movies_raw = sc.textFile("./ml-1m/movies.dat")
movies_raw.takeSample(false,10, seed=0).foreach(println)

// Création d'une class Movies qui aura pour but de mapper les lignes du fichier en film (DataFrame)
case class Movies(movieId: Int, Title: String, Genre: String)
val movies = movies_raw.map(x => x.split("::")).map(m => Movies(m(0).toInt, m(1).toString, m(2).toString )).toDF()
movies.show(10, false)


// Cut 80 20 des ratings
val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2), seed=0L)

// val trainingRatio = training.count().toDouble/ratings.count().toDouble*100
// val testRatio = test.count().toDouble/ratings.count().toDouble*100
//
// println("Total number of ratings = " + ratings.count())
// println("Training dataset count = " + training.count() + ", " + BigDecimal(trainingRatio).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble + "%")
// println("Test dataset count = " + test.count() + ", " + BigDecimal(testRatio).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble+ "%")
//
// training.sample(false, 0.0001, seed=0).show(10)
// test.sample(false, 0.0001, seed=0).show(10)

// val cvModel = MatrixFactorizationModel.load(sc, "cvModel_Movielens")

// My user
val userId = 3003
val movies_watched = ratings.filter(ratings("userId") === userId)
movies_watched.show(10)
movies_watched.select(min($"rating"), max($"rating"), avg($"rating") ).show()
ratings.as('a).filter(ratings("userId") === userId).join(movies.as('b), $"a.movieId" === $"b.movieId").select("a.userId", "a.movieId", "b.Title", "b.Genre", "a.rating").sort($"a.rating".desc).show(10,false)

val movies_notwatched = ratings.filter(ratings("userId") !== userId)
movies_notwatched.sample(false, 0.0001, seed=0).show(5)
println("Count = " + movies_notwatched.count())

val movies_notwatched_movieId = ratings.filter(ratings("userId") === userId).as('a).join(movies.as('b), $"a.movieId" === $"b.movieId", "right").filter($"a.movieId".isNull).select($"b.movieId", $"b.Title").sort($"b.movieId".asc)
movies_notwatched_movieId.show(10, false)


println("Total number of movies = " + movies.count())
println("Number of movies rated by user = " + ratings.filter(ratings("userId") === userId).count())
println("Number of movies NOT rated by user = " + movies_notwatched_movieId.count())

val data_userId = movies_notwatched_movieId.withColumn("userId", lit(userId))
data_userId.show(10, false)

val predictions_userId = cvModel.transform(data_userId).na.drop()
val top10 = predictions_userId.select("Title", "prediction").sort($"prediction".desc).show(10, false)

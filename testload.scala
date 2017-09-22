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

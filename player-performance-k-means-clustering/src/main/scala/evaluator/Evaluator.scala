package evaluator

import org.apache.commons.math3.ml.clustering.evaluation.ClusterEvaluator
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.sql.DataFrame

object Evaluator {
  def silhouette(predictions: DataFrame): Double = {
    val evaluator = new ClusteringEvaluator()
    val silhouette = evaluator.evaluate(predictions)
    silhouette
  }
}

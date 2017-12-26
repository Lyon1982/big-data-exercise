package com.github.lyon1982.BigDataExercise

import ch.cern.sparkmeasure.StageMetrics
import com.github.lyon1982.BigDataExercise.jobs.BigDataExerciseJob
import org.apache.spark.sql.SparkSession

object BigDataExerciseAppWithMeasure {

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder.appName("Big Data Exercise Application Measure").getOrCreate()

    val stageMetrics = StageMetrics(spark)
    stageMetrics.begin()

    new BigDataExerciseJob().run()

    stageMetrics.end()
    stageMetrics.printReport()
    stageMetrics.printAccumulables

    spark.stop()
  }
}

package com.github.lyon1982.BigDataExercise

import ch.cern.sparkmeasure.StageMetrics
import com.github.lyon1982.BigDataExercise.jobs.BigDataExerciseJob
import org.apache.spark.sql.SparkSession

object BigDataExerciseAppWithMeasure {

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder.appName("Big Data Exercise Application").getOrCreate()

    val stageMetrics = StageMetrics(spark)
    stageMetrics.begin()

    val (dataset1, dataset2, dataset3) = new BigDataExerciseJob().run()

    BigDataExerciseApp.writeDataframe(dataset1, "/vagrant_app/data/ds1")
    BigDataExerciseApp.writeDataframe(dataset2, "/vagrant_app/data/ds2")
    BigDataExerciseApp.writeDataframe(dataset3, "/vagrant_app/data/ds3")

    stageMetrics.end()
    stageMetrics.printReport()
    stageMetrics.printAccumulables

    spark.stop()
  }
}

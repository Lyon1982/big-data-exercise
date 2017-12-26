package com.github.lyon1982.BigDataExercise

import com.github.lyon1982.BigDataExercise.jobs.BigDataExerciseJob
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

object BigDataExerciseApp {

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder.appName("Big Data Exercise Application").getOrCreate()

    val (dataset1, dataset2, dataset3) = new BigDataExerciseJob().run()

    writeDataframe(dataset1, "/vagrant_app/data/ds1")
    writeDataframe(dataset2, "/vagrant_app/data/ds2")
    writeDataframe(dataset3, "/vagrant_app/data/ds3")

    spark.stop()
  }

  def writeDataframe(ds: Dataset[Row], path: String)(implicit spark: SparkSession): Unit = {
    ds
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv(path)
  }

}

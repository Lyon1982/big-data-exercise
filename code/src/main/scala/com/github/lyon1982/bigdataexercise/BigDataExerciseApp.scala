package com.github.lyon1982.BigDataExercise

import com.github.lyon1982.BigDataExercise.jobs.BigDataExerciseJob
import org.apache.spark.sql.SparkSession

object BigDataExerciseApp {

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder.appName("Big Data Exercise Application").getOrCreate()

    new BigDataExerciseJob().run()

    spark.stop()
  }

}

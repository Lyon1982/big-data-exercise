package com.github.lyon1982.BigDataExercise.builder.columngen

import org.apache.spark.sql.{Dataset, Row}

/***
  * Add a calculated column to dataframe.
  */
trait ColumnGenerator {

  /**
    * Add a calculated column to dataframe.
    *
    * @param df original dataframe
    * @return dataframe with new added column
    */
  def generate(df: Dataset[Row]): Dataset[Row]

}

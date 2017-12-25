package com.github.lyon1982.BigDataExercise.builder.reader

import org.apache.spark.sql.{Dataset, Row}

/**
  * Read a specified resource as dataframe.
  */
trait DataFrameReader {

  /**
    * Read a specified resource as dataframe.
    *
    * @return dataframe
    */
  def read(): Dataset[Row]

}

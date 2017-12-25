package com.github.lyon1982.BigDataExercise.builder.reader

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  * Read csv file as dataframe.
  *
  * @param path path to the csv file
  * @param schema schema
  * @param header if the csv file has a header
  * @param spark spark session
  */
class CSVDataFrameReader(path: String, schema: StructType, header: Boolean = true)(implicit spark: SparkSession) extends DataFrameReader {

  /**
    * Read csv file as dataframe.
    *
    * @return dataframe
    */
  override def read(): Dataset[Row] = {
    spark.read.option("header", header).schema(schema).csv(path)
  }

}

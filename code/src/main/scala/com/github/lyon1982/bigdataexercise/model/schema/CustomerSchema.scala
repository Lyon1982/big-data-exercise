package com.github.lyon1982.BigDataExercise.model.schema

import org.apache.spark.sql.types.{StringType, StructField, StructType}

object CustomerSchema {

  def schema = StructType(StructField("ClientId", StringType)
    :: StructField("FirstName", StringType)
    :: StructField("LastName", StringType)
    :: StructField("Address", StringType)
    :: StructField("City", StringType)
    :: StructField("County", StringType)
    :: StructField("State", StringType)
    :: StructField("Zip", StringType)
    :: StructField("BirthDate", StringType)
    :: StructField("Gender", StringType)
    :: Nil)

}

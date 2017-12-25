package com.github.lyon1982.BigDataExercise.model.schema

import org.apache.spark.sql.types.{StringType, StructField, StructType}

object HotelSchema {

  def schema = StructType(StructField("HotelId", StringType)
    :: StructField("HotelName", StringType)
    :: StructField("City", StringType)
    :: StructField("Country", StringType)
    :: StructField("StarRating", StringType)
    :: StructField("Latitude", StringType)
    :: StructField("Longitude", StringType)
    :: StructField("RoomCount", StringType)
    :: StructField("RoomType", StringType)
    :: Nil)

}

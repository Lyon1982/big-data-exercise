package com.github.lyon1982.BigDataExercise.model.schema

import org.apache.spark.sql.types._

object HotelBookingSchema {

  def schema = StructType(StructField("BookingId", StringType)
    :: StructField("HotelId", StringType)
    :: StructField("BookingDate", StringType)
    :: StructField("ClientId", StringType)
    :: StructField("StayDate", StringType)
    :: StructField("Occupancy", IntegerType)
    :: StructField("StayDuration", IntegerType)
    :: StructField("RoomRateUSD", DoubleType)
    :: Nil)

}

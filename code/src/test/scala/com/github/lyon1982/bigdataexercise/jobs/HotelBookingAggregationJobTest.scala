package com.github.lyon1982.BigDataExercise.jobs

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

class HotelBookingAggregationJobTest extends FunSuite with DataFrameSuiteBase {

  test("Give hotel and booking dataframe, it should return the aggregation summaries.") {
    val hotels = spark.createDataFrame(
      sc.parallelize(
        Seq(Row("482571", "ooty", "India"), Row("482572", "beijing", "China")))
      , StructType(StructField("HotelId", StringType, true) :: StructField("City", StringType, true) :: StructField("Country", StringType, true) :: Nil))

    val bookings = spark.createDataFrame(
      sc.parallelize(
        Seq(
          Row("482571", 3, 10),
          Row("482571", 7, 20),
          Row("482572", 2, 5)))
      , StructType(StructField("HotelId", StringType, true) :: StructField("StayDuration", IntegerType, true) :: StructField("BookingStayInterval", IntegerType, true) :: Nil))

    val (countryGroupedSummary, cityGroupedSummary) = new HotelBookingAggregationJob().run(bookings, hotels)(spark)

    val expectedCountryGroupedSummary = spark.createDataFrame(
      sc.parallelize(Seq(Row("China", 1L, 5L, 2L), Row("India", 2L, 30L, 10L)))
      , StructType(StructField("HotelCountry", StringType, true) :: StructField("Count", LongType, true) :: StructField("BookingStayInterval", LongType, true) :: StructField("StayDuration", LongType, true) :: Nil))

    val expectedCityGroupedSummary = spark.createDataFrame(
      sc.parallelize(Seq(Row("beijing", 1L, 5L, 2L), Row("ooty", 2L, 30L, 10L)))
      , StructType(StructField("HotelCity", StringType, true) :: StructField("Count", LongType, true) :: StructField("BookingStayInterval", LongType, true) :: StructField("StayDuration", LongType, true) :: Nil))

    assertDataFrameEquals(expectedCountryGroupedSummary, countryGroupedSummary)
    assertDataFrameEquals(expectedCityGroupedSummary, cityGroupedSummary)
  }

}

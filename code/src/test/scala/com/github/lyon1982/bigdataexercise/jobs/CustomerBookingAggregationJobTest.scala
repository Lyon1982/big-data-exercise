package com.github.lyon1982.BigDataExercise.jobs

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

class CustomerBookingAggregationJobTest extends FunSuite with DataFrameSuiteBase {

  test("Give customer and booking dataframe, it should return the aggregation summaries.") {
    val customers = spark.createDataFrame(
      sc.parallelize(
        Seq(Row("1", "M", 30), Row("2", "F", 40)))
      , StructType(StructField("ClientId", StringType, true) :: StructField("Gender", StringType, true) :: StructField("Age", IntegerType, true) :: Nil))

    val bookings = spark.createDataFrame(
      sc.parallelize(
        Seq(
          Row("1", 3, 10),
          Row("1", 7, 20),
          Row("2", 2, 5)))
      , StructType(StructField("ClientId", StringType, true) :: StructField("StayDuration", IntegerType, true) :: StructField("BookingStayInterval", IntegerType, true) :: Nil))

    val (genderGroupedSummary, ageGroupedSummary) = new CustomerBookingAggregationJob().run(bookings, customers)(spark)

    val expectedGenderGroupedSummary = spark.createDataFrame(
      sc.parallelize(Seq(Row("F", 1L, 5L, 2L), Row("M", 2L, 30L, 10L)))
      , StructType(StructField("CustomerGender", StringType, true) :: StructField("Count", LongType, true) :: StructField("BookingStayInterval", LongType, true) :: StructField("StayDuration", LongType, true) :: Nil))

    val expectedAgeGroupedSummary = spark.createDataFrame(
      sc.parallelize(Seq(Row(40, 1L, 5L, 2L), Row(30, 2L, 30L, 10L)))
      , StructType(StructField("CustomerAge", IntegerType, true) :: StructField("Count", LongType, true) :: StructField("BookingStayInterval", LongType, true) :: StructField("StayDuration", LongType, true) :: Nil))

    assertDataFrameEquals(expectedGenderGroupedSummary, genderGroupedSummary)
    assertDataFrameEquals(expectedAgeGroupedSummary, ageGroupedSummary)
  }

}

package com.github.lyon1982.BigDataExercise.builder.columngen

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.FunSuite

class BookingStayIntervalColumnGeneratorTest extends FunSuite with DataFrameSuiteBase {

  test("Given a dataframe with column BookingDate and StayDate in right pattern, it should add a new column BookingStayInterval and calculate the value correctly") {
    val df = spark.createDataFrame(sc.parallelize(Seq(Row("12.20.16", "12.25.16"), Row("12.10.16", "12.20.16"))), StructType(StructField("BookingDate", StringType, true) :: StructField("StayDate", StringType, true) :: Nil))
    val dfWithAge = new BookingStayIntervalColumnGenerator().generate(df)

    val expected = spark.createDataFrame(sc.parallelize(Seq(Row("12.20.16", "12.25.16", 5), Row("12.10.16", "12.20.16", 10))), StructType(StructField("BookingDate", StringType, true) :: StructField("StayDate", StringType, true) :: StructField("BookingStayInterval", IntegerType, true) :: Nil))
    assertDataFrameEquals(expected, dfWithAge)
  }

  test("Given a dataframe with column BookingDate and StayDate in right pattern, it should replace those with a new column BookingStayInterval and calculate the value correctly") {
    val df = spark.createDataFrame(sc.parallelize(Seq(Row("12.20.16", "12.25.16"), Row("12.10.16", "12.20.16"))), StructType(StructField("BookingDate", StringType, true) :: StructField("StayDate", StringType, true) :: Nil))
    val dfWithAge = new BookingStayIntervalColumnGenerator(replace = true).generate(df)

    val expected = spark.createDataFrame(sc.parallelize(Seq(Row(5), Row(10))), StructType(StructField("BookingStayInterval", IntegerType, true) :: Nil))
    assertDataFrameEquals(expected, dfWithAge)
  }

}

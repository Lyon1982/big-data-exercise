package com.github.lyon1982.BigDataExercise.builder.columngen

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.Row
import org.scalatest.FunSuite

class CustomerAgeColumnGeneratorTest extends FunSuite with DataFrameSuiteBase {

  def yearsAgo(n: Int): String = LocalDate.now().minusYears(n).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))

  test("Given a dataframe with a column BirthDate in right pattern, it should add a new column Age and calculate the value correctly") {
    val df = spark.createDataFrame(sc.parallelize(Seq(Row(yearsAgo(5)), Row(yearsAgo(10)))), StructType(StructField("BirthDate", StringType, true) :: Nil))
    val dfWithAge = new CustomerAgeColumnGenerator().generate(df)

    val expected = spark.createDataFrame(sc.parallelize(Seq(Row(yearsAgo(5), 5), Row(yearsAgo(10), 10))), StructType(StructField("BirthDate", StringType, true) :: StructField("Age", IntegerType, true) :: Nil))
    assertDataFrameEquals(expected, dfWithAge)
  }

  test("Given a dataframe with a column BirthDate in right pattern, it should replace those with a new column Age and calculate the value correctly") {
    val df = spark.createDataFrame(sc.parallelize(Seq(Row(yearsAgo(5)), Row(yearsAgo(10)))), StructType(StructField("BirthDate", StringType, true) :: Nil))
    val dfWithAge = new CustomerAgeColumnGenerator(replace = true).generate(df)

    val expected = spark.createDataFrame(sc.parallelize(Seq(Row(5), Row(10))), StructType(StructField("Age", IntegerType, true) :: Nil))
    assertDataFrameEquals(expected, dfWithAge)
  }

}

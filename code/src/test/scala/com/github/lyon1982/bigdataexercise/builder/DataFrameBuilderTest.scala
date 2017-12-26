package com.github.lyon1982.BigDataExercise.builder

import com.github.lyon1982.BigDataExercise.builder.columngen.ColumnGenerator
import com.github.lyon1982.BigDataExercise.builder.reader.DataFrameReader
import com.github.lyon1982.BigDataExercise.builder.validator.Validator
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.FunSuite

class DataFrameBuilderTest extends FunSuite with DataFrameSuiteBase {

  test("Given a couple needed columns, a validator and a column generator, it should return a dataframe with valid data and those columns that needed.") {
    import spark.implicits._

    val df = new DataFrameBuilder()
      .addNeededColumns("Col1", "Col3")
      .setDataValidator(new Validator {
        implicit val columnValidators: Map[String, ValidateFunction] = Map(
          "Col1" -> validateIntGreaterThan(0)
          , "Col2" -> validateNonEmptyString
          , "Col3" -> validateIntGreaterThan(0))

        override def validate(row: Row): Boolean = validateColumns(row)
      })
      .appendCustomColumn(new ColumnGenerator {
        override def generate(df: Dataset[Row]): Dataset[Row] = {
          df.withColumn("GenCol", df("Col1") + df("Col3"))
        }
      })
      .create(new DataFrameReader {
        override def read(): Dataset[Row] = {
          spark.createDataFrame(
            sc.parallelize(
              Seq(Row(0, "somestr", 1), Row(1, "somestr", 1)))
            , StructType(StructField("Col1", IntegerType, true) :: StructField("Col2", StringType, true) :: StructField("Col3", IntegerType, true) :: Nil))
        }
      })

    val expected = spark.createDataFrame(
      sc.parallelize(Seq(Row(1, 1, 2)))
      , StructType(StructField("Col1", IntegerType, true) :: StructField("Col3", IntegerType, true) :: StructField("GenCol", IntegerType, true) :: Nil))

    assertDataFrameEquals(expected, df)
  }

}

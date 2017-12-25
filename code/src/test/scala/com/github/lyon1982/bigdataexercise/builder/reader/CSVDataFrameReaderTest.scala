package com.github.lyon1982.BigDataExercise.builder.reader

import java.io.File
import java.nio.file.{Files, Paths, StandardOpenOption}

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.FunSuite

class CSVDataFrameReaderTest extends FunSuite with DataFrameSuiteBase {

  test("Given a valid csv file with header, it should parse it to a dataframe.") {
    val tempFile = File.createTempFile("BigDataExercise", "csv")
    Files.write(Paths.get(tempFile.getAbsolutePath), "H1,H2\nstr,5\n".getBytes(), StandardOpenOption.CREATE)

    val schema = StructType(StructField("H1", StringType) :: StructField("H2", IntegerType) :: Nil)
    val df = new CSVDataFrameReader(path = tempFile.getAbsolutePath, schema = schema, header = true)(spark).read()

    assertDataFrameEquals(spark.createDataFrame(sc.parallelize(Seq(Row("str", 5))), schema), df)

    tempFile.delete()
  }

}

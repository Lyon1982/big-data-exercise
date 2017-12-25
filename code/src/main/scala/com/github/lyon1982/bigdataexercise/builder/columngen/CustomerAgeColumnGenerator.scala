package com.github.lyon1982.BigDataExercise.builder.columngen

import com.github.lyon1982.BigDataExercise.utils.DateTimeTools
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions.udf

/**
  * Add the age column and calculate the value.
  *
  * @param birthDateColumnName birth date column name
  * @param ageColumnName       age column name
  * @param replace             whether replace the original column
  */
class CustomerAgeColumnGenerator(val birthDateColumnName: String = "BirthDate", val ageColumnName: String = "Age", replace: Boolean = false) extends ColumnGenerator {

  // Age calculation function
  private val calculateAge = udf((birthDate: String) => {
    DateTimeTools.calculateYearsFromNow(birthDate)
  })


  /**
    * Add age column.
    *
    * @param df dataframe with birth date column
    * @return dataframe with age column added
    */
  def generate(df: Dataset[Row]): Dataset[Row] = {
    val withNewColumn = df.withColumn(ageColumnName, calculateAge(df(birthDateColumnName)))
    if (replace) withNewColumn.drop(birthDateColumnName) else withNewColumn
  }

}

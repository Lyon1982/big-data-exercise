package com.github.lyon1982.BigDataExercise.builder

import com.github.lyon1982.BigDataExercise.builder.validator.Validator
import com.github.lyon1982.BigDataExercise.builder.columngen.ColumnGenerator
import com.github.lyon1982.BigDataExercise.builder.reader.DataFrameReader
import org.apache.spark.sql.{Dataset, Row}

/**
  * Create a dataframe with columns that just needed and extra calculated column, also apply validation.
  */
class DataFrameBuilder {

  private var neededColumns: Seq[String] = Seq()

  private var validator: Option[Validator] = Option.empty

  private var columnGenerators: Seq[ColumnGenerator] = Seq()

  /**
    * Add needed columns. If you want to use all columns then you shouldn't call this method.
    *
    * @param columns needed columns
    * @return this builder
    */
  def addNeededColumns(columns: String*): DataFrameBuilder = {
    if (columns.isEmpty) throw new IllegalArgumentException("Columns can not be empty!")
    this.neededColumns = this.neededColumns ++ columns
    this
  }

  /**
    * Set the validator to validate the data.
    *
    * @param validator data validator
    * @return this builder
    */
  def setDataValidator(validator: Validator): DataFrameBuilder = {
    Option(validator) match {
      case Some(v) => this.validator = Option(v)
      case None => throw new IllegalArgumentException("Validator can not be empty!")
    }
    this
  }

  /**
    * Add customer column.
    *
    * @param columnGenerator column generator
    * @return this builder
    */
  def appendCustomColumn(columnGenerator: ColumnGenerator): DataFrameBuilder = {
    Option(columnGenerator) match {
      case Some(v) => this.columnGenerators = this.columnGenerators :+ v
      case None => throw new IllegalArgumentException("Column generator can not be empty!")
    }
    this
  }

  /**
    * Build a dataframe with this particular setup.
    *
    * @param reader dataframe reader
    * @return datafram
    */
  def create(reader: DataFrameReader): Dataset[Row] = {
    var df = reader.read()

    // Only use the columns that are needed if specified
    if (neededColumns.isEmpty) {
      neededColumns = neededColumns ++ df.columns
    } else {
      df.columns.diff(this.neededColumns).foreach(col => df = df.drop(col))
    }

    // Validate
    // TODO: use coalesce if too many(> 30%) invalid records
    validator.foreach(v => {
      v.setColumns(neededColumns)
      df = df.filter(v.validate(_))
    })

    // Append & calculate new columns
    columnGenerators.foreach(gen => df = gen.generate(df))

    df
  }

}

package insight

import org.apache.spark.sql.{Column, DataFrame}

object DataFrameHelper {
  implicit class RichDataFrame(df: DataFrame) {
    // utilizing foldLeft to conduct `.withColumn` on single DataFrame multiple times
    def withColumns(columns: (String, Column)*): DataFrame = {
      columns.foldLeft(df) { case (dataframe, (field, column)) =>
        dataframe.withColumn(field, column)
      }
    }
  }
}

package ru.raiffeisen.etl

import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import ru.raiffeisen.etl.config.Config
import ru.raiffeisen.etl.hadoop.HiveAccess

/**
  * spark_tmptoparquet
  *
  */
object Spark_TmpToParquet {
  def main(args: Array[String]): Unit = {

    println(args)

    val configParams = new Config(args)

    val sparkContext: SparkContext = configParams.sparkContextLocal
    val sqlContext = new HiveContext(sparkContext)

    sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    try {
      try {
        new HiveAccess().createTmpTableWithString(configParams, sqlContext) // Create tmp table if not exist
        new HiveAccess().insertIntoTableByCreated(configParams, sqlContext)
      }
      finally {
        new HiveAccess().dropTmpTable(configParams, sqlContext) // Drop table if exist
      }
    } finally {
      sparkContext.stop()
    }

  }
}

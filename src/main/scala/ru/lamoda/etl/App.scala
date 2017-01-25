package ru.lamoda.etl

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import ru.lamoda.etl.config.Config
import ru.lamoda.etl.hadoop.HiveAccess

/**
  * Hello world!
  *
  */
object App {
  def main(args: Array[String]): Unit = {

    val configParams = new Config(args)

    val sparkConf = new SparkConf()
      .setAppName("dwh_app")
      .setMaster("yarn-client")
      .set("spark.driver.allowMultipleContexts", "true")
    val spcontext = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(spcontext)

    sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    try {
      try {
        new HiveAccess().createTmpTableWithString(configParams, sqlContext) // Create tmp table if not exist
        new HiveAccess().insertIntoTableByCreated(configParams, sqlContext)
      } finally {
        new HiveAccess().dropTmpTable(configParams, sqlContext) // Drop table if exist
      }
    } catch {
      case e: IllegalArgumentException => System.exit(1)
    } finally {
      spcontext.stop()
    }

  }
}

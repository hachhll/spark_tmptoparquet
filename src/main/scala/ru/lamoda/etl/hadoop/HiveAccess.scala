package ru.lamoda.etl.hadoop

import org.apache.spark.sql.hive.HiveContext
import ru.lamoda.etl.config.Config

/**
  * Created by gevorg.hachaturyan on 19/01/2017.
  */
class HiveAccess {

  def dropTable(configParams: Config,  sqlContext: HiveContext): Unit = {

    sqlContext.sql("DROP TABLE "
        + " IF EXISTS "
        + configParams.tableName
      )
  }

  def dropTmpTable(configParams: Config,  sqlContext: HiveContext): Unit = {

    //val hiveContext =  configParams.getHiveContext

    sqlContext.sql("DROP TABLE "
        + " IF EXISTS "
        + configParams.tableName + configParams.inc_id
      )
  }

  def createTmpTable(configParams: Config,  sqlContext: HiveContext): Unit = {

    sqlContext.sql("CREATE TABLE IF NOT EXISTS "
        + configParams.tableName
        + " ( "
        + configParams.filedListWithType
        + " ) "
        + " ROW FORMAT DELIMITED FIELDS TERMINATED BY '"
        + configParams.fieldDelim
        + "'"
        + "LINES TERMINATED BY '\n'"
      )
  }

  def createTmpTableWithString(configParams: Config,  sqlContext: HiveContext): Unit = {

    sqlContext.sql("CREATE TABLE IF NOT EXISTS "
        + configParams.tableName + configParams.inc_id
        + " ( "
        + configParams.filedList.replace("\"", "").split(",").mkString(" String,") + " String"
        + " ) "
        + " ROW FORMAT DELIMITED FIELDS TERMINATED BY '"
        + configParams.fieldDelim
        + "'"
        + "LINES TERMINATED BY '\n'"
      )
  }


  def insertIntoTableByCreated(configParams: Config,  sqlContext: HiveContext): Unit = {

    sqlContext.sql("INSERT INTO TABLE "
        + configParams.tableName
        + " PARTITION (created_part, inc_id_part) SELECT "
        + configParams.filedList
        + ", " + " from_unixtime(unix_timestamp(created),'yyyyMMdd') "
        + ", " + configParams.inc_id
        + " FROM "
        + configParams.tableName + configParams.inc_id
      )
  }
}

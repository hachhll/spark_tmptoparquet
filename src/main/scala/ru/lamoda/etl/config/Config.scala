package ru.lamoda.etl.config

import java.io.InputStream

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by gevorg.hachaturyan on 23/01/2017.
  */
class Config(args: Array[String]) {

  val hdpConfigFiles: List[String] = getValueProperties("hadoopConfigFile", "/default.properties")
  val sourceFolder = getValueProperty("localDefaultField", "/default.properties")
  val targetFolder = getValueProperty("hdfsHiveDefaultField", "/default.properties")
  val filedList = getCommandLineParam(args, "=", "filedList")
  val fieldDelim = getCommandLineParam(args, "=", "fieldDelim")
  val inc_id = getCommandLineParam(args, "=", "inc_id")
  val tableName = getCommandLineParam(args, "=", "tableName")
  val aliasName = getCommandLineParam(args, "=", "aliasName") // Not need to check in try - catch
  val filedListWithType = ""

  def getValueProperties(valName: String, fileName: String): List[String] = {
    val stream: InputStream = getClass.getResourceAsStream(fileName)
    Source.fromInputStream(stream)
      .getLines()
      .find(_.startsWith(valName + "="))
      .map(_.replace(valName + "=", "")).toList
  }

  def getValueProperties(valName: String, fileName: String, fileDelim: String): List[String] = {
    val stream: InputStream = getClass.getResourceAsStream(fileName)
    Source.fromInputStream(stream)
      .getLines()
      .find(_.startsWith(valName + fileDelim))
      .map(_.replace(valName + fileDelim, "")).toList
  }

  def getValueProperty(valName: String, fileName: String): String = {
    val stream: InputStream = getClass.getResourceAsStream(fileName)
    val mapValue = Source.fromInputStream(stream)
      .getLines()
      .find(_.startsWith(valName + "="))
      .map {
        _.split("=")
      }.map { case Array(f1, f2) => (f1, f2) }.toMap
    mapValue(valName)
  }

  def getMapValuesByDelim(argsArray: Array[String], delim: String): Map[String, String] = {
    argsArray.map {
      _.split(delim)
    }.map { case Array(f1, f2) => (f1, f2) }.toMap
  }

  def getCommandLineParam(argsArray: Array[String], delim: String, paramName: String): String = {
    val ms = getMapValuesByDelim(argsArray, delim)
    val resValue = ms(paramName)
    if (resValue.equals("")) {
      val resValue = getValueProperty(ms(paramName), "/default.properties")
      if (resValue.equals(""))
        new IllegalArgumentException("Value for parameter: " + paramName + "is not set")
    }
    resValue
  }

  def getHiveContext: HiveContext = {
    val sparkConf = new SparkConf()
      .setAppName("dwh_app")
      .setMaster("local[*]")
      .set("spark.driver.allowMultipleContexts", "true")
    val spcontext = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(spcontext)

    sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    sqlContext
  }

  def getHiveContext(masterMode: String): HiveContext = {
    val sparkConf = new SparkConf()
      .setAppName("dwh_app")
      .setMaster(masterMode)
      .set("spark.driver.allowMultipleContexts", "true")
    val spcontext = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(spcontext)


    sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    sqlContext

  }

  def getHadoopConfig(configFiles: List[String]): FileSystem = {
    val hadoopConf = new Configuration()
    for (fileConfig: String <- configFiles) {
      hadoopConf.addResource(new Path(fileConfig))
    }
    val hdfs = FileSystem.get(hadoopConf)
    hdfs
  }
}




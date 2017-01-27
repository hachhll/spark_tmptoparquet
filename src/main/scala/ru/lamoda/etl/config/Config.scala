package ru.lamoda.etl.config

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by gevorg.hachaturyan on 23/01/2017.
  */
class Config(args: Array[String]) {

  val sparkContextLocal = getSparkContext

  def getSparkContext: SparkContext = {
    val sparkConf = new SparkConf()
    val spcontext = new SparkContext(sparkConf)
    spcontext.getConf.get("spark.home")
    spcontext.getConf.get("spark.master")
    spcontext.getConf.get("spark.java.home")
    spcontext.getConf.get("spark.driver.allowMultipleContexts")
    try {
      spcontext.getConf.get("spark.driver.memory")
      spcontext.getConf.get("spark.akka.frameSize")
      spcontext.getConf.get("spark.executor.memory")
      spcontext.getConf.get("spark.executor.instances")
      spcontext.getConf.get("spark.executor.cores")
      spcontext.getConf.get("spark.default.parallelism")
    } catch {
      case e: java.util.NoSuchElementException => println (e.getMessage)
    }
    spcontext
  }

  val tableName = getCommandLineParam(args, "=", "tableName")
  val inc_id = getCommandLineParam(args, "=", "inc_id")
  val filedList = getCommandLineParam(args, "=", "filedList")
  val fieldDelim = getCommandLineParam(args, "=", "fieldDelim")

  def getMapValuesByDelim(argsArray: Array[String], delim: String): Map[String, String] = {
    argsArray.map {
      _.split(delim)
    }.map { case Array(f1, f2) => (f1, f2) }.toMap
  }

  def getCommandLineParam(argsArray: Array[String], delim: String, paramName: String): String = {
    val ms = getMapValuesByDelim(argsArray, delim)
    var resValue = ""
    try {
      resValue = ms(paramName)
    } catch {
      case e: java.util.NoSuchElementException => println("key not found: " + paramName)
    }
    resValue
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




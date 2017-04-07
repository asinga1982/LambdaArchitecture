package utils

import java.lang.management.ManagementFactory

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
  * Created by aanishsingla on 24-Mar-17.
  */
object SparkUtils {
  // Check if the pgm is ran from IDE
  val is_IDE = ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")


  def getSparkContext(appName:String) = {


    var checkpointDir = ""

    //Cretae config
    val conf = new SparkConf()
      .setAppName(appName)
      .set("spark.casandra.connection.host", "127.0.0.1")


    // Check if running from IDE
     if (is_IDE)
    {
      System.setProperty("hadoop.home.dir", "E:\\newhadoop") // required for winutils
      conf.setMaster("local[*]")
      checkpointDir = "file:///e:/temp"
    } else {
       checkpointDir = "hdfs://127.0.0.1:9000/spark/checkpoint"
     }

    // setup spark context
    val sc = SparkContext.getOrCreate(conf)

    sc.setCheckpointDir(checkpointDir)
    //Return Spark Context
    sc
  }

  def getSQLContext(sc:SparkContext) = {

    implicit val sqlContext = SQLContext.getOrCreate(sc)

    sqlContext
  }

  def getStreamingContext(streamingApp : (SparkContext, Duration) => StreamingContext, sc: SparkContext, batchDuration: Duration) = {
  val creatingFunc = () => streamingApp(sc, batchDuration)
  val ssc = sc.getCheckpointDir match {
    case Some(checkpointDir) => StreamingContext.getActiveOrCreate(checkpointDir, creatingFunc, sc.hadoopConfiguration, createOnError = true)
    case None => StreamingContext.getActiveOrCreate(creatingFunc)
  }
  sc.getCheckpointDir.foreach(cp => ssc.checkpoint(cp))
  ssc
  }

}



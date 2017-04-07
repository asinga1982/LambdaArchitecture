package batch

import com.datastax.driver.core.Session
import com.datastax.spark.connector
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import utils.SparkUtils.{getSQLContext, getSparkContext}

import scala.tools.nsc.interpreter.session


/**
  * To Query Cassandra Tables
  * Created by aanishsingla on 07-Apr-17.
  */
object fetchCassTables {
  def main(args: Array[String]): Unit = {

    //Create spark Context
    val sc = getSparkContext("Spark with Cassandra")

    //Create SQL Context
    val sqlContext = getSQLContext(sc)

    val keyspace = "lambda"

    val rdd1 = sc.cassandraTable("lambda", "stream_activity_by_product")

    val rdd2 = sc.cassandraTable("lambda", "batch_visitors_by_product")

    val rdd3 = sc.cassandraTable("lambda", "batch_activity_by_product").select("product", "timestamp_hour")
      //.where("product == CVS Pharmacy,200mg Ibuprofen Capsules")

    // Join by RDD
    val rdd4 = rdd1.joinWithCassandraTable("lambda", "batch_activity_by_product")

    println("RDD4 Content")
    rdd4.foreach(println)
    /*
    println("RDD2 Content")
    rdd2.foreach(println)

    println("RDD3 Content")
    rdd3.foreach(println)
  */
  }
}

package batch

import com.datastax.driver.core.Session
import com.datastax.spark.connector
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import utils.SparkUtils.{getSQLContext, getSparkContext}

import scala.tools.nsc.interpreter.session

/**
  * To create Casssandra table and keyspace from Spark
  * Created by aanishsingla on 06-Apr-17.
  */
object CreateCassTables {
  def main(args: Array[String]): Unit = {

    //Create spark Context
    val sc = getSparkContext("Spark with Cassandra")

    //Create SQL Context
    val sqlContext = getSQLContext(sc)

    val keyspace = "lambda"

    CassandraConnector(sc.getConf).withSessionDo { session =>
      session.execute(s"DROP KEYSPACE if EXISTS $keyspace")
      session.execute("""CREATE KEYSPACE if NOT EXISTS %s
      WITH replication = {'class':'SimpleStrategy','replication_factor':'1'};""".format(keyspace))
      session.execute("""CREATE TABLE lambda.words (word text PRIMARY KEY, count int);""")
      session.execute("""INSERT INTO lambda.words (word, count) VALUES ('bar', 20);""")
      session.execute("""INSERT INTO lambda.words (word, count) VALUES ('foo', 10);""")

      session.execute("""CREATE TABLE lambda.stream_activity_by_product (
                         product text,
                         timestamp_hour bigint,
                         purchase_count bigint,
                         add_to_cart_count bigint,
                         page_view_count bigint,
                         PRIMARY KEY (product, timestamp_hour)
                         ) WITH CLUSTERING ORDER BY (timestamp_hour DESC);""")

      session.execute("""CREATE TABLE lambda.batch_activity_by_product (
                         product text,
                         timestamp_hour bigint,
                         purchase_count bigint,
                         add_to_cart_count bigint,
                         page_view_count bigint,
                         PRIMARY KEY (product, timestamp_hour)
                         ) WITH CLUSTERING ORDER BY (timestamp_hour DESC);""")


      session.execute("""CREATE TABLE lambda.batch_visitors_by_product (
                         product text,
                         timestamp_hour bigint,
                         unique_visitors bigint,
                         PRIMARY KEY (product, timestamp_hour)
                         ) WITH CLUSTERING ORDER BY (timestamp_hour DESC);""")

    }

    val rdd1 = sc.cassandraTable("lambda", "words")

    rdd1.foreach(println)

     //sqlContext.sql(createDDL) // Creates a Table in Cassandra

  }
}

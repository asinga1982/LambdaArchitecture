package batch

import java.lang.management.ManagementFactory

import config.Settings
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import domain._
import org.apache.spark.sql.functions._
import utils.SparkUtils._

/**
  * 1. Read data written by Streaming Job
  * 2. Create visitors by product and activity by product using Spark SQL
  * 3. Save data to Cassandra tables and HDFS
  * * Created by Aanish Singla on 5/1/2016.
  */
object BatchJob {
  def main (args: Array[String]): Unit = {

    //Create spark Context
    val sc = getSparkContext("Lambda with Spark")

    //Create SQL Context
    val sqlContext = getSQLContext(sc)

   // implicit val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._
    val wlc = Settings.WebLogGen
    val hdfsPath = wlc.hdfsPath

    //Read data (written by StreamingJob) which is less than 6 hours old
    val inputDF = sqlContext.read.parquet(hdfsPath)
      .where("unix_timestamp() - timestamp_hour/1000 <= 60*60*6")

    // initialize input RDD
   // val sourceFile = "E:\\Boxes\\spark-kafka-cassandra-applying-lambda-architecture-master\\vagrant\\data.tsv"
   // val sourceFile = "file:///vagrant/data.tsv"
   // val input = sc.textFile(sourceFile)

    //Split fields  separated by tab and and create a RDD of type Activity

    /*
    val inputRDD = input.flatMap{ line =>
      val record = line.split("\\t")
      val MS_ts_hr = 1000 * 60 * 60

      if(record.length == 7)
        Some(Activity(record(0).toLong / MS_ts_hr * MS_ts_hr, record(1), record(2), record(3), record(4), record(5), record(6)   ))
      else
        None
    }

    //Convert to DataFrame and select all coulmns
    val inputDF = inputRDD.toDF()
    */
    val df = inputDF.select(
      add_months(from_unixtime(inputDF("timestamp_hour") / 1000), 1).as("timestamp_hour"),
      inputDF("referrer"), inputDF("action"), inputDF("prevPage"), inputDF("page"), inputDF("visitor"), inputDF("product")
    ).cache()

  //Crate a temp tale

    df.registerTempTable("activity")

    // Create a User defined fn to calculate score

    sqlContext.udf.register("myfunc", (page_view: Long, add_to_cart:Long) =>
                  if(page_view == 0) 0 else add_to_cart/page_view )

  // Aggregate data by product and timestamp and count of distinct vistors
    val visitorsByPrd = sqlContext.sql(
      """ SELECT product, timestamp_hour, COUNT(DISTINCT visitor) as unique_visitors
        |FROM activity
        |GROUP BY product, timestamp_hour """.stripMargin)

  //  visitorsByPrd.show()

  // write the DataFrame to Cassandra
      visitorsByPrd
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace"->"lambda", "table" -> "batch_visitors_by_product"))
      .save()

    // Aggregate data by product and timestamp and sum of pge views, add to carts and purchases

    val activityByProduct = sqlContext.sql("""SELECT
                                            product,
                                            timestamp_hour,
                                            sum(case when action = 'purchase' then 1 else 0 end) as purchase_count,
                                            sum(case when action = 'add_to_cart' then 1 else 0 end) as add_to_cart_count,
                                            sum(case when action = 'page_view' then 1 else 0 end) as page_view_count
                                            from activity
                                            group by product, timestamp_hour """).cache()

    //activityByProduct.show()

    //Write to Cassandra
    activityByProduct
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "lambda", "table" -> "batch_activity_by_product"))
      .save()

    activityByProduct.registerTempTable("act")

    //activityByProduct.printSchema()

  // Find the Prodcuts which have good add to cart ratio
    val newdf = sqlContext.sql("""SELECT product,
                  timestamp_hour,
                  myfunc(page_view_count, add_to_cart_count) as score
                  |FROM act
                  |order by score desc""".stripMargin)

    //newdf.show(5)

    // Same operations using RDD
    /*
    val keyedRDD = inputDFkeyBy(a => (a.product, a.timestamp_hour)).cache()

    val cont = inputRDD.groupBy(a => (a.product, a.timestamp_hour))
      .mapValues{a =>
      a.groupBy(_.visitor).mapValues(_.size)

      }
      .mapValues(_.size)

    //cont.foreach(println)

    val keyedByPrdct2 = keyedRDD.mapValues( a => a.visitor)
      .distinct
      .countByKey()

    //keyedByPrdct2.foreach(println)

    val actByAction = keyedRDD
      .mapValues { a =>
        a.action match {
          case "purchase" => (1,0,0)
          case "add_to_cart" => (0,1,0)
          case "page_view" => (0,0,1)
        }
      }
      .reduceByKey((a,b) => (a._1+b._1, a._2+b._2, a._3 + b._3) )
    */
    // Write data to HDFS
    //  activityByProduct.write.mode(SaveMode.Append)
    //  .parquet("E:\\Boxes\\spark-kafka-cassandra-applying-lambda-architecture-master\\vagrant\\new")
    activityByProduct.write.partitionBy("timestamp_hour").mode(SaveMode.Append)
      .parquet("hdfs://127.0.0.1:9000/lambda/batch1")

    // test by reading back
    // val pDF = sqlContext.sql("SELECT * FROM parquet.`E:\\Boxes\\spark-kafka-cassandra-applying-lambda-architecture-master\\vagrant\\new\\` WHERE page_view_count > 2  ")
    // pDF.show(10)
  }
}

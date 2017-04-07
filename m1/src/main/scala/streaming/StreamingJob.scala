package streaming

import domain.{Activity, ActivityByProduct}
import _root_.kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.{Duration, Minutes, Seconds, StateSpec, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import utils.SparkUtils._
import functions._
import com.twitter.algebird.HyperLogLogMonoid
import config.Settings
import config.Settings.WebLogGen
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import org.apache.spark.sql.SaveMode
import org.apache.spark.storage.StorageLevel
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector._

import scala.util.Try

/**
  * Created by aanishsingla on 24-Mar-17.
  */
object StreamingJob {
  def main(args: Array[String]): Unit = {

    //Create spark Context
    val sc = getSparkContext("Lambda with Spark")
    val sqlContext = getSQLContext(sc)

    sc.setLogLevel("ERROR")

    import sqlContext.implicits._

    val batchDuration = Seconds(4)
    def streamingApp(sc: SparkContext, batchDuration: Duration) = {
      //Create Spark streaming Context
      val ssc = new StreamingContext(sc, batchDuration)
      val wlc = Settings.WebLogGen

      // Set input path
      val inputPath = is_IDE match {
        case true => "file:///E:/Boxes/spark-kafka-cassandra-applying-lambda-architecture-master/vagrant/input"
        case false => "file:///vagrant/input"
      }

      val topic = "weblogs-text"

      // Parameters or direct API
      val kafkaDirectParam = Map(
        "metadata.broker.list" -> "localhost:9092",  //Kafka Broker
        "group.id" -> "lambda",
        "auto.offset.reset" -> "largest"
      )

      var fromOffsets : Map[TopicAndPartition, Long] = Map.empty
      val hdfsPath = wlc.hdfsPath

      Try(sqlContext.read.parquet(hdfsPath)).foreach( hdfsData =>
        fromOffsets = hdfsData.groupBy("topic", "partition").agg(max("UntilOffset").as("UntilOffset"))
          .collect().map { row =>
          (TopicAndPartition(row.getAs[String]("topic"), row.getAs[Int]("partition")),
            row.getAs[String]("UntilOffset").toLong + 1)
        }.toMap
      )

      /*
      val hdfsData =  sqlContext.read.parquet(hdfsPath)

    //Retrieve offsets from HDFS to provide resilience from code changes
      val fromOffsets = hdfsData.groupBy("Topic","Partition").agg(max("UntilOffset").as("UntilOffset")).collect()
        .map{ row =>
          (TopicAndPartition(row.getAs[String]("Topic"), row.getAs[Int]("Partition")),
            row.getAs[String]("UntilOffset").toLong + 1)
        }.toMap
     */
        val kafkaDirectStream = fromOffsets.isEmpty match{
        case true =>
          //Set up Direct API receiver
           KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
            ssc,kafkaDirectParam, Set(topic))
        case false =>
          //If offsets are available use them to create the stream
          KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
            ssc, kafkaDirectParam, fromOffsets, {mmd:MessageAndMetadata[String, String] => (mmd.key(), mmd.message())})
      }

      /*
      // Receiver Parameters
      val kafkaParams = Map(
        "zookeeper.connect" -> "localhost:2181",  //ZooKeeper
        "group.id" -> "lambda",
        "auto.offset.reset" -> "largest"
      )
      */

      /*
      //Set up 3 receivers using receiver based approach
      val receiverCount = 3

      // Creating 3 receivers
      val kafkaStreams = (1 to receiverCount). map { _ =>
        KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
          ssc, kafkaParams, Map(topic -> 1), StorageLevel.MEMORY_AND_DISK)
          }

      // Combine all the 3 streams
      val kafkaStream = ssc.union(kafkaStreams).map(_._2)

      */
      // Create the streaming from file
    //  val textDstream = ssc.textFileStream(inputPath)

     //val textDstream = kafkaStream
      val textDstream = kafkaDirectStream
      // Print Data from file
      //textDstream.print()

      // Work on RDDS inside Dstream
      val activityDSream = textDstream.transform ( input =>
        functions.RDDtoRDDActivity(input)
        ).cache()

      //Write to HDFS
      activityDSream.foreachRDD { rdd =>
        val activityDF= rdd.toDF().selectExpr("timestamp_hour", "referrer", "action", "prevPage", "page", "visitor", "product",
          "inputProps.topic as topic", "inputProps.kafkaPartition as partition", "inputProps.fromOffset as FromOffset",
          "inputProps.untilOffset as UntilOffset")

        activityDF
          .write
          .partitionBy("topic", "partition", "timestamp_hour")
          .mode(SaveMode.Append)
          .parquet(hdfsPath)
      }

      val activityStateSpec =
        StateSpec
          .function(mapActivityStateFunc)
          .timeout(Minutes(120))

      // Act on the DStream and run SQL statement and convert to a map
      val statefulStream = activityDSream.transform(input => {

        val df = input.toDF()
        df.registerTempTable("activity")

        val activityByProduct = sqlContext.sql(
          """SELECT
                                            product,
                                            timestamp_hour,
                                            sum(case when action = 'purchase' then 1 else 0 end) as purchase_count,
                                            sum(case when action = 'add_to_cart' then 1 else 0 end) as add_to_cart_count,
                                            sum(case when action = 'page_view' then 1 else 0 end) as page_view_count
                                            from activity
                                            group by product, timestamp_hour """)

        activityByProduct.map{ r =>
          ((r.getString(0), r.getLong(1)),
            ActivityByProduct(r.getString(0), r.getLong(1), r.getLong(2), r.getLong(3), r.getLong(4)))

        }

      }
      ).mapWithState(activityStateSpec)

      val activityStateSnapshot = statefulStream.stateSnapshots()
      activityStateSnapshot
          .reduceByKeyAndWindow(
            (a,b) => b,
            (x,y) => x,
            Seconds(30/4 * 4)
          )
        //.foreachRDD(rdd => {rdd
            .map(sr => ActivityByProduct(sr._1._1, sr._1._2, sr._2._1, sr._2._2, sr._2._3))
            .saveToCassandra("lambda", "stream_activity_by_product")

          //  .toDF().registerTempTable("ActivityByProduct")

          //val out = sqlContext.sql("""SELECT * FROM ActivityByProduct""")
          //out.show(5)
        //})

      /*
        .updateStateByKey((newval:Seq[ActivityByProduct],currentState:Option[(Long, Long, Long)]) => {
        var (purchase_count, add_to_cart_count, page_view_count) = currentState.getOrElse(0L, 0L, 0L)

        newval.foreach(newrec => {
          purchase_count += newrec.purchaseCount
          add_to_cart_count += newrec.addToCartCount
          page_view_count += newrec.pageViewCount
        }
          )

        Some((purchase_count, add_to_cart_count, page_view_count))
      }
        )
    */

      ssc
    }
    // Start the process and run indefinitely

    val ssc = getStreamingContext(streamingApp, sc, batchDuration)
    ssc.start()
    ssc.awaitTermination()
  }
}

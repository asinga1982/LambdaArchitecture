import domain.{Activity, ActivityByProduct}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.State
import org.apache.spark.streaming.kafka.HasOffsetRanges

/**
  * Holds functions for:
  * a. RDDtoRDDActivity -> Converts RDD of (String,String) to RDD of type Activity by first converting it to Array[OffsetRanges]
  * and fetching topic, partition, from offset and To offset
  * b. mapActivityStateFunc ->
  * Created by aanishsingla on 03-Apr-17.
  */
package object functions {

  def RDDtoRDDActivity(input:RDD[(String, String)]):RDD[Activity] = {
    val offsetRanges = input.asInstanceOf[HasOffsetRanges].offsetRanges
    input.mapPartitionsWithIndex({ (index, it) =>
      val or = offsetRanges(index)

      it.flatMap { kv =>
        val line = kv._2
        val record = line.split("\\t")
        val MS_ts_hr = 1000 * 60 * 60

        if (record.length == 7)
          Some(Activity(record(0).toLong / MS_ts_hr * MS_ts_hr, record(1), record(2), record(3), record(4), record(5), record(6),
            Map("topic" -> or.topic, "kafkaPartition" -> or.partition.toString, "fromOffset" -> or.fromOffset.toString,
              "untilOffset" -> or.untilOffset.toString)))
        else
          None
      }
    })
  }

  def mapActivityStateFunc = (k:(String, Long), v:Option[ActivityByProduct], state: State[(Long, Long, Long)]) => {

    var (purchase_count, add_to_cart_count, page_view_count) = state.getOption().getOrElse(0L, 0L, 0L)

    val newVal = v match {
      case Some(a:ActivityByProduct) => ( a.purchaseCount,a.addToCartCount, a.pageViewCount)
      case _ => (0L, 0L, 0L)
    }

    purchase_count += newVal._1
    add_to_cart_count += newVal._2
    page_view_count += newVal._3

    state.update(purchase_count, add_to_cart_count, page_view_count)

    val underExposed = {
      if (purchase_count == 0)
        0
      else
        page_view_count/purchase_count
    }
    underExposed
  }

}
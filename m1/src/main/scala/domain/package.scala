/**
  * Created by aanishsingla on 22-Mar-17.
  */
package object domain {
  case class Activity(timestamp_hour: Long,
                      referrer: String,
                      action:String,
                      prevPage: String,
                      page: String,
                      visitor: String,
                      product: String,
                      inputProps: Map[String,String] =Map() )

  case class ActivityByProduct(product: String,
                      timestamp_hour: Long,
                      purchaseCount: Long,
                      addToCartCount:Long,
                      pageViewCount: Long)

  case class VisitorsByProduct(product: String,
                               timestamp_hour: Long,
                               unique_visitors:Long)
}

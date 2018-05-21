package demo

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream


object HashTagsStreaming {
  case class Popularity(tag: String, amount: Int)

  private[demo] def extractTrendingTags(input: RDD[String]): RDD[Popularity] =
    input.flatMap(_.split("\\s+")) // Split on any white character
      .filter(_.startsWith("#")) // Keep only the hashtags
      // Remove punctuation, force to lowercase
      .map(_.replaceAll("[,.!?:;]", "").toLowerCase)
      // Remove the first #
      .map(_.replaceFirst("^#", ""))
      .filter(!_.isEmpty) // Remove any non-words
      .map((_, 1)) // Create word count pairs
      .reduceByKey(_ + _) // Count occurrences
      .map(r => Popularity(r._1, r._2))
      // Sort hashtags by descending number of occurrences
      .sortBy(r => (-r.amount, r.tag), ascending = true)

  def processTrendingHashTags(input: DStream[String],
                              windowLength: Int,
                              slidingInterval: Int,
                              n: Int,
                              handler: Array[Popularity] => Unit): Unit = {
    val sortedHashtags: DStream[Popularity] = input
      .window(Seconds(windowLength), Seconds(slidingInterval)) //create a window
      .transform(extractTrendingTags(_)) //apply transformation

    sortedHashtags.foreachRDD(rdd => {
      handler(rdd.take(n)) //take top N hashtags and save to external source
    })
  }

}

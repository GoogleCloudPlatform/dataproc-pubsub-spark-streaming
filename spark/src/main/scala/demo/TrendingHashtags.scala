/*
 Copyright Google Inc. 2018
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at
 http://www.apache.org/licenses/LICENSE-2.0
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package demo

import java.nio.charset.StandardCharsets

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.pubsub.PubsubUtils
import org.apache.spark.streaming.pubsub.SparkGCPCredentials
import org.apache.spark.streaming.pubsub.SparkPubsubMessage
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf

import com.google.cloud.Timestamp
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.FullEntity
import com.google.cloud.datastore.ListValue;


object TrendingHashtags {
  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      System.err.println(
        """
          | Usage: TrendingHashtags <projectID> <windowLength> <slidingInterval> <totalRunningTime>
          |
          |     <projectID>: ID of Google Cloud project
          |     <windowLength>: The duration of the window, in seconds
          |     <slidingInterval>: The interval at which the window calculation is performed, in seconds
          |     <totalRunningTime>: Total running time for the application, in minutes. If 0, runs indefinitely until termination.
          |
        """.stripMargin)
      System.exit(1)
    }

    val Seq(projectID, windowLength, slidingInterval, totalRunningTime) = args.toSeq

    val inputSubscription = "tweets-subscription"  // Cloud Pub/Sub subscription for incoming tweets

    // Create Spark context
    val sparkConf = new SparkConf().setAppName("TrendingHashtags")
    val ssc = new StreamingContext(sparkConf, Seconds(slidingInterval.toInt))

    // Create streams
    val pubsubStream: ReceiverInputDStream[SparkPubsubMessage] = PubsubUtils.createStream(
      ssc, projectID, None, inputSubscription,
      SparkGCPCredentials.builder.build(), StorageLevel.MEMORY_AND_DISK_SER_2)
    val windowedStream = pubsubStream.window(Seconds(windowLength.toInt), Seconds(slidingInterval.toInt))

    // Extract and count hashtags
    val hashtags = (
        windowedStream
        .map(message =>              // Extract the Pub/Sub message data
            new String(
                message.getData(),
                StandardCharsets.UTF_8
            )
        )
        .flatMap(_.split("\\s+"))    // Split on any white character
        .filter(_.startsWith("#"))   // Keep only the hashtags
        .map(_.replaceAll(
            "[,.!?:;]", "")          // Remove punctuation
            .toLowerCase)            // Force to lowercase
        .filter(!_.isEmpty)          // Remove any non-words
        .map((_, 1))                 // Create word count pairs
        .reduceByKey(_ + _)          // Count occurrences
    )

    // Sort hashtags by descending number of occurrences
    val sortedHashtags = hashtags.transform(rdd => rdd.sortBy(_._2, ascending=false))

    val datastore = DatastoreOptions.getDefaultInstance().getService()

    // Publish the hashtags with highest occurrences to Cloud Pub/Sub
    sortedHashtags.foreachRDD { rdd =>

        val datetime = Timestamp.now()
        val hashtags = rdd.take(10).map( record =>
            Map("name" -> record._1, "occurrences" -> record._2)
        )

        // Create the Cloud Datastore embedded entity to host the hashtags
        val listValue = ListValue.newBuilder()
        val hashtagKeyFactory = datastore.newKeyFactory().setKind("Hashtag")
        for (hashtag <- hashtags) {
            val entity = FullEntity.newBuilder(hashtagKeyFactory.newKey())
                .set("name", hashtag("name").asInstanceOf[String])
                .set("occurrences", hashtag("occurrences").asInstanceOf[Int])
                .build()
            listValue.addValue(entity)
        }

        // Save the values to Cloud Datastore
        val keyFactory = datastore.newKeyFactory().setKind("TrendingHashtags")
        val entity = FullEntity.newBuilder(keyFactory.newKey())
            .set("datetime", datetime)
            .set("hashtags", listValue.build())
            .build()
        datastore.add(entity)

        // Display some info in the job's logs
        println("\n-------------------------")
        println(s"Window ending ${datetime} for the past ${windowLength} seconds\n")
        if (hashtags.length == 0) {
            println ("No trending hashtags in this window.")
        }
        else {
            println ("Trending hashtags in this window:")
            for (hashtag <- hashtags) {
                val name = hashtag("name")
                val occurrences = hashtag("occurrences")
                println(s"${name}, ${occurrences}")
            }
        }
    }

    // Start streaming until we receive an explicit termination
    ssc.start()

    if (totalRunningTime.toInt == 0) {
        ssc.awaitTermination()
    }
    else {
        ssc.awaitTerminationOrTimeout(1000 * 60 * totalRunningTime.toInt)
    }
  }

}
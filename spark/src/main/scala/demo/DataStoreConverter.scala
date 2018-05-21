package demo

import com.google.cloud.Timestamp
import com.google.cloud.datastore._
import demo.HashTagsStreaming.Popularity

object DataStoreConverter {

  private def convertToDatastore(keyFactory: KeyFactory,
                                 record: Popularity): FullEntity[IncompleteKey] =
    FullEntity.newBuilder(keyFactory.newKey())
      .set("name", record.tag)
      .set("occurrences", record.amount)
      .build()

  private[demo] def convertToEntity(hashtags: Array[Popularity],
                                    keyFactory: String => KeyFactory): FullEntity[IncompleteKey] = {
    val hashtagKeyFactory: KeyFactory = keyFactory("Hashtag")

    val listValue = hashtags.foldLeft[ListValue.Builder](ListValue.newBuilder())(
      (listValue, hashTag) => listValue.addValue(convertToDatastore(hashtagKeyFactory, hashTag))
    )

    val rowKeyFactory: KeyFactory = keyFactory("TrendingHashtags")

    FullEntity.newBuilder(rowKeyFactory.newKey())
      .set("datetime", Timestamp.now())
      .set("hashtags", listValue.build())
      .build()
  }

  def saveRDDtoDataStore(tags: Array[Popularity],
                         windowLength: Int,
                         datastore: Datastore): Unit = {
    val keyFactoryBuilder = (s: String) => datastore.newKeyFactory().setKind(s)

    val entity: FullEntity[IncompleteKey] = convertToEntity(tags, keyFactoryBuilder)

    datastore.add(entity)

    // Display some info in the job's logs
    println("\n-------------------------")
    println(s"Window ending ${Timestamp.now()} for the past ${windowLength} seconds\n")
    if (tags.length == 0) {
      println("No trending hashtags in this window.")
    }
    else {
      println("Trending hashtags in this window:")
      tags.foreach(hashtag => println(s"${hashtag.tag}, ${hashtag.amount}"))
    }
  }
}

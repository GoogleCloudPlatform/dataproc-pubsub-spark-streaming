In this tutorial you learn how to deploy an [Apache Spark](https://spark.apache.org/) streaming application on
[Cloud Dataproc](https://cloud.google.com/dataproc/) and process messages from [Cloud Pub/Sub](https://cloud.google.com/pubsub/)
in near real-time. The system you build in this scenario generates thousands of random tweets, identifies trending
hashtags over a sliding window, saves results in [Cloud Datastore](https://cloud.google.com/datastore/), and displays
the results on a web page.

Please refer to the related article for all the steps to follow in this tutorial:

https://cloud.google.com/solutions/using-apache-spark-dstreams-with-dataproc-and-pubsub

Contents of this repository:

* `http_function`: Javascript code for the HTTP function deployed on [Cloud Functions](https://cloud.google.com/functions/).
* `spark`: Scala code for the Apache Spark streaming application.
* `tweet-generator`: Python code for the randomized tweet generator.

Running the tests
-----------------

To run the tests:

```bash
cd spark
mvn test
```

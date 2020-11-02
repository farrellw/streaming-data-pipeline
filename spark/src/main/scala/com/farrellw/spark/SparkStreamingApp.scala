package com.farrellw.spark

import java.sql.Date

import com.farrellw.spark.models.{Customer, EnrichedReview, Review}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.log4j.Logger
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.util.Try

/**
 * Spark Structured Streaming app
 *
 * Takes one argument, for Kafka bootstrap servers (ex: localhost:9092)
 */
object SparkStreamingApp {
  lazy val logger: Logger = Logger.getLogger(this.getClass)

  // Two implicit helpers for Hbase to help in converting String to Bytes, and back.
  implicit def stringToBytes(str: String): Array[Byte] = Bytes.toBytes(str)

  implicit def bytesToString(bytes: Array[Byte]): String = Bytes.toString(bytes)

  val jobName = "SparkStreamingApp"
  val datanodeAddress = "hdfs://cdh.kitmenke.com:8020"

  val tableName = "will:users"

  def main(args: Array[String]): Unit = {
    // Set system property to username on the Cloudera node.
    System.setProperty("HADOOP_USER_NAME", "will")

    try {
      val spark = SparkSession.builder().config("spark.hadoop.dfs.client.use.datanode.hostname", "true").config("spark.hadoop.fs.defaultFS", datanodeAddress).appName(jobName).master("local[*]").getOrCreate()

      val bootstrapServers = args(0)

      // Lines 43 left in to debug
      // if data is not currently being published to the Kafka topic.
      val df: DataFrame = spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrapServers)
        .option("subscribe", "reviews")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", "5")
        .option("sep", "\t")
        .load()
        .selectExpr("CAST(value AS STRING)")

      import spark.implicits._

      // Travel into Dataset World
      val ds = df.select("value").as[String]

      // Two versions of parsing left in for comparison. Can only keep one in when running the program
      val parsed: Dataset[Review] = verboseParse(ds, spark)
      //      val parsed = parse(ds)

      val newRdd = parsed.mapPartitions(partition => {
        // Setup Hbase connection on each partition
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", "cdh.kitmenke.com:2181")
        val connection = ConnectionFactory.createConnection(conf)

        val table = connection.getTable(TableName.valueOf(tableName))

        // Within each partition, map over every review and retrieve the customer with the corresponding customer_id
        val newPartition = partition.map(r => {
          val get = new Get(r.customer_id).addFamily("f1")
          val result = table.get(get)

          // Retrieve values from the result returned
          val name = result.getValue("f1", "name")
          val birthdate = result.getValue("f1", "birthdate")
          val mail = result.getValue("f1", "mail")
          val sex = result.getValue("f1", "sex")
          val username = result.getValue("f1", "username")

          EnrichedReview(r, Customer(name, birthdate, mail, sex, username))
        }).toList //Collect the partition on its own machine before closing the hbase connection. If you don't, hbase collection will close before any of the get requests are sent.


        connection.close()
        newPartition.iterator
      })

      val query = newRdd.writeStream
        .outputMode(OutputMode.Append())
        .format("json")
        .option("path", datanodeAddress + "/user/will/enriched_reviews")
        .option("checkpointLocation", datanodeAddress + "/user/will/enriched_reviews_checkpoint")
        .trigger(Trigger.ProcessingTime("5 seconds"))
        .start()

      query.awaitTermination()
    } catch {
      case e: Exception => logger.error(s"$jobName error in main", e)
    }
  }

  def verboseParse(ds: Dataset[String], sparkSession: SparkSession): Dataset[Review] = {
    import sparkSession.implicits._
    val split = ds.map(_.split("\t"))
    // Filter out invalid data. Make sure every record has the appropriate number of elements and is NOT the header row.
    val headerColumnOne = "marketplace"
    val validRecords = split
      .filter(_.length > 2)
      .filter(x => {
        val firstColumn = x(0)
        firstColumn != headerColumnOne
      })

    // Create a Review case class from the record
    val reviews: Dataset[Review] = validRecords.map(x => {
      // Remove the trailing newline.
      val dateAsString = x(14).stripLineEnd

      // Parse into a date object using Date.valueOf
      val date = Try(Date.valueOf(dateAsString)).toOption

      // Turn array into a Review case class
      Review(x(0), x(1), x(2), x(3), x(4).toInt, x(5), x(6), x(7).toInt, x(8).toInt, x(9).toInt, x(10), x(11), x(12), x(13), date)
    })

    reviews
  }

  def parse(ds: Dataset[String], sparkSession: SparkSession): Dataset[Review] = {
    import sparkSession.implicits._

    ds.map(_.split("\t")).filter(r => r.length > 2 && r(0) != "marketplace").map(x => {
      Review(x(0), x(1), x(2), x(3), x(4).toInt, x(5), x(6), x(7).toInt, x(8).toInt, x(9).toInt, x(10), x(11), x(12), x(13), Try(Date.valueOf(x(14).stripLineEnd)).toOption)
    })
  }
}

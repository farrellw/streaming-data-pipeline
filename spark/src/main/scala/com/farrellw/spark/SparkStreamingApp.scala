package com.farrellw.spark

import com.farrellw.spark.models.{Customer, EnrichedReview, WrappedReview}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Spark Structured Streaming app
 *
 * Takes one argument, for Kafka bootstrap servers (ex: localhost:9092)
 */
object SparkStreamingApp {
  lazy val logger: Logger = Logger.getLogger(this.getClass)
  implicit def stringToBytes(str: String): Array[Byte] = Bytes.toBytes(str)
  implicit def bytesToString(bytes: Array[Byte]): String = Bytes.toString(bytes)

  val jobName = "SparkStreamingApp"
  val schema: StructType = new StructType()
    .add("marketplace", StringType, nullable = true)
    .add("customer_id", IntegerType, nullable = true)
    .add("review_id", StringType, nullable = true)
    .add("product_id", StringType, nullable = true)
    .add("product_parent", IntegerType, nullable = true)
    .add("product_title", StringType, nullable = true)
    .add("product_category", StringType, nullable = true)
    .add("star_rating", IntegerType, nullable = true)
    .add("helpful_votes", IntegerType, nullable = true)
    .add("total_votes", IntegerType, nullable = true)
    .add("vine", StringType, nullable = true)
    .add("verified_purchase", StringType, nullable = true)
    .add("review_headline", StringType, nullable = true)
    .add("review_body", StringType, nullable = true)
    .add("review_date", TimestampType, nullable = true)

  def main(args: Array[String]): Unit = {
    try {
      val spark = SparkSession.builder().appName(jobName).master("local[*]").getOrCreate()
      val bootstrapServers = args(0)


      // Lines 50 and 51 are left in to debug
      // if data is not currently being published to the Kafka topic.
      val df = spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrapServers)
        .option("subscribe", "reviews")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", "200")
        .load()
        .selectExpr("CAST(value AS STRING)")

      df.printSchema()

      val parsed = compute(df)

      import spark.implicits._
      val structured = parsed.as[WrappedReview].map(_.js)

      val newRdd = structured.mapPartitions(partition => {
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", "35.184.255.239")

        // TODO switch and use a connection pool
        val connection = ConnectionFactory.createConnection(conf)

        val table = connection.getTable(TableName.valueOf("kit:users"))

        val newPartition = partition.map(r => {
          val get = new Get(r.customer_id.toString).addFamily("f1")
          val result = table.get(get)
          val name = result.getValue("f1", "name")
          val birthdate = result.getValue("f1", "birthdate")
          val mail = result.getValue("f1","mail")
          val sex = result.getValue("f1", "sex")
          val username = result.getValue("f1", "username")
          EnrichedReview(r, Customer(name, birthdate, mail, sex, username))
        }).toList //Collect the partition on its own machine before closing the hbase connection


        connection.close()
        newPartition.iterator
      })

      val query = newRdd.writeStream
        .outputMode(OutputMode.Append())
        .format("parquet")
        .option("path", "hdfs://35.184.255.239:8888/user/will")
        .trigger(Trigger.ProcessingTime("5 seconds"))
        .start()

      query.awaitTermination()
    } catch {
      case e: Exception => logger.error(s"$jobName error in main", e)
    }
  }

  def compute(df: DataFrame): DataFrame = {
    df.select(from_json(df("value"), schema) as "js")
  }
}

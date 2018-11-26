package kit

import java.io._
import java.text.SimpleDateFormat
import java.util.{Date, Properties}
import com.typesafe.config.ConfigFactory
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.slf4j.LoggerFactory

object KafkaKits {

  val logger = LoggerFactory.getLogger(KafkaKits.getClass)

  // load kafka config
  lazy val proConfig = ConfigFactory.load("kafka.conf")

  lazy val kafkaBrokerList = proConfig.getString("kafka.producer.broker.list")

  val props = new Properties()
  props.put("bootstrap.servers", kafkaBrokerList)
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  lazy val producer = new KafkaProducer[String, String](props)

  def send(topicName: String, iteratorSend: Iterator[String]): Unit = {
    iteratorSend.foreach { record =>
      producer.send(new ProducerRecord[String, String](topicName, record))
    }
  }

  def send(topicName: String, record: String): Unit = {
    producer.send(new ProducerRecord[String, String](topicName, record))
  }

  def close(): Unit = {
    producer.close()
  }

  /**
    * Create DStream from checkpoint file or out of nowhere!
    * checkPoint  on hdfs
    *
    * @param ssc
    * @param hdfs
    * @param offsetPath
    * @param kafkaParams
    * @param topicName
    * @return
    */
  def createKafkaDStream(ssc: StreamingContext,
                         hdfs: FileSystem,
                         offsetPath: Path,
                         kafkaParams: Map[String, String],
                         topicName: String) = {
    if (hdfs.exists(offsetPath)) {
      // Read offsetRanges from file.
      val objInputStream = new ObjectInputStream(hdfs.open(offsetPath))
      val offsetRanges = objInputStream.readObject.asInstanceOf[Array[OffsetRange]]
      objInputStream.close()
      // set offsetMap
      var fromOffsets = Map[TopicAndPartition, Long]()
      offsetRanges.foreach(o =>
        fromOffsets = fromOffsets + (o.topicAndPartition -> o.untilOffset))
      val extractKeyValue = (msgAndMeta: MessageAndMetadata[String, String]) =>
        (msgAndMeta.key, msgAndMeta.message)
      KafkaUtils.createDirectStream[String,
        String,
        StringDecoder,
        StringDecoder,
        (String, String)](ssc,
        kafkaParams,
        fromOffsets,
        extractKeyValue)
    } else {
      val topicsSet = Set[String](topicName)
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc,
        kafkaParams,
        topicsSet)
    }
  }

  /**
    * Write kafka offset to hdfs.
    *
    */
  def saveOffset(hdfs: FileSystem,
                 offsetPath: Path,
                 offsetRanges: Array[OffsetRange]): Unit = {
    val oos = new ObjectOutputStream(hdfs.create(offsetPath))
    oos.writeObject(offsetRanges)
    oos.close()
  }

  /**
    * Checkpoint kafka stream offset to file.
    *
    * @param stream
    * @param destPath
    * @tparam T
    */
  def checkpointOffset[T](stream: DStream[T], hdfs: FileSystem, destPath: Path): Unit = {
    stream.foreachRDD { rdd =>
      logger.info(s" ====== Checkpointing to $destPath ====== ")
      val offsetOS = new ObjectOutputStream(hdfs.create(destPath))
      offsetOS.writeObject(rdd.asInstanceOf[HasOffsetRanges].offsetRanges)
      offsetOS.close()
    }
  }

  /**
    * Recover memorizeDate from checkpoint OR create new.
    *
    */
  def getMemorizedDate(hdfs: FileSystem, datePath: Path): String = {
    val format = new SimpleDateFormat("yyyy-MM-dd") // Date format for hp_stat_date
    if (hdfs.exists(datePath)) {
      val ois = new ObjectInputStream(hdfs.open(datePath))
      val currentData = ois.readObject.asInstanceOf[String]
      ois.close()
      currentData
    } else format.format(new Date())
  }

  /**
    * Write current date to hdfs.
    *
    */
  def checkpointDate(hdfs: FileSystem, datePath: Path, date: String): Unit = {
    val oos = new ObjectOutputStream(hdfs.create(datePath))
    oos.writeObject(date)
    oos.close()
  }


  /**
    * Create DStream from checkpoint file or out of nowhere!
    * checkPoint on local
    *
    * @param ssc
    * @param offsetPath
    * @param kafkaParams
    * @param topicName
    * @return
    */
  def createKafkaDStreamLocal(ssc: StreamingContext,
                              offsetPath: String,
                              kafkaParams: Map[String, String],
                              topicName: String) = {

    val file = new File(offsetPath)
    if (file.exists()) {
      // Read offsetRanges from file.
      val objInputStream = new ObjectInputStream(new FileInputStream(file))
      val offsetRanges = objInputStream.readObject.asInstanceOf[Array[OffsetRange]]
      objInputStream.close()
      // set offsetMap
      var fromOffsets = Map[TopicAndPartition, Long]()
      offsetRanges.foreach(o =>
        fromOffsets = fromOffsets + (o.topicAndPartition -> o.untilOffset))
      val extractKeyValue = (msgAndMeta: MessageAndMetadata[String, String]) =>
        (msgAndMeta.key, msgAndMeta.message)
      KafkaUtils.createDirectStream[String,
        String,
        StringDecoder,
        StringDecoder,
        (String, String)](ssc,
        kafkaParams,
        fromOffsets,
        extractKeyValue)

    } else {
      val topicsSet = Set[String](topicName)
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc,
        kafkaParams,
        topicsSet)
    }
  }

  /**
    * Write kafka offset to local.
    *
    */
  def checkpointOffsetLocal(offsetPath: String,
                            offsetRanges: Array[OffsetRange]): Unit = {
    val file = new File(offsetPath)
    if (!file.exists()) {
      file.createNewFile()
    }
    val oos = new ObjectOutputStream(new FileOutputStream(file))
    oos.writeObject(offsetRanges)
    oos.close()

  }
}

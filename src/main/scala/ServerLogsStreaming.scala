import java.io.File
import java.nio.charset.Charset
import java.text.SimpleDateFormat
import java.util.Date

import com.google.common.io.Files
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._

import scala.util.Try

case class LogRecord(host: String, source: String, timestamp: String, url: String, code: Int)

object ServerLogsStreaming {

  def createOutputMessage(message: String) : String = {
    val formatter = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss");
    formatter.format(new Date()) ++ " INFO " ++ message ++ "\n";
  }

  def createLogRecord(record: String) : Seq[LogRecord] = {
    val splittedStr = record.split(" ")
    if (splittedStr.length < 5) {
      return Seq.empty
    }
    val host = splittedStr(0)
    val source = splittedStr(1)
    val timestamp = splittedStr(2)
    val url = splittedStr(3)
    val codeStr = splittedStr(4)
    if (Try(codeStr.toInt).isFailure) {
      return Seq.empty
    }
    val code = codeStr.toInt
    return Seq(LogRecord(host, source, timestamp, url, code))
  }

  val updateStateNew = (values: Seq[Long], state: Option[(Long, Boolean)]) => {
    if (state.isEmpty) {
      Some((values.foldLeft(0L)((r, c) => math.max(r, c)), true))
    } else {
      val prevTimestamp = state.get._1
      val newTimeStamp = values.foldLeft(prevTimestamp)((r, c) => math.max(r, c))
      if (newTimeStamp - prevTimestamp > 60 * 1000) {
        Some(newTimeStamp, true)
      } else {
        Some(newTimeStamp, false)
      }
    }
  }

  val updateStateLost = (values: Seq[Int], state: Option[Int]) => {
    if (values.isEmpty) {
      val current = state.get
      if (current < 0) {
        None
      } else {
        Some(current - 1)
      }
    } else {
      Some(60)
    }
  }

  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.println("Your arguments were " + args.mkString("[", ", ", "]"))
      System.err.println(
        """
          |Usage: ServerLogsStreaming <hostname> <port> <checkpoint-directory>
          |     <output-file>. <hostname> and <port> describe the TCP server that Spark
          |     Streaming would connect to receive data. <checkpoint-directory> directory to
          |     local (file:/) or HDFS-compatible (hdfs:/) file system.
          |     <output-file> should be in local file system
          |
          |
          |In local mode, <master> should be 'local[n]' with n > 1
          |Both <checkpoint-directory> and <output-file> must be absolute paths
        """.stripMargin
      )
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("ServerLogsStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.checkpoint(args(2))
    val outputFile = new File(args(3))

    val logs = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    val logParams = logs.flatMap(str => createLogRecord(str))
    val hostPairs = logParams.map(rec => (rec.host, 1))
    val codeFiltered = logParams.filter(rec => rec.code < 200 || rec.code >= 300)
    val codePairs = codeFiltered.map(rec => (rec.host, 1))
    val hostCounts = hostPairs.reduceByKeyAndWindow((a:Int,b:Int) => (a + b), Seconds(60), Seconds(10))
    val codeCounts = codePairs.reduceByKeyAndWindow((a:Int,b:Int) => (a + b), Seconds(60), Seconds(60))
    val host1mAvg = hostCounts.map(x => (x._1, x._2 / 60.0))
    val host1mWrongCode = codeCounts.filter(x => x._2 >= 2)

    host1mAvg.foreachRDD({ (rdd: RDD[(String, Double)], time: Time) =>
      val outputArr = rdd.collect()
      if (outputArr.length > 0) {
        for (host <- outputArr) {
          val output = createOutputMessage(f"${host._1}: ${host._2}%.3f RPS")
          Files.append(output, outputFile, Charset.defaultCharset())
        }
      } else {
        val output = createOutputMessage("There are no hosts to aggregate")
        Files.append(output, outputFile, Charset.defaultCharset())
      }
    })
    host1mWrongCode.foreachRDD({ (rdd: RDD[(String, Int)], time: Time) =>
      val outputArr = rdd.collect()
      for (host <- outputArr) {
        val output = createOutputMessage(f"Host ${host._1} received ${host._2} non-200 codes.")
        Files.append(output, outputFile, Charset.defaultCharset())
      }
    })

    val hostActivityNew = logParams.map(rec => (rec.host, System.currentTimeMillis))
    val hostStateNew = hostActivityNew.updateStateByKey(updateStateNew)
    val newHosts = hostStateNew.filter(rec => rec._2._2 == true)
    newHosts.foreachRDD({ (rdd: RDD[(String, (Long, Boolean))], time: Time) =>
      val outputArr = rdd.collect()
      for (host <- outputArr) {
        val output = createOutputMessage(f"Found new host: ${host._1}. Welcome!")
        Files.append(output, outputFile, Charset.defaultCharset())
      }
    })

    val hostActivityLost = logParams.map(rec => (rec.host, 1))
    val hostStateLost = hostActivityLost.updateStateByKey(updateStateLost)
    val lostHosts = hostStateLost.filter(rec => rec._2 == 0)
    lostHosts.foreachRDD({ (rdd: RDD[(String, Int)], time: Time) =>
      val outputArr = rdd.collect()
      for (host <- outputArr) {
        val output = createOutputMessage(f"Host ${host._1} lost connection. Remove it from aggregation.")
        Files.append(output, outputFile, Charset.defaultCharset())
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}

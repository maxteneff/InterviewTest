import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._

import scala.util.Try

case class LogRecord(host: String, source: String, timestamp: String, url: String, code: Int)

object ServerLogsStreaming {
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
      Some((values.foldLeft(state.get._1)((r, c) => math.max(r, c)), false))
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
    if (args.length < 2) {
      System.err.println("Usage: ServerLogsStreaming <hostname> <port>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("ServerLogsStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.checkpoint("file:/tmp")

    val logs = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    val logParams = logs.flatMap(str => createLogRecord(str))
    val hostPairs = logParams.map(rec => (rec.host, 1))
    val codeFiltered = logParams.filter(rec => rec.code < 200 || rec.code >= 300)
    val codePairs = codeFiltered.map(rec => (rec.host, 1))
    val hostCounts = hostPairs.reduceByKeyAndWindow((a:Int,b:Int) => (a + b), Seconds(60), Seconds(10))
    val codeCounts = codePairs.reduceByKeyAndWindow((a:Int,b:Int) => (a + b), Seconds(60), Seconds(60))
    val host1mAvg = hostCounts.map(x => (x._1, x._2 / 60.0))
    val host1mWrongCode = codeCounts.filter(x => x._2 >= 2)
    //    host1mAvg.print()
//    host1mWrongCode.print()

    val hostActivityNew = logParams.map(rec => (rec.host, System.currentTimeMillis))
    val hostStateNew = hostActivityNew.updateStateByKey(updateStateNew)
    val newHosts = hostStateNew.filter(rec => rec._2._2 == true)
    newHosts.print()

    val hostActivityLost = logParams.map(rec => (rec.host, 1))
    val hostStateLost = hostActivityLost.updateStateByKey(updateStateLost)
    val lostHosts = hostStateLost.filter(rec => rec._2 == 0)
    lostHosts.print()

    //val lostHosts = hostStateNew.filter(pair => System.currentTimeMillis() - pair._2._1 > 60 * 1000)
//    lostHosts.print()
//    hostMinAvg.saveAsTextFiles("hostCount")
//    codeCounts.saveAsTextFiles("codeCounts")
    ssc.start()
    ssc.awaitTermination()
  }
}

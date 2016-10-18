import com.holdenkarau.spark.testing.StreamingSuiteBase
import org.apache.spark.streaming.{Duration, Seconds}

/**
  * Created by user on 18.10.16.
  */
class StreamingTest extends StreamingSuiteBase {

  // Batch duration
  override def batchDuration: Duration = Seconds(1)

  // Whether to use manual clock or not
  override def useManualClock: Boolean = true

  // Whether to actually wait in real time before changing manual clock
//  override def actuallyWait: Boolean = true

  test("test processing input records") {
    //OK batch
    val inputBatch1 = List( "host-1 81.73.150.239 2013-10-10T04:02:21.000 \"/handle-bars\" 101",
                            "host-2 123.221.14.56 2013-10-10T03:14:19.000 \"/forks\" 200",
                            "host-3 233.192.62.103 2013-10-10T03:18:25.000 \"/store\" 400")
    //wrong input
    val inputBatch2 = List("Wrong input")
    // empty batch
    val inputBatch3 = null
    // wrong (non-int) code
    val inputBatch4 = List( "host-1 81.73.150.239 2013-10-10T04:02:21.000 \"/handle-bars\" ab200") //wrong code
    val input = List(inputBatch1, inputBatch2, inputBatch3, inputBatch4)

    val exBatch1 = List(LogRecord("host-1", "81.73.150.239", "2013-10-10T04:02:21.000", "\"/handle-bars\"", 101),
                        LogRecord("host-2", "123.221.14.56", "2013-10-10T03:14:19.000", "\"/forks\"", 200),
                        LogRecord("host-3", "233.192.62.103", "2013-10-10T03:18:25.000", "\"/store\"", 400))
    //wrong input
    val exBatch2 = List()
    // wrong (non-int) code
    val exBatch3 = List()
    val expected = List(exBatch1, exBatch2, exBatch3)
    testOperation[String, LogRecord](input,  ServerLogsStreaming.createRecordStream _, expected, ordered = true)
  }

}

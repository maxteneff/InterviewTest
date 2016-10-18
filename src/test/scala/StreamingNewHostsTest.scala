import com.holdenkarau.spark.testing.StreamingSuiteBase

class StreamingNewHostsTest extends StreamingSuiteBase {

    override def actuallyWait: Boolean = true
    test("test checking new hosts") {
      val batch = List(
        LogRecord("host-1", "81.73.150.239", "2013-10-10T04:02:21.000", "\"/handle-bars\"", 201),
        LogRecord("host-2", "123.221.14.56", "2013-10-10T03:14:19.000", "\"/forks\"", 200),
        LogRecord("host-3", "233.192.62.103", "2013-10-10T03:18:25.000", "\"/store\"", 202)
      )
      val batch2 = List(
        LogRecord("host-4", "81.73.150.239", "2013-10-10T04:02:21.000", "\"/handle-bars\"", 201),
        LogRecord("host-5", "123.221.14.56", "2013-10-10T03:14:19.000", "\"/forks\"", 200),
        LogRecord("host-6", "233.192.62.103", "2013-10-10T03:18:25.000", "\"/store\"", 202)
      )

      val input = List(batch) ++ List(batch2) ++ List.fill(60)(null) ++ List(batch)
      val expectedOutput = List(
        List(("host-1", true), ("host-2", true), ("host-3", true)),
        List(("host-4", true), ("host-5", true), ("host-6", true))
      ) ++ List.fill(60)(List()) ++ List(
        List(("host-1", true), ("host-2", true), ("host-3", true))
      )

      testOperation[LogRecord, (String, Boolean)](
        input,
        ServerLogsStreaming.createNewHostsStream _,
        expectedOutput,
        ordered = false
      )
    }

}

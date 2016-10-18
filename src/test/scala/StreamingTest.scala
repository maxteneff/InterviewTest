import com.holdenkarau.spark.testing.StreamingSuiteBase

class StreamingTest extends StreamingSuiteBase {

  test("test processing input records") {
    //OK batch
    val inputBatch1 = List(
      "host-1 81.73.150.239 2013-10-10T04:02:21.000 \"/handle-bars\" 101",
      "host-2 123.221.14.56 2013-10-10T03:14:19.000 \"/forks\" 200",
      "host-3 233.192.62.103 2013-10-10T03:18:25.000 \"/store\" 400"
    )
    //wrong input
    val inputBatch2 = List("Wrong input")
    // empty batch
    val inputBatch3 = null
    // wrong (non-int) code
    val inputBatch4 = List( "host-1 81.73.150.239 2013-10-10T04:02:21.000 \"/handle-bars\" ab200")
    val input = List(inputBatch1, inputBatch2, inputBatch3, inputBatch4)

    //OK batch
    val exBatch1 = List(
      LogRecord("host-1", "81.73.150.239", "2013-10-10T04:02:21.000", "\"/handle-bars\"", 101),
      LogRecord("host-2", "123.221.14.56", "2013-10-10T03:14:19.000", "\"/forks\"", 200),
      LogRecord("host-3", "233.192.62.103", "2013-10-10T03:18:25.000", "\"/store\"", 400)
    )
    //wrong input
    val exBatch2 = List()
    // wrong (non-int) code
    val exBatch3 = List()
    val expected = List(exBatch1, exBatch2, exBatch3)
    testOperation[String, LogRecord](
      input,
      ServerLogsStreaming.createRecordStream _,
      expected,
      ordered = false
    )
  }

  test("test counting average rps every 10 seconds") {
    val host1Batch = List(
      LogRecord("host-1", "81.73.150.239", "2013-10-10T04:02:21.000", "\"/handle-bars\"", 101),
      LogRecord("host-1", "123.221.14.56", "2013-10-10T03:14:19.000", "\"/forks\"", 200),
      LogRecord("host-1", "233.192.62.103", "2013-10-10T03:18:25.000", "\"/store\"", 400)
    )

    val host1Input =  List.fill(10)(host1Batch)
    val expectedOutput10Sec = List(List(("host-1", 0.5)))
    testOperation[LogRecord, (String, Double)](
      host1Input,
      ServerLogsStreaming.createHost1mAvgStream _,
      expectedOutput10Sec,
      ordered = false
    )

    val host2Batch = List(
      LogRecord("host-2", "81.73.150.239", "2013-10-10T04:02:21.000", "\"/handle-bars\"", 101),
      LogRecord("host-2", "123.221.14.56", "2013-10-10T03:14:19.000", "\"/forks\"", 200),
      LogRecord("host-2", "233.192.62.103", "2013-10-10T03:18:25.000", "\"/store\"", 400)
    )
    val host2Input =  host1Input ++ List.fill(10)(host2Batch)
    val expectedOutput2OSec = List(List(("host-1", 0.5)), List(("host-1", 0.5), ("host-2", 0.5)))
    testOperation[LogRecord, (String, Double)](
      host2Input,
      ServerLogsStreaming.createHost1mAvgStream _,
      expectedOutput2OSec,
      ordered = false
    )

    val finalInput = host2Input ++ List.fill(50)(null)
    val expectedOutput7OSec = List(
      List(("host-1", 0.5)),  //after 10 sec
      List(("host-1", 0.5), ("host-2", 0.5)), //after 20 sec
      List(("host-1", 0.5), ("host-2", 0.5)), //after 30 sec
      List(("host-1", 0.5), ("host-2", 0.5)), //after 40 sec
      List(("host-1", 0.5), ("host-2", 0.5)), //after 50 sec
      List(("host-1", 0.5), ("host-2", 0.5)), //after 60 sec
      List(("host-2", 0.5))   //after 70 sec
    )
    testOperation[LogRecord, (String, Double)](
      finalInput,
      ServerLogsStreaming.createHost1mAvgStream _,
      expectedOutput7OSec,
      ordered = false
    )
  }

  test("test counting non-200 codes per host every 60 seconds") {
    //Only 2xx codes
    val successBatch = List(
      LogRecord("host-1", "81.73.150.239", "2013-10-10T04:02:21.000", "\"/handle-bars\"", 201),
      LogRecord("host-2", "123.221.14.56", "2013-10-10T03:14:19.000", "\"/forks\"", 200),
      LogRecord("host-3", "233.192.62.103", "2013-10-10T03:18:25.000", "\"/store\"", 202)
    )

    val input60sec =  List.fill(60)(successBatch)
    val expectedOutput60Sec = List(List())
    testOperation[LogRecord, (String, Int)](
      input60sec,
      ServerLogsStreaming.createHost1mWrongCodeStream _,
      expectedOutput60Sec,
      ordered = false
    )

    //Without 2xx codes
    val failBatch = List(
      LogRecord("host-1", "81.73.150.239", "2013-10-10T04:02:21.000", "\"/handle-bars\"", 101),
      LogRecord("host-2", "123.221.14.56", "2013-10-10T03:14:19.000", "\"/forks\"", 300),
      LogRecord("host-3", "233.192.62.103", "2013-10-10T03:18:25.000", "\"/store\"", 404)
    )

    val input120sec =  input60sec ++ List.fill(30)(successBatch) ++ List.fill(30)(failBatch)
    val expectedOutput120Sec = List(
      List(), // after 60 sec
      List(("host-1", 30), ("host-2", 30), ("host-3", 30)) //after 120 sec
    )
    testOperation[LogRecord, (String, Int)](
      input120sec,
      ServerLogsStreaming.createHost1mWrongCodeStream _,
      expectedOutput120Sec,
      ordered = false
    )

    val input180sec =  input120sec ++ List.fill(59)(successBatch) ++ List.fill(1)(failBatch)
    val expectedOutput180Sec = List(
      List(), //after 60 sec
      List(("host-1", 30), ("host-2", 30), ("host-3", 30)), //after 120 sec
      List() //after 180 sec
    )
    testOperation[LogRecord, (String, Int)](
      input180sec,
      ServerLogsStreaming.createHost1mWrongCodeStream _,
      expectedOutput180Sec,
      ordered = false
    )
  }

  test("test checking lost hosts") {
    val batch = List(
      LogRecord("host-1", "81.73.150.239", "2013-10-10T04:02:21.000", "\"/handle-bars\"", 201),
      LogRecord("host-2", "123.221.14.56", "2013-10-10T03:14:19.000", "\"/forks\"", 200),
      LogRecord("host-3", "233.192.62.103", "2013-10-10T03:18:25.000", "\"/store\"", 202)
    )

    val input = List(null) ++ List(batch) ++ List.fill(60)(null)
    val expectedOutput = List() ++ List.fill(60)(List()) ++ List(
      List(("host-1", 0), ("host-2", 0), ("host-3", 0))
    )

    testOperation[LogRecord, (String, Int)](
      input,
      ServerLogsStreaming.createLostHostsStream _,
      expectedOutput,
      ordered = false
    )
  }
}

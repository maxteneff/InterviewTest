import com.holdenkarau.spark.testing.StreamingSuiteBase
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by user on 18.10.16.
  */
class StreamingTest extends StreamingSuiteBase {

  test("") {
    val input = List(List("hi"), List("hi holden"), List("bye"))
    val expected = List(List("hi"), List("hi", "holden"), List("bye"))
    testOperation[String, String](input, tokenize _, expected, ordered = false)
  }

  // This is the sample operation we are testing
  def tokenize(f: DStream[String]): DStream[String] = {
    f.flatMap(_.split(" "))
  }

}

package part2datastreams

import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._

object EssentialStreams {

  def applicationTemplate(): Unit = {
    // 1 - execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // in between, add any sort of computations
    //    import org.apache.flink.streaming.api.scala._ // import TypeInformation for the data of your DataStreams
    val simpleNumberStream: DataStream[Int] = env.fromElements(1, 2, 3)

    // perform some actions
    simpleNumberStream.print()

    // at the end
    env.execute() // trigger all the computations that were described earlier
  }

  // transformations
  def demoTransformations(): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val numbers: DataStream[Int] = env.fromElements(1, 2, 3, 4, 5)

    // checking parallelism
    println(s"Current parallelism: ${env.getParallelism}")

    // set different parallelism
    env.setParallelism(2)
    println(s"New parallelism: ${env.getParallelism}")

    // map
    val doubledNumbers = numbers.map(_ * 2)

    // flatMap
    val expandedNumbers = numbers.flatMap(n => List(n, n + 1))

    // filter
    val filteredNumbers = numbers.filter(_ % 2 == 0)
      /* you can set parallelism here */ .setParallelism(4)

    val finalData = expandedNumbers.writeAsText("output/expandedStream.txt") // directory with 8 files
    // set parallelism in the sink
    finalData.setParallelism(3)

    env.execute()
  }

  /**
   *
   * Exercise: FizzBuzz on Flink
   *  - take a stream of 100 natural numbers
   *  - for every number
   *    - if n % 3 == 0 then return "fizz"
   *    - if n % 5 == 0 => "buzz"
   *    - if both => "fizzbuzz"
   *
   *  - print the numbers for which you said "fizzbuzz" to a file
   */

  def fizzBuzz() = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val simpleNumberStream: DataStream[Int] = env.fromCollection(1 to 100)

    val fb = simpleNumberStream
      .map(n => {
        if ((n % 3 == 0) && (n % 5 == 0)) (n, "fizzbuzz")
        else if (n % 3 == 0) (n, "fizz")
        else if (n % 5 == 0) (n, "buzz")
        else (n, "")
      })
      .filter(_._2 == "fizzbuzz")
      .map(_._1)

    // alternative to
    // fb.writeAsText("output/fizzbuzz.txt").setParallelism(1)

    // add a SINK
    fb.addSink(
      StreamingFileSink
        .forRowFormat(
          new Path("output/streaming_sink"),
          new SimpleStringEncoder[Int]("UTF-8")
        )
        .build()
    ).setParallelism(1)

    env.execute()

  }


  def main(args: Array[String]): Unit = {
    fizzBuzz()
  }

}

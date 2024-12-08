package part3state

import generators.shopping.{AddToShoppingCartEvent, SingleShoppingCartEventsGenerator}
import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction, RichFlatMapFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object RichFunctions {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)

  val numbersStream: DataStream[Int] = env.fromElements(1, 2, 3, 4, 5, 6)

  // Pure FP
  val tenxNumbers: DataStream[Int] = numbersStream.map(_ * 10)

  // "explicit" map functions
  val tenxNumbers_v2: DataStream[Int] = numbersStream.map(new MapFunction[Int, Int] {
    override def map(value: Int): Int = value * 10
  })

  // Rich Map function
  val tenxNumbers_v3: DataStream[Int] = numbersStream.map(new RichMapFunction[Int, Int] {
    override def map(value: Int): Int = value * 10
  })

  // Rich map function + lifecycle methods
  val tenxNumbersWithLifecycle: DataStream[Int] = numbersStream.map(new RichMapFunction[Int, Int] {
    override def map(value: Int): Int = value * 10 // mandatory override

    // optional overrides: lifecycle methods (open/close)
    // called BEFORE data goes through
    override def open(parameters: Configuration): Unit =
      println("Starting my work!!")

    // invoked AFTER all the data
    override def close(): Unit =
      println("Finishing my work...")
  })

  // ProcessFunction - the most general function abstraction in Flink
  val tenxNumbersProcess: DataStream[Int] = numbersStream.process(new ProcessFunction[Int, Int] {
    override def processElement(value: Int, ctx: ProcessFunction[Int, Int]#Context, out: Collector[Int]): Unit =
      out.collect(value * 10)

    // can also override the lifecycle methods
    override def open(parameters: Configuration): Unit =
      println("Process function starting")

    override def close(): Unit =
      println("Closing process function")
  })

  /**
   * Exercise: "explode" all purchase events to a single item
   * [("boots", 2), (iPhone, 1)] ->
   * ["boots", "boots", "iPhone"]
   * - lambdas
   * - explicit functions
   * - rich functions
   * - process functions
   */
  def exercise(): Unit = {
    val exerciseEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val shoppingCartStream: DataStream[AddToShoppingCartEvent] = exerciseEnv.addSource(new SingleShoppingCartEventsGenerator(100)) // ~10 events/s
      .filter(_.isInstanceOf[AddToShoppingCartEvent])
      .map(_.asInstanceOf[AddToShoppingCartEvent])

    // 1 - lambdas: flatMap
    val itemsPurchasedStream: DataStream[String] =
      shoppingCartStream
        .flatMap(event => (1 to event.quantity).map(_ => event.sku))

    // 2 - explicit flatMap functions
    val itemsPurchasedStream_V2: DataStream[String] =
      shoppingCartStream.flatMap(new FlatMapFunction[AddToShoppingCartEvent, String] {
        override def flatMap(value: AddToShoppingCartEvent, out: Collector[String]): Unit =
          (1 to value.quantity).foreach(_ => out.collect(value.sku))
      })

    // 3 - rich flatMap functions
    val itemsPurchasedStream_V3: DataStream[String] =
      shoppingCartStream.flatMap(new RichFlatMapFunction[AddToShoppingCartEvent, String] {
        override def flatMap(value: AddToShoppingCartEvent, out: Collector[String]): Unit =
          (1 to value.quantity).foreach(_ => out.collect(value.sku))

        override def open(parameters: Configuration): Unit =
          println("Processing with rich flatMap function")

        override def close(): Unit =
          println("Finishing rich flatMap function")
      })

    // 4 - process function
    val itemsPurchasedStream_V4: DataStream[String] =
      shoppingCartStream.process(new ProcessFunction[AddToShoppingCartEvent, String] {
        override def processElement(value: AddToShoppingCartEvent, ctx: ProcessFunction[AddToShoppingCartEvent, String]#Context, out: Collector[String]): Unit =
          (1 to value.quantity).foreach(_ => out.collect(value.sku))
      })

    itemsPurchasedStream_V3.print()

    exerciseEnv.execute()
  }


  def main(args: Array[String]): Unit = {
    exercise()
  }

}

package sourcesandsinks.examples

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SQLContext}
import utils.SparkSetup

/*  Create a Rate source sending 100 events per second and a console Sink that shows 10 records of the output every 10 seconds
    in append outputMode */
object Example1 extends App with SparkSetup {
  val source = spark.readStream
    .format("rate")
    .option("rowsPerSecond", 100)
    .load()

  source.writeStream
    .format("console")
    .option("numRows", "10")
    .option("truncate", "false")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .start

  spark.streams.awaitAnyTermination()
}

/* Create a MemoryStream Source that sends object of type [Product] that contains as attributes
category(string), gender(string), size(string), price(Double).
In this case, send 3 elements and print them in the console
The name of the query has to be 'Products'
 */

case class Product(category: String, gender: String, size: String, price: Double)

object Example2 extends App with SparkSetup {

  implicit val sqlContext: SQLContext = spark.sqlContext

  import sqlContext.implicits._
  val inputStream = MemoryStream[Product]
  inputStream.addData(Product("category1", "gender1", "size1", 1))
  inputStream.addData(Product("category2", "gender2", "size2", 2))
  inputStream.addData(Product("category3", "gender3", "size3", 3))

  inputStream.toDF.writeStream
    .format("console")
    .option("numRows", "10")
    .option("truncate", "false")
    //.trigger(Trigger.Once())
    .trigger(Trigger.AvailableNow())
    .start

  spark.streams.awaitAnyTermination()

}

/*  Create a MemoryStream Source that sends object of type [Product] that contains as attributes
category(string), gender(string), size(string), price(Double).
In this case, send 3 elements and store them in a Memory Sink, the name of the table has to be 'products'

After 10 seconds(timeout), show all the products available and also a count
 */

object Example3 extends App with SparkSetup {
  implicit val sqlContext: SQLContext = spark.sqlContext

  import sqlContext.implicits._
  val inputStream = MemoryStream[Product]
  inputStream.addData(Product("category1", "gender1", "size1", 1))
  inputStream.addData(Product("category2", "gender2", "size2", 2))
  inputStream.addData(Product("category3", "gender3", "size3", 3))

  val process = inputStream.toDF.writeStream
    .format("memory")
    .queryName("product")
    .start()

  process.awaitTermination(10 * 1000)
  val productDF = spark.sql("select * from product") //aqui ya nos salimos de streaming y podemos usar spark normal
  productDF.show
  println(s"count: ${productDF.count}")
}

/*  Create a MemoryStream Source that sends object of type [Product] that contains as attributes
category(string), gender(string), size(string), price(Double).
In this case, send 3 elements and store them in a Memory Sink, the name of the table has to be 'products'

 Now, without timeout, show all that are available and a count */

object Example4 extends App with SparkSetup {
  implicit val sqlContext: SQLContext = spark.sqlContext

  import sqlContext.implicits._
  val inputStream = MemoryStream[Product]
  inputStream.addData(Product("category1", "gender1", "size1", 1))
  inputStream.addData(Product("category2", "gender2", "size2", 2))
  inputStream.addData(Product("category3", "gender3", "size3", 3))

  val process = inputStream.toDF.writeStream
    .format("memory")
    .queryName("product")
    .start()
    .processAllAvailable() //Con esto procesamos toda la info disponible

  val productDF = spark.sql("select * from product")
  productDF.show
  println(s"count: ${productDF.count}")
}

/*  Create a socket source where you send data in json-string format like this : {"company": "databricks", "employees":200, "date": "2021-04-03 16:00:00"}
    and print the output on the console with append mode
 */

//El socket para escribir se abre con `nc -l -p 8083`

object Example5 extends App with SparkSetup {
  implicit val sqlContext: SQLContext = spark.sqlContext

  val inputStream = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", "8083")
    .load()

  val process = inputStream.toDF.writeStream
    .format("console")
    .option("truncate", "false")
    .option("numRows", "1")
    .start

  process.awaitTermination
}

/*  Create a socket source where you send data in json-string format like this : {"company": "databricks", "employees":200, "date": "2021-04-03 16:00:00"}
    and print the output on the console with update mode and trigger the query every 5 seconds
 */

object Example6 extends App with SparkSetup {
  implicit val sqlContext: SQLContext = spark.sqlContext

  val inputStream = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", "8083")
    .load()

  val process = inputStream.toDF.writeStream
    .format("console")
    .option("truncate", "false")
    .outputMode("update")
    .trigger(Trigger.ProcessingTime("5 seconds"))
    .start

  process.awaitTermination

}

/*  Create a file source where you read data in json format like this : {"key":"mango","value":1}
    and print the output on the console with append mode
 */

object Example7 extends App with SparkSetup {
  implicit val sqlContext: SQLContext = spark.sqlContext

  val inputStream = spark.readStream
    .schema("key STRING, value INT")
    .json("src/main/resources/data/sourcesandsinks/json")

  val process = inputStream.toDF.writeStream
    .format("console")
    .option("truncate", "false")
    .outputMode("append")
    .start

  process.awaitTermination
}

/*  Create a file source where you read data in json format like this : {"key":"mango","value":1}
    and then use foreachBatch sink
    Básicamente, permite aplicar operaciones a cada lote de datos en el stream como si fuera un DataFrame estático, y es especialmente útil cuando deseas realizar operaciones de procesamiento adicional, como actualizaciones de estado o escrituras en bases de datos externas, que no son fácilmente realizables con las operaciones estándar de streaming.
 */

object Example8 extends App with SparkSetup {
  import org.apache.spark.sql.functions.lit

  val inputStream = spark.readStream
    .schema("key STRING, value INT")
    .json("src/main/resources/data/sourcesandsinks/json")

  val process = inputStream.toDF.writeStream
    .foreachBatch((ds: DataFrame, id: Long) =>
      ds //Este contenido es batch asi que puedo meter aqui toda mi logica y luego meter un trigger para migrar a Streaming
        .withColumn("columnaPatata", lit("patataNueva"))
        .show()
    )
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .start

  process.awaitTermination
}

/*
Create a rate source that writes to a table called "rate_table", after, create another source that reads
 from the table "rate_table" and print in the screen.
 */

object Example9 extends App with SparkSetup {
  val sourceRate = spark.readStream
    .format("rate")
    .option("rowsPerSecond", 100)
    .load()

  sourceRate.writeStream
    .option("checkpointLocation", "/tmp/check/")
    .option("path", "/tmp/table/")
    .toTable("rate_table")

  val sourceTable = spark.readStream
    .option("checkpointLocation", "/tmp/check/")
    .table("rate_table")

  sourceTable.toDF.writeStream
    .format("console")
    .option("truncate", "false")
    .outputMode("append")
    .start

  spark.streams.awaitAnyTermination
}

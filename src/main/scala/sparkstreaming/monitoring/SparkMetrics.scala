package monitoring

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQueryListener

trait SparkMetrics extends Logging {

  implicit val spark: SparkSession

  def metrics(): Unit = {
    spark.streams.addListener(new StreamingQueryListener {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit =
        println(s"QueryStarted [id = ${event.id}, name = ${event.name}, runId = ${event.runId}]")

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit =
        println(s"QueryProgress ${event.progress}")

      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit =
        println(s"QueryTerminated [id = ${event.id}, runId = ${event.runId}, error = ${event.exception}]")
    })
  }

}
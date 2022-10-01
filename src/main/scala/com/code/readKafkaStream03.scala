package main.scala.com.code

import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Schema, Slide, Table, TableDescriptor, TableEnvironment, Tumble, TumbleWithSize}
import org.apache.flink.table.api.Expressions.{$, lit}
import org.apache.flink.table.expressions.ExpressionParserImpl.PROCTIME

object readKafkaStream03 extends  App {

  val config = new Configuration();

  val settings = EnvironmentSettings
    .newInstance()
    .withConfiguration(config)
    .inStreamingMode()
    // .inBatchMode()
    .build()

  // Sliding window example with processing timestamp as window field

  val tEnv: TableEnvironment = TableEnvironment.create(settings);


  val tEnv_config = tEnv.getConfig();

  tEnv_config.set("table.exec.source.idle-timeout", "1000 ms");
  tEnv_config.set("table.exec.state.ttl", "1000 ms");
  tEnv_config.set("table.exec.mini-batch.enabled", "true");
  tEnv_config.set("table.exec.mini-batch.size", "1");
  tEnv_config.set("table.exec.mini-batch.allow-latency", "1000 ms");

  // tEnv.config.configuration.setString("table.exec.source.idle-timeout", "5000 ms")

  //Example for Event time based windowing

  val schema = Schema.newBuilder()
  schema.column("id", DataTypes.INT())
  schema.column("type", DataTypes.STRING())
  schema.column("amount", DataTypes.FLOAT())
  schema.column("trx_timestamp", DataTypes.TIMESTAMP(3))
  schema.columnByExpression("procs_timestamp","PROCTIME()")
  //schema.watermark("trx_timestamp", "trx_timestamp - INTERVAL '10' MINUTE")


  tEnv.createTemporaryTable("kafka_stream_input2", TableDescriptor.forConnector("kafka")
    .schema(schema.build())
    .format("csv")
    .option("topic", "test_producer01")
    .option("properties.bootstrap.servers", "localhost:9092")
    .option("properties.group.id", "flink-test")
    .option("scan.startup.mode", "earliest-offset")
    .build()
  )


  val Table1: Table = tEnv.from("kafka_stream_input2");

  // Creating the Window Results with Table API syntax
  val Table2 = Table1
    .window(Slide.over(lit(5).minutes()).every(lit(1).minutes()).on($("procs_timestamp")).as("w"))
    .groupBy($("type"), $("w"))
    .select($("type"), $("amount").sum().as("sum_amount"), $("w").start().as("window_start"), $("w").end().as("window_end")).execute().print()


}

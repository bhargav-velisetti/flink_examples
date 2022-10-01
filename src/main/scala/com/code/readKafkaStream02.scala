package com.code

import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Schema, Table, TableDescriptor, TableEnvironment}

object readKafkaStream02 extends App {

  val config = new Configuration();

  val settings = EnvironmentSettings
    .newInstance()
    .withConfiguration(config)
    .inStreamingMode()
    // .inBatchMode()
    .build()

  val tEnv : TableEnvironment = TableEnvironment.create(settings);

  val tEnv_config = tEnv.getConfig();

  tEnv_config.set("table.exec.source.idle-timeout", "1000 ms");
  tEnv_config.set("table.exec.state.ttl", "1000 ms");
  tEnv_config.set("table.exec.mini-batch.enabled", "true");
  tEnv_config.set("table.exec.mini-batch.size", "1");
  tEnv_config.set("table.exec.mini-batch.allow-latency", "1000 ms");

  // tEnv.config.configuration.setString("table.exec.source.idle-timeout", "5000 ms")

  val schema = Schema.newBuilder()
  schema.column("id", DataTypes.INT())
  schema.column("type", DataTypes.STRING())
  schema.column("amount", DataTypes.FLOAT())
  schema.column("trx_timestamp", DataTypes.TIMESTAMP(3))
  schema.watermark("trx_timestamp", "trx_timestamp - INTERVAL '1' MINUTE")

  tEnv.createTemporaryTable("kafka_stream_input",  TableDescriptor.forConnector("kafka")
    .schema(schema.build())
    .format("csv")
    .option("topic","test_producer01")
    .option("properties.bootstrap.servers","localhost:9092")
    .option("properties.group.id","flink-test")
    .option("scan.startup.mode" , "earliest-offset")
    .build()
  )

  tEnv.executeSql(
    """select type, sum(amount) as sum_ammount , window_start , window_end FROM TABLE
      |(
      |TUMBLE( DATA => TABLE kafka_stream_input,TIMECOL => DESCRIPTOR(trx_timestamp),SIZE => INTERVAL '10' MINUTES)
      |) GROUP BY type, window_start, window_end ;
      |""".stripMargin ).print()


  val temp_table : Table  = tEnv.sqlQuery(
  """select  type, sum(amount) as sum_ammount , window_start , window_end from TABLE
    |(
    |TUMBLE( DATA => TABLE kafka_stream_input,TIMECOL => DESCRIPTOR(trx_timestamp),SIZE => INTERVAL '10' MINUTES)
    |) GROUP BY type, window_start, window_end ;
    |""".stripMargin )

  tEnv.registerTable("temp_table",temp_table)

  temp_table.execute().print()





}

package com.code

import com.code.secondFlinkApp.schema
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Schema, TableDescriptor, TableEnvironment, TableResult}

object readKafkaStream01 extends App {

  val settings = EnvironmentSettings
    .newInstance()
    .inStreamingMode()
   // .inBatchMode()
    .build()

  val tEnv = TableEnvironment.create(settings);

  val schema = Schema.newBuilder()
  schema.column("messageId", DataTypes.INT())
  schema.column("messageData", DataTypes.STRING())


  tEnv.createTemporaryTable("kafka_stream_input",  TableDescriptor.forConnector("kafka")
      .schema(schema.build())
      .format("csv")
      .option("topic","test_producer01")
      .option("properties.bootstrap.servers","localhost:9092")
      .option("properties.group.id","flink-test")
      .option("scan.startup.mode" , "earliest-offset")
      .build()
  )


  var table1 = tEnv.executeSql("select * from kafka_stream_input").print()




}

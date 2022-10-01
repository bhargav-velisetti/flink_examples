package com.code

import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Schema, Table, TableDescriptor, TableEnvironment, TableResult}


object secondFlinkApp extends App {

  val settings = EnvironmentSettings
    .newInstance()
    // .inStreamingMode()
    .inBatchMode()
    .build()

  val tEnv = TableEnvironment.create(settings);

  val schema = Schema.newBuilder()
  schema.column("ship_name", DataTypes.STRING())
  schema.column("cruise_line", DataTypes.STRING())
  schema.column("age", DataTypes.INT())
  schema.column("tonnage", DataTypes.FLOAT())
  schema.column("passengers", DataTypes.FLOAT())
  schema.column("length", DataTypes.FLOAT())
  schema.column("cabins", DataTypes.FLOAT())
  schema.column("pc_density", DataTypes.FLOAT())
  schema.column("crew", DataTypes.FLOAT())


  tEnv.createTemporaryTable("TempShipTable",
    TableDescriptor.forConnector("filesystem").schema(schema.build()).format("csv").option("path", "data\\cruise_ship_info.csv").build()
  )

  val filtered_table: TableResult = tEnv.executeSql("select * from TempShipTable where age > 10")

  val filtered_table2: Table = tEnv.sqlQuery("select * from TempShipTable where age > 10")


  println("job client \n " + filtered_table.getJobClient().toString)

  println("table schema \n " + filtered_table.getTableSchema())

  println("resolved schema \n" + filtered_table.getResolvedSchema())

  println("result kind \n" + filtered_table.getResultKind())


  filtered_table.print()

  filtered_table2.select("age").execute().print()




}

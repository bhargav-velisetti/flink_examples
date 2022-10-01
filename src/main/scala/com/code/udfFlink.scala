package com.code

import com.code.secondFlinkApp.tEnv
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Schema, Table, TableDescriptor, TableEnvironment}
import org.apache.flink.table.functions.ScalarFunction

object udfFlink extends App {

  val settings = EnvironmentSettings
    .newInstance()
    // .inStreamingMode()
    .inBatchMode()
    .build()

  val tEnv = TableEnvironment.create(settings);

  class isOld extends ScalarFunction {
    def eval(s: Integer): String = {
      if (s > 10) {
        return "YES"
      } else
        {
         return "NO"
        }
   }
  }

 tEnv.createTemporaryFunction("isOld", classOf[isOld]);

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

  var table1 : Table = tEnv.sqlQuery("select age , isOld(age) as is_it_old from TempShipTable ")

  table1.printSchema()

  table1.execute.print()




}

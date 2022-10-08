package scala.com.code

import main.scala.com.code.buildSchema
import main.scala.com.code.secondFlinkApp.{schema, tEnv}
import org.apache.flink.table.api.{EnvironmentSettings, Schema, TableDescriptor, TableEnvironment}

object testjson extends  App {

  val input_schema : Schema =  buildSchema(os.pwd/"data"/"cruise_ship_schema.json" ).apply()

  val settings = EnvironmentSettings
    .newInstance()
    // .inStreamingMode()
    .inBatchMode()
    .build()

  val tEnv = TableEnvironment.create(settings);

  tEnv.createTemporaryTable("TempShipTable2",
    TableDescriptor.forConnector("filesystem").schema(input_schema).format("csv").option("path", "data\\cruise_ship_info.csv").build()
  )

  tEnv.sqlQuery("select * from TempShipTable2").execute().print()




}

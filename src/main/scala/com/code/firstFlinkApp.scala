package main.scala.com.code

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment, TableResult}
import scala.collection.convert.ImplicitConversions.`iterator asScala`

object firstFlinkApp extends App {

  val settings = EnvironmentSettings
    .newInstance()
    // .inStreamingMode()
    .inBatchMode()
    .build()

  val tEnv = TableEnvironment.create(settings)

  println(" added some ... ")

  // println(args(0))

  tEnv.executeSql(
    """
      | CREATE TABLE ship_info (
      | ship_name STRING,
      | cruise_line STRING,
      | age INTEGER,
      | tonnage FLOAT,
      | passengers FLOAT,
      | length FLOAT,
      | cabins FLOAT,
      | pc_density FLOAT,
      | crew FLOAT )
      | with (
      | 'connector' = 'filesystem',
      | 'path' = 'data\cruise_ship_info.csv',
      | 'format' = 'csv'
      |  )
    """.stripMargin);

  tEnv.executeSql(
    """
      | CREATE TABLE print_ship_info (
      | ship_name STRING,
      | cruise_line STRING,
      | age INTEGER,
      | tonnage FLOAT,
      | passengers FLOAT,
      | length FLOAT,
      | cabins FLOAT,
      | pc_density FLOAT,
      | crew FLOAT )
      | with (
      | 'connector' = 'print')
                                """.stripMargin);

  val result = tEnv.executeSql("insert into print_ship_info select * from ship_info")

  //result.print();

  val count = tEnv.executeSql("select count(*) from ship_info").collect.toSeq(0)
  val count2 = tEnv.executeSql("select count(*) from ship_info").collect().toList(0)

  println("count of CSV is from list  imple = " + count2.getField(0))

  // count.print();

  println("count of CSV is = " + count.getField(0))

  println(count.getField(0) == 158)
  println(count2.getField(0) == 158)


  tEnv.executeSql(
    """
      | CREATE TABLE sink_example (
      | ship_name STRING,
      | cruise_line STRING,
      | age INTEGER,
      | tonnage FLOAT,
      | passengers FLOAT,
      | length FLOAT,
      | cabins FLOAT,
      | pc_density FLOAT,
      | crew FLOAT )
      | with (
      | 'connector' = 'filesystem',
      | 'path' = 'data\sink_by_flink_',
         //          | 'name' = 'sink_by_flink.csv',
      | 'format' = 'csv'
      | )""".stripMargin);


  tEnv.executeSql("insert into sink_example select * from ship_info order by ship_name");


}

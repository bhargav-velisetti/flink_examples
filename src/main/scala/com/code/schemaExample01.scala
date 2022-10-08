package main.scala.com.code

import org.apache.flink.table.api.{DataTypes, Schema}
import org.apache.flink.table.types.{AbstractDataType, DataType}

object schemaExample01 extends  App{

  val schema : Schema.Builder = Schema.newBuilder()
  val sc : Array[String] = new Array[String](2)
  val sc2 : Array[AbstractDataType[_]] = new Array[AbstractDataType[_]](2)
  sc.update(0,"name")
  sc.update(1,"age")
  sc2.update(0,DataTypes.STRING())
  sc2.update(1,DataTypes.INT())

  schema.fromFields(sc,sc2)
  schema.build()








}

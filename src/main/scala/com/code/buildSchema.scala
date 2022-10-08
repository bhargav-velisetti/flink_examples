package main.scala.com.code

import org.apache.flink.table.api.{DataTypes, Schema}
import org.apache.flink.table.types.AbstractDataType


case class buildSchema(schemaFilePath : os.Path)  {

  def dType(col_type: String) = col_type match {
    case "STRING" => DataTypes.STRING()
    case "INTEGER" => DataTypes.INT()
    case "FLOAT" => DataTypes.FLOAT()
   // case _ => println(s"${col_type} does not found in given schema file")

  }

  def apply() : Schema = {
    val file = this.schemaFilePath;
    val jsonString = os.read(file)
    val data  = ujson.read(jsonString)
    val schema = Schema.newBuilder()
    for ( (i,j) <- data.obj.iterator ) {
      println(s"${i} ====> ${j}")
      schema.column(i,dType(j.str : String )  )
    }
    return schema.build()

 }

}


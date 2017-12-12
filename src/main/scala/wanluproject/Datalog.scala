package wanluproject

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalog

 class DatalogContext(program: String, sparkSession: SparkSession){


  def datalog(program: String) = {

     val datalogProgram = parse(program)

     val executor = new Executor(datalogProgram, sparkSession)

      executor.evaluate()

  }

  def parse(code: String): DatalogProgram = {
    DatalogParser(code) match{
      case Right(datalogProgram) => datalogProgram
    }
  }

 /* def registerRelation(name: String) = {
    sparkSession.catalog.listTables().toString

  } */



}



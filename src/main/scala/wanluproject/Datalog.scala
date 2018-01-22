package wanluproject

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalog

 class DatalogContext(program: String, sparkSession: SparkSession){


  def datalog(program: String, method: String) = {

     val datalogProgram = parse(program)

    val evaluator = new Evaluator(datalogProgram,sparkSession)

    method match{
      case naive => evaluator.naive()
      case semi_naive => evaluator.semi_naive()
    }

  }

  def parse(code: String): DatalogProgram = {
    DatalogParser(code) match{
      case Right(datalogProgram) => datalogProgram
    }
  }


}



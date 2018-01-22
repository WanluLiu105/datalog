package wanluproject

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession


object Util {


  def naturalJoin(left: DataFrame, right: DataFrame): DataFrame = {

    val leftCols = left.columns.toSet
    val rightCols = right.columns.toSet
    val commonCols = leftCols.intersect(rightCols).toSeq

    //println("join")
    if (commonCols.isEmpty) {
     // left.crossJoin(right).show()
      left.crossJoin(right)
    }
    else {
     // left.join(right, commonCols).show()
      left.join(right, commonCols)
    }

  }

  def giveAlias(p: Predicate, spark:SparkSession) ={
      val df = spark.table(p.name)
      df.toDF(p.argArray: _*)
  }

  def project(rule: Rule, df: DataFrame): DataFrame = {

    val cols: List[String] = rule.head.argArray.toList
    df.select(cols.head, cols.tail: _*)

  }

  def union(left: DataFrame, right: DataFrame): DataFrame ={

    val cols: Seq[String] = right.columns.toSeq
    val left1 = left.toDF(cols: _*)
    val result = left1.union(right)   // distinct?
    //result.show()
    result
  }

  def filter(query: Query, df: DataFrame): DataFrame = {

    var result = df
    var condition: Seq[(Int, String)] = query.constraint

    while (condition.isEmpty != true) {
      print("filter : " + condition)
      result = result.filter(result(result.columns(condition.head._1)) === condition.head._2)
      condition = condition.drop(1)
    }
    result
  }

  //def where(rule: Rule, df: DataFrame): DataFrame = { }


}


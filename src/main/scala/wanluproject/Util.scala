package wanluproject

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

object Util {


  def naturalJoin(left: DataFrame, right: DataFrame): DataFrame = {

    val leftCols = left.columns.toSet
    val rightCols = right.columns.toSet
    val commonCols = leftCols.intersect(rightCols).toSeq

    if (commonCols.isEmpty)
      left.crossJoin(right)
    else
      left.join(right, commonCols)

  }

 /* def giveAlias(p: Predicate): DataFrame = {
    //println("give alias"+ p.name)
    Database.relations.get(p.name) match {
      case Some(df) => df.toDF(p.argArray: _*)
    }
  }*/

  def giveAlias(p: Predicate, spark:SparkSession) ={
     val df = spark.table(p.name)
     df.toDF(p.argArray: _*)
  }

  def project(rule: Rule, df: DataFrame): DataFrame = {

    val cols: List[String] = rule.head.argArray.toList
    df.select(cols.head, cols.tail: _*)

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


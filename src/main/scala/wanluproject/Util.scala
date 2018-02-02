package wanluproject

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession


object Util {


  def naturalJoin(left: DataFrame, right: DataFrame): DataFrame = {

    val start = System.currentTimeMillis()
    val leftCols = left.columns.toSet
    val rightCols = right.columns.toSet
    val commonCols = leftCols.intersect(rightCols).toSeq

    //println("join")
    val result =
      if (commonCols.isEmpty) {
     // left.crossJoin(right).show()
      left.crossJoin(right)
    }
    else {
     // left.join(right, commonCols).show()
      left.join(right, commonCols)
    }
    println("join time:" + (System.currentTimeMillis()-start) )
    result
  }

  def giveAlias(p: Predicate, spark:SparkSession) ={
      val df = spark.table(p.name)
      df.toDF(p.argArray: _*)
  }

  def project(cols: List[String], df: DataFrame): DataFrame = {

    // check if the head varibles are bound in the body before execute
    // here must are common cols
    val start = System.currentTimeMillis()
    val result = df.select(cols.head, cols.tail: _*)
    println("project time:" + (System.currentTimeMillis() - start))
    result

  }

  def union(left: DataFrame, right: DataFrame): DataFrame ={

    val start = System.currentTimeMillis()
    val cols: Seq[String] = right.columns.toSeq
    val left1 = left.toDF(cols: _*)
    val result = left1.union(right)   // distinct?
    println("union time:" + (System.currentTimeMillis() - start))
    result
  }

 /* def filter(query: Query, df: DataFrame): DataFrame = {

    var result = df
    var condition: Seq[(Int, String)] = query.constraint

    while (condition.isEmpty != true) {
      print("filter : " + condition)
      result = result.filter(result(result.columns(condition.head._1)) === condition.head._2)
      condition = condition.drop(1)
    }
    result
  }*/

  def select(conditions: Seq[Expr], df: DataFrame): DataFrame = {
    val start = System.currentTimeMillis()
    val cons = conditions.map(_.asInstanceOf[Condition])
    var result = df

    for(c <- cons){
      val lhs = c.lhs.value
      c.rhs match{
        case Variable(x) =>
          val rhs = c.rhs.asInstanceOf[Variable].value
          result =
          c.op match{
            case "==" => df.filter(df(lhs)===df(rhs))
            case ">=" => df.filter(df(lhs)>=df(rhs))
            case "<=" => df.filter(df(lhs)<=df(rhs))
            case ">" => df.filter(df(lhs)>df(rhs))
            case "<" => df.filter(df(lhs)<df(rhs))
            case "!=" => df.filter(df(lhs)=!=df(rhs))
          }
        case Constant(a) =>
          val rhs = c.rhs.asInstanceOf[Constant].value
          result =
            c.op match{
              case "==" => df.filter(df(lhs)===rhs)
              case ">=" => df.filter(df(lhs)>=rhs)
              case "<=" => df.filter(df(lhs)<=rhs)
              case ">" => df.filter(df(lhs)>rhs)
              case "<" => df.filter(df(lhs)<rhs)
              case "!=" => df.filter(df(lhs)=!=rhs)
            }
      }
    }
    println("select time:" + (System.currentTimeMillis() - start))
     result
  }


}


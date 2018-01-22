package wanluproject


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.broadcast

class Executor(datalogProgram: DatalogProgram, spark: SparkSession) {

 /* def loadFact() = {
    if (datalogProgram.hasFact == true) {
      // println("hasFact")
      val name: Seq[String] = datalogProgram.factList.map(_._1).toSeq
      val schema: Seq[StructType] = datalogProgram.factList.map(p => p._2.head._2).toSeq
      val value: Seq[RDD[Row]] = datalogProgram.factList.map(p => spark.sparkContext.parallelize(p._2.map(_._3))).toSeq
      for (i <- 0 until name.length) {
        var df: DataFrame = spark.createDataFrame(value(i), schema(i))
        if (Database.relationList.contains(name(i))) {
          val df0 = spark.table(name(i))
          df = df.union(df0)
          df.createOrReplaceTempView(name(i))
        }
        df.createOrReplaceTempView(name(i))
        broadcast(df)
      }

    }
  }*/


 // val hasfilter: Boolean = datalogProgram.qr.hasConstraint

  /*def evaluate(): Unit = {

    loadFact()

   // spark.catalog.listTables().show()

   // datalogProgram.order

    var ruleStack = datalogProgram.ruleOrder

    println("ruleStack" + ruleStack)

    while (ruleStack.isEmpty != true) {
      val n = ruleStack.head._2
      val isRecursive = ruleStack.head._3 match {
        case "recursive" => true
        case "base" => false
      }
      evaluateRule(datalogProgram.clauses(n).asInstanceOf[Rule], isRecursive)
      ruleStack = ruleStack.drop(1)

    }*/

   /* if (hasfilter == true) {

      val all = spark.table(datalogProgram.qr.name)

      val result = Util.filter(datalogProgram.qr, all)

      result.createOrReplaceTempView(datalogProgram.qr.name)

    }*/

  //}


 /* def evaluateRule(rule: Rule, isRecursive: Boolean): Unit = {

    println("evaluate:" + rule.head.name)

    isRecursive match {

      case false =>
        val predicate: Seq[DataFrame] = rule.bodies.map(Util.giveAlias(_, spark))
        var result: DataFrame = Util.project(rule, predicate.reduce(Util.naturalJoin(_, _))).distinct()
        result.createOrReplaceTempView(rule.head.name)

      case true =>

        for (p <- rule.bodies) {
          if (p.name.equals(rule.head.name)) p.name = "delta"
        }

        val base: DataFrame = spark.table(rule.head.name)

        var delta: DataFrame = Util.giveAlias(rule.head, spark).cache()
        delta.createOrReplaceTempView("delta")

        var all: DataFrame = delta

        while (delta.count > 0) {

          val predicateR: Seq[DataFrame] = rule.bodies.map(Util.giveAlias(_, spark))

          delta = Util.project(rule, predicateR.reduce(Util.naturalJoin(_, _))).except(all).distinct().cache()

          delta.createOrReplaceTempView("delta")
          spark.table("delta").show()

          all = all.union(delta).distinct().cache()

          if (delta.count > 0) {
            all.createOrReplaceTempView("all")
            all.cache()
          }
          else {
            all.createOrReplaceTempView(rule.head.name)
          }
        }


    }


  }*/


}

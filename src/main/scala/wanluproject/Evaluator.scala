package wanluproject

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.HashMap


class Evaluator(datalogProgram: DatalogProgram, spark: SparkSession) {

  def loadFact() = {
    if (datalogProgram.hasFact == true) {
      val names: Seq[String] = datalogProgram.factList.map(_._1).toSeq
      val schema: Seq[StructType] = datalogProgram.factList.map(p => p._2.head._2).toSeq
      val value: Seq[RDD[Row]] = datalogProgram.factList.map(p => spark.sparkContext.parallelize(p._2.map(_._3))).toSeq
      for (i <- 0 until names.length) {
        var df: DataFrame = spark.createDataFrame(value(i), schema(i))
        if (spark.catalog.tableExists(names(i))) {
          val df0 = spark.table(names(i))
          df = df.union(df0)
          df.cache().createOrReplaceTempView(names(i))
          df.cache().createOrReplaceTempView("fact_" + names(i))
        }
        df.cache().createOrReplaceTempView(names(i))
        df.cache().createOrReplaceTempView("fact_" + names(i))
      }
    }
  }

  val rules = datalogProgram.ruleList.map(ifIDB)

  val idb = datalogProgram.idbList
  val idb_array: Array[String] = idb.map(_._1).toArray

  def ifIDB(rule: Rule): Unit = {
    val ary = datalogProgram.idbList.map(_._1).toArray
    rule.hasIdbSubgoal = !rule.bodies.filter(p => ary.contains(p.name)).isEmpty
  }

  def testIDB() = {
    datalogProgram.clauses.filter(_.isInstanceOf[Rule]).map(_.asInstanceOf[Rule]).map(ifIDB(_))

    val evaluateRulesLine = datalogProgram.clauses.zipWithIndex.filter(_._1.isInstanceOf[Rule]).
      map(pair => (pair._1.asInstanceOf[Rule].head.name, pair._2, pair._1.asInstanceOf[Rule].hasIdbSubgoal)).groupBy(_._1)

    evaluateRulesLine
  }

  /*def semi_naive(): Unit = {

    val start = System.currentTimeMillis()

    val evaluateRules = testIDB()

    //idb = empty
    for (i <- idb) {
      println(i._1)
      val df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], i._2)
      df.createOrReplaceTempView(i._1)
      val name = "delta" + i._1
      val delta = df
      df.createOrReplaceTempView(name)
    }

    loadFact()

    spark.catalog.listTables().show()

    var fixpoint = false
    val n = idb.length

    var fixpoints: Array[Boolean] = new Array(n)
    var iter = 0
    val recursiveRuleLine = new Array[Seq[Rule]](n)

    while (!fixpoint) {
      println("iter:" + iter)

      for (i <- 0 until idb.length) {

        var rules = Seq[Rule]()
        iter match {
          case 0 =>
            val rulesIndice: Seq[Int] = evaluateRules(idb(i)._1).map(_._2)
            rules = for (i <- rulesIndice) yield datalogProgram.clauses(i).asInstanceOf[Rule]
            fixpoints(i) = semi_naive_evaluate_idb(rules, spark)
          case 1 =>
            val rulesIndice: Seq[Int] = evaluateRules(idb(i)._1).filter(_._3.equals(true)).map(_._2)
            rules = for (i <- rulesIndice) yield datalogProgram.clauses(i).asInstanceOf[Rule]
            val rewriteRules = semi_naive_rewrite(rules)
            recursiveRuleLine(i) = rewriteRules
            fixpoints(i) = semi_naive_evaluate_idb(rewriteRules, spark)
          case _ =>
            val rules = recursiveRuleLine(i)
            fixpoints(i) = semi_naive_evaluate_idb(rules, spark)
        }

      }

      fixpoint = if (fixpoints.filter(_.equals(false)).isEmpty == true) true else false
      iter = iter + 1
    }
    println("semi_naive:" + (System.currentTimeMillis() - start))
  }*/

  def semi_naive() = {

    def toEvaluate(rule: Rule, map: HashMap[String, Boolean]): Boolean = {
      val newResult = rule.bodies.filter(p => p.name.take(2).equals("d_")).filter(p => map(p.original_name) == true)
      val result = if (newResult.isEmpty) true else false
      result
    }

    val start = System.currentTimeMillis()
    val evaluateRules = testIDB()
    //idb = empty
    for (i <- idb) {
      val df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], i._2)
      df.cache().createOrReplaceTempView(i._1)
    }

    loadFact()

    for (i <- idb) {
      val delta = spark.table(i._1)
      val name = "d_" + i._1
      delta.cache().createOrReplaceTempView(name)
    }

    var fixpoint = false
    val n = idb.length
    var fixpoints: Array[Boolean] = new Array(n)

    val recursiveRules = new Array[Seq[Rule]](n)

    // initialization
    for (i <- 0 until idb_array.length) {
      val edb_rulesIndice: Seq[Int] = evaluateRules(idb(i)._1).filter(_._3.equals(false)).map(_._2)
      val edb_rules = for (i <- edb_rulesIndice) yield datalogProgram.clauses(i).asInstanceOf[Rule]
      if (edb_rules.isEmpty == false) {
        val name = edb_rules(0).head.name
        val delta_name = "d_" + name
        val dfs =
          if (datalogProgram.fList.map(_._1).contains(name) == true)
            edb_rules.map(evaluate_rule(_, spark)).filter(_.head(1).isEmpty == false) :+ spark.table("fact_" + name)
          else
            edb_rules.map(evaluate_rule(_, spark)).filter(_.head(1).isEmpty == false)

        val delta: DataFrame = dfs.reduce((l, r) => Util.union(l, r)).cache()
        delta.createOrReplaceTempView(delta_name)
        val all = delta.cache()
        all.createOrReplaceTempView(name)
      }
    }

    var iter = 1

    while (!fixpoint) {
      println("iter:" + iter)

      for (i <- idb) {
        val df = spark.table("d_" + i._1)
        df.createOrReplaceTempView("pre_d_" + i._1)
      }

      var pre_fixpoints = HashMap[String, Boolean]()
      for (i <- 0 until idb_array.length) {
        pre_fixpoints += (idb_array(i) -> fixpoints(i))
      }

      for (i <- 0 until idb_array.length) {
        var rules = Seq[Rule]()
        iter match {
          case 1 =>
            val rulesIndice: Seq[Int] = evaluateRules(idb(i)._1).filter(_._3.equals(true)).map(_._2)
            rules = for (i <- rulesIndice) yield datalogProgram.clauses(i).asInstanceOf[Rule]
            val rewriteRules = semi_naive_rewrite(rules)
            recursiveRules(i) = rewriteRules
             //val rulesToEvaluate = recursiveRules(i)
            val rulesToEvaluate = recursiveRules(i).filter(toEvaluate(_, pre_fixpoints))
            fixpoints(i) = semi_naive_evaluate_idb(rulesToEvaluate, spark)
          case _ =>
            // val rulesToEvaluate = recursiveRules(i)
            val rulesToEvaluate = recursiveRules(i).filter(toEvaluate(_, pre_fixpoints))
            fixpoints(i) = semi_naive_evaluate_idb(rulesToEvaluate, spark)
        }
      }

      for( i <- idb){
        val delta_name = "d_" + i._1
        if( !spark.table(delta_name).head(1).isEmpty ) {
          val start = System.currentTimeMillis()
          val delta = spark.table(delta_name).checkpoint(true)
          delta.createOrReplaceTempView(delta_name)
          val all = Util.union(spark.table(i._1), spark.table("d_" + i._1)).checkpoint(true)
          all.cache().createOrReplaceTempView(i._1)
        }
      }

      println(fixpoints.mkString(","))
      fixpoint = if (fixpoints.filter(_.equals(false)).isEmpty == true) true else false
      iter = iter + 1
    }
    println("semi_naive:" + (System.currentTimeMillis() - start))
  }


  def semi_naive_evaluate_idb(rules: Seq[Rule], spark: SparkSession): Boolean = {

    val start = System.currentTimeMillis()
    var fixpoint = true
    if (rules.isEmpty == true) fixpoint
    else {
      //1. name
      val name = rules(0).head.name
      val delta_name = "d_" + name
      //2. evaluate rule
      val dfs = rules.map(evaluate_rule(_, spark)).filter(_.head(1).isEmpty == false)

      //3. update delta
      if (dfs.isEmpty == false) {
        val result: DataFrame = dfs.reduce((l, r) => Util.union(l, r)).cache()
        val delta: DataFrame = result.except(spark.table(name)).cache()
        if (delta.head(1).isEmpty == false ) {
          delta.createOrReplaceTempView(delta_name)
          fixpoint = false
        }
      }
      else{
        val delta = spark.emptyDataFrame.cache()
        delta.createOrReplaceTempView(delta_name)
        delta.show()
      }

      println("evaluate idb: " + (System.currentTimeMillis() - start))
      fixpoint
    }
  }

  def semi_naive_rewrite(rules: Seq[Rule]): Seq[Rule] = {
    var rewriteRules = Seq[Rule]()
    for (r <- rules) {
      var rw = false
      for (i <- 0 until r.bodies.length)
        if (idb_array.contains(r.bodies(i).name)) {
          rw = true
          val rp = r.bodies(i).copy(id = Identifier("d_" + r.bodies(i).name))
          rp.original_name = r.bodies(i).name
          val rbody: Seq[Literal] = r.body.patch(i, Seq(rp), 1)
          val rr = r.copy(body = rbody)
          rewriteRules = rewriteRules :+ rr
        }
      if (rw == false)
        rewriteRules = rewriteRules :+ r
    }
    rewriteRules
  }

  def naive(): Unit = {

    val evaluateRules: Map[String, Seq[(String, Int, Boolean)]] = testIDB()

    //idb = empty
    for (i <- idb) {
      val df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], i._2)
      df.createOrReplaceTempView(i._1)
    }

    loadFact()

    var fixpoint = false
    var iter = 1
    val n = idb.length
    var fixpoints: Array[Boolean] = new Array(n)

    val recursiveRuleLine = new Array[Seq[Int]](n)

    while (!fixpoint) {

      println("iter:" + iter)
      for (i <- idb) {
        val df = spark.table(i._1).cache()
        df.createOrReplaceTempView("pre_" + i._1)
      }

      for (i <- 0 until idb.length) {

        val rulesIndice: Seq[Int] = evaluateRules(idb(i)._1).map(_._2)

        var rules: Seq[Rule] = for (i <- rulesIndice) yield datalogProgram.clauses(i).asInstanceOf[Rule]
        fixpoints(i) = naive_evaluate_idb(rules, spark)

        if (iter == 0)
          fixpoints(i) = false
      }

      println(fixpoints.mkString(","))
      fixpoint = if (fixpoints.filter(_.equals(false)).isEmpty == true) true else false
      iter = iter + 1
    }
  }

  def naive_evaluate_idb(rules: Seq[Rule], spark: SparkSession): Boolean = {

    val start = System.currentTimeMillis()
    var fixpoint = true
    if (rules.isEmpty == true) fixpoint
    else {
      val name = rules(0).head.name
      val dfs =
        if (datalogProgram.fList.map(_._1).contains(name) == true)
          rules.map(evaluate_rule(_, spark)).filter(_.head(1).isEmpty == false) :+ spark.table("fact_" + name)
        else
          rules.map(evaluate_rule(_, spark)).filter(_.head(1).isEmpty == false)
      if (dfs.isEmpty == false) {
        val result: DataFrame = dfs.reduce((l, r) => Util.union(l, r)).cache()
        if (result.except(spark.table("pre_"+name)).head(1).isEmpty == false) {
          val all = result.checkpoint(true) //local Checkpoint in 2.3.0
          all.createOrReplaceTempView(name)
          fixpoint = false
        }
      }
    }
    println("evaluate idb:" + (System.currentTimeMillis() - start))
    fixpoint
  }

  /* def evaluate_rule(rule: Rule, spark: SparkSession): DataFrame = {

     val predicates: Seq[DataFrame] = rule.bodies.map(Util.giveAlias(_, spark)).filter(_.isInstanceOf[DataFrame]).map(_.asInstanceOf[DataFrame])
     val selectCondition = rule.selectCondition
     val cols: List[String] = rule.head.argArray.toList
     val joined = predicates.reduce(Util.naturalJoin(_, _, spark))
     val selected = if (selectCondition.isEmpty == false) Util.select(selectCondition, joined) else joined
     var result: DataFrame = {
       predicates.length match {
         case 0 =>
           spark.emptyDataFrame
         case _ =>
           Util.project(cols, selected, spark)
       }
     }
     result
   }*/

  def evaluate_rule(rule: Rule, spark: SparkSession): DataFrame = {

    val relations: Seq[DataFrame] = rule.bodies.map(Util.giveAlias(_, spark)).filter(_.isInstanceOf[DataFrame]).map(_.asInstanceOf[DataFrame])
    var result: DataFrame = {
      relations.length match {
        case 0 =>
          spark.emptyDataFrame
        case _ =>
          val selectCondition = rule.selectCondition
          val cols: List[String] = rule.head.argArray.toList
          val joined = relations.reduce(Util.naturalJoin(_, _, spark))
          val selected = if (selectCondition.isEmpty == false) Util.select(selectCondition, joined) else joined
          Util.project(cols, selected, spark)
      }
    }
    result
  }
}

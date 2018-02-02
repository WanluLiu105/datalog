package wanluproject

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


class Evaluator(datalogProgram: DatalogProgram, spark: SparkSession) {

  def loadFact() = {
    if (datalogProgram.hasFact == true) {
      val names: Seq[String] = datalogProgram.factList.map(_._1).toSeq
      println("f name:" + names.mkString(","))
      val schema: Seq[StructType] = datalogProgram.factList.map(p => p._2.head._2).toSeq
      val value: Seq[RDD[Row]] = datalogProgram.factList.map(p => spark.sparkContext.parallelize(p._2.map(_._3))).toSeq
      for (i <- 0 until names.length) {
        var df: DataFrame = spark.createDataFrame(value(i), schema(i))
        if (spark.catalog.tableExists(names(i))) {
          val df0 = spark.table(names(i))
          df = df.union(df0)
          df.createOrReplaceTempView(names(i))
        }
        df.createOrReplaceTempView(names(i))
        df.cache()
      }
    }
  }


  val rules = datalogProgram.ruleList.map(ifIDB)

  val idb = datalogProgram.idbList
  val idb_array: Array[String] = idb.map(_._1).toArray
  //val edb = datalogProgram.edbList

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

  def semi_naive(): Unit = {

    val start = System.currentTimeMillis()

    val evaluateRules = testIDB()

    //idb = empty
    for (i <- idb) {
      println( i._1)
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
            fixpoints(i) = semi_naive_evaluate_idb(rules, spark,iter)
          case 1 =>
            val rulesIndice: Seq[Int] = evaluateRules(idb(i)._1).filter(_._3.equals(true)).map(_._2)
            rules = for (i <- rulesIndice) yield datalogProgram.clauses(i).asInstanceOf[Rule]
            val rewriteRules = semi_naive_rewrite(rules)
            recursiveRuleLine(i) = rewriteRules
            fixpoints(i) = semi_naive_evaluate_idb(rewriteRules, spark,iter)
          case _ =>
            val rules = recursiveRuleLine(i)
            fixpoints(i) = semi_naive_evaluate_idb(rules, spark,iter)
        }

      }

      fixpoint = if (fixpoints.filter(_.equals(false)).isEmpty == true) true else false
      iter = iter + 1
    }
    println("semi_naive:" + (System.currentTimeMillis() - start))
  }

  def semi_naive_evaluate_idb(rules: Seq[Rule], spark: SparkSession,iter: Int): Boolean = {

    val start = System.currentTimeMillis()
    var fixpoint = true
    if (rules.isEmpty == true) fixpoint
    else {
      val name = rules(0).head.name
      val dfs = rules.map(evaluate_rule(_, spark))
      val delta: DataFrame = dfs.reduce((l, r) => Util.union(l, r)).except(spark.table(name))
     // if( iter != 0 && iter%3 == 0) delta.checkpoint(true) else delta.cache
      delta.cache()
      println("partition:" + delta.rdd.getNumPartitions)
      println("delta:" + name)
      //delta.show()
      val delta_name = "delta" + name
      val all = spark.table(name).union(delta).distinct()
    //  if( iter != 0 && iter%3 == 0) all.checkpoint(true) else all.cache
      all.cache()
      delta.createOrReplaceTempView(delta_name)
      all.createOrReplaceTempView(name)
      if (delta.count() != 0) {
        delta.createOrReplaceTempView(delta_name)
        all.createOrReplaceTempView(name)
        fixpoint = false
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
          val rp = r.bodies(i).copy(id = Identifier("delta" + r.bodies(i).id.value))
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
      println("idbempty:" + i._1)
      val df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], i._2)
      df.createOrReplaceTempView(i._1)
    }

    loadFact()

    var fixpoint = false
    var iter = 0
    val n = idb.length
    var fixpoints: Array[Boolean] = new Array(n)

    val recursiveRuleLine = new Array[Seq[Int]](n)

    spark.catalog.listTables().show()

    while (!fixpoint) {

      for (i <- 0 until idb.length) {

        val rulesIndice: Seq[Int] = iter match {
          case 0 =>
            evaluateRules(idb(i)._1).filter(_._3.equals(false)).map(_._2)
          case 1 =>
            recursiveRuleLine(i) = evaluateRules(idb(i)._1).filter(_._3.equals(true)).map(_._2)
            evaluateRules(idb(i)._1).filter(_._3.equals(true)).map(_._2)
          case _ =>
            recursiveRuleLine(i)
        }

        // val rulesIndice: Seq[Int] = evaluateRules(idb(i)._1).map(_._2)
        var rules: Seq[Rule] = for (i <- rulesIndice) yield datalogProgram.clauses(i).asInstanceOf[Rule]
        fixpoints(i) = naive_evaluate_idb(rules, spark)

        if (iter == 0)
          fixpoints(i) = false
      }

      // println(fixpoints.mkString(","))
      fixpoint = if (fixpoints.filter(_.equals(false)).isEmpty == true) true else false
      // println(fixpoint + " iter:" + iter)
      iter = iter + 1
    }
  }

  def naive_evaluate_idb(rules: Seq[Rule], spark: SparkSession): Boolean = {

    var fixpoint = true
    if (rules.isEmpty == true) fixpoint
    else {
      val name = rules(0).head.name
      val dfs = rules.map(evaluate_rule(_, spark))
      val result: DataFrame = dfs.reduce((l, r) => Util.union(l, r)).union(spark.table(name)).distinct().cache()
      if (result.except(spark.table(name)).count() != 0) {
        result.createOrReplaceTempView(name)
        fixpoint = false
      }
      fixpoint
    }
  }

  def evaluate_rule(rule: Rule, spark: SparkSession): DataFrame = {

    val predicates: Seq[DataFrame] = rule.bodies.map(Util.giveAlias(_, spark)).filter(_.isInstanceOf[DataFrame]).map(_.asInstanceOf[DataFrame])
    val selectCondition = rule.selectCondition
    val cols: List[String] = rule.head.argArray.toList
    val joined = predicates.reduce(Util.naturalJoin(_, _))
    val selected = if(selectCondition.isEmpty == false) Util.select(selectCondition, joined) else joined
    var result: DataFrame = {
      predicates.length match {
        case 0 =>
          spark.emptyDataFrame
        case _ =>
          Util.project(cols, selected)
      }
    }
    result
  }

  /*def magic_set() ={
    val ms = new MagicSet(datalogProgram)
    val program = ms.magic_rewrite()
   // println(program.clauses.mkString("\n"))*/



}

package wanluproject

import org.apache.spark.sql.SparkSession

class MagicSet(datalogProgram: DatalogProgram, spark: SparkSession) {

  val idb_array = datalogProgram.idbList.map(_._1).toArray

  def magic_set(): DatalogProgram = {
    val magicstarttime = System.currentTimeMillis()
    val query = datalogProgram.query match {
      case Some(qr) => qr
      case None => throw SemanticException("no query")
    }
    val arg_list = query.free
    //bind the constant
    for (index <- query.bindIndex) {
      query.predicate.bindFree(index) = 'b'
    }
    val query_predicate: Predicate = query.predicate.copy(id = Identifier(query.predicate.name + "_" + query.predicate.bindFree.mkString("")))
    for (i <- 0 until query_predicate.bindFree.length) {
      query_predicate.bindFree(i) = query.predicate.bindFree(i)
    }
    query_predicate.original_name = query.predicate.name
    val query_r_h = Predicate(Identifier("query"), arg_list)
    val query_rule = Rule(head = query_r_h, body = Seq(query_predicate))
    val rw_query_p = Predicate(query_predicate.id, query.queryV)
    val rw_query_r = Rule(head = query_r_h, body = Seq(rw_query_p) ++ query.queryCondition)
    val cons = query.predicate.args.filter(_.isInstanceOf[Constant]).map(_.asInstanceOf[Constant])
    val magic_fact_name = magic_predicate_generate(query_predicate).name
    val magicFact = Fact(Identifier(magic_fact_name), args = cons)
    val clausesWithQuery = datalogProgram.clauses :+ query_rule
    val programWithQuery = datalogProgram.copy(clausesWithQuery, None)
    val adProgram = magic_adorn_clauses(programWithQuery, query_predicate)
    val mPredicate: Seq[Rule] = magic_predicates_generate(adProgram)
    val addMagicPredicate: Seq[Rule] = add_magic_predicate(adProgram)
    val rwClauses = rw_query_r +: magicFact +: mPredicate ++: addMagicPredicate
    // val rwClauses = magicFact +: mPredicate ++: addMagicPredicate
    //println()
    println(rwClauses.mkString("\n"))
    val rwProgram = DatalogProgram(rwClauses, None)
    rwProgram
    //val time = (System.currentTimeMillis()- magicstarttime)
    //println("rwtime:" + time)
    // evaluate(rwProgram)

  }

  def magic_adorn_clauses(program: DatalogProgram, query_predicate: Predicate): Seq[Rule] = {
    // println("adorn clauses")

    var current_adorned_idb = Seq[Predicate](query_predicate)
    var all_adorned_idb = Set[String](query_predicate.name)
    var adorned_clauses = Seq[Rule]()

    def has_bind(p: Predicate, hb: Set[String], pb: Set[String]): Boolean = {
      val bind1 = p.argArray.toSet & hb // bound by the head
      val bind2 = p.argArray.toSet & pb //bound by previously evaluated predicates
      val re = if (bind1.isEmpty == true && bind2.isEmpty == true) false else true
      re
    }

    def magic_adorn_rule(rule: Rule, h: Predicate): Rule = {
      // println("adorn rule")
      var adorned = Seq[Predicate]()
      // boound: variable occurs in a literal in the adorned list
      var pre_bind = Set[String]()
      //bound: variable is bound by the head
      var non_adorned = rule.bodies.filter(_.isInstanceOf[Predicate]).map(_.asInstanceOf[Predicate])
      val h_p = rule.head.copy(Identifier(rule.head.name + "_" + h.bindFree.mkString("")))
      h_p.original_name = rule.head.name
      for (i <- 0 until h.bindFree.length) {
        h_p.bindFree(i) = h.bindFree(i)
      }
      // adorn the head predicate
      val head_bind = (for (i <- 0 until h_p.bindFree.length; if (h_p.bindFree(i).equals('b'))) yield h_p.argArray(i)).toSet

      def edb_bind(p: Predicate, hb: Set[String], pb: Set[String]): Boolean = {
        val b = if (idb_array.contains(p.original_name) == false && has_bind(p, hb, pb) == true) true
        else false
        b
      }

      def idb_bind(p: Predicate, hb: Set[String], pb: Set[String]): Boolean = {
        val b = if (idb_array.contains(p.original_name) == true && has_bind(p, hb, pb) == true) true
        else false
        b
      }

      def find(): (Predicate, Char, Boolean) = {

        //return( predicate, edb/idb, has/has not bind)

        val ebind_index: Int = non_adorned.indexWhere(edb_bind(_, head_bind, pre_bind) == true)
        if (ebind_index != -1) {
          // if has bind, true; else,false
          val e = non_adorned(ebind_index).asInstanceOf[Predicate]
          non_adorned = non_adorned.take(ebind_index) ++ non_adorned.drop(ebind_index + 1)
          return (e, 'e', true)
        } else {
          val ibind_index = non_adorned.indexWhere(idb_bind(_, head_bind, pre_bind))
          if (ibind_index != -1) {
            val e = non_adorned(ibind_index).asInstanceOf[Predicate]
            non_adorned = non_adorned.take(ibind_index) ++ non_adorned.drop(ibind_index + 1)
            return (e, 'i', true)
          } else {
            val edb_index = non_adorned.indexWhere(p => idb_array.contains(p.original_name) == false)
            if (edb_index != -1) {
              val e = non_adorned(edb_index).asInstanceOf[Predicate]
              non_adorned = non_adorned.take(edb_index) ++ non_adorned.drop(edb_index + 1)
              return (e, 'e', false)
            }
            else {
              val idb_index = non_adorned.indexWhere(p => idb_array.contains(p.original_name) == true)
              val e = non_adorned(idb_index).asInstanceOf[Predicate]
              non_adorned = non_adorned.take(idb_index) ++ non_adorned.drop(idb_index + 1)
              return (e, 'i', false)
            }

          }

        }
      }

      while (non_adorned.isEmpty == false) {

        val pair = find()
        val b = pair._1

        (pair._2, pair._3) match {
          case ('e', true) =>
            val bind1 = b.argArray.toSet & head_bind
            val bind2 = b.argArray.toSet & pre_bind
            val bind = bind1 ++ bind2
            for (i <- 0 until b.argArray.length) {
              if (bind.contains(b.argArray(i)) == true) b.bindFree(i) = 'b'
            }
            adorned = adorned :+ b
          case ('i', true) =>
            val bfs = new Array[Char](h.argArray.length)
            for (i <- 0 until bfs.length) bfs(i) = 'f'
            val bind1 = b.argArray.toSet & head_bind
            val bind2 = b.argArray.toSet & pre_bind
            val bind = bind1 ++ bind2
            for (i <- 0 until b.argArray.length) {
              if (bind.contains(b.argArray(i)) == true) bfs(i) = 'b'
            }
            val ap = b.copy(id = Identifier(b.name + "_" + bfs.mkString("")))
            ap.original_name = b.name
            for (i <- 0 until ap.bindFree.length) {
              ap.bindFree(i) = bfs(i)
            }
            if (all_adorned_idb.contains(ap.name).equals(false)) {
              current_adorned_idb = current_adorned_idb :+ ap
              all_adorned_idb = all_adorned_idb + ap.name
              // println("all:" +all_adorned_idb.mkString("，"))
            }
            adorned = adorned :+ ap
          case ('e', false) =>
            adorned = adorned :+ b
          case ('i', false) =>
            val bfs = new Array[Char](h.argArray.length)
            for (i <- 0 until bfs.length) bfs(i) = 'f'
            val ap = b.copy(id = Identifier(b.name + "_" + bfs.mkString("")))
            ap.original_name = b.name
            for (i <- 0 until ap.bindFree.length) {
              ap.bindFree(i) = bfs(i)
            }
            adorned = adorned :+ ap

        }
        pre_bind = pre_bind ++ b.argArray
      }

      val ar = rule.copy(head = h_p, body = adorned)
      // println(ar)
      ar
    }

    while (current_adorned_idb.isEmpty == false) {
      val p = current_adorned_idb.head
      val rulesToAdorn: Seq[Rule] = program.clauses.filter(_.isInstanceOf[Rule]).map(_.asInstanceOf[Rule]).filter(h => h.head.name.equals(p.original_name))
      for (r <- rulesToAdorn) {
        val rr = magic_adorn_rule(r, p)
        adorned_clauses = adorned_clauses :+ rr
      }
      current_adorned_idb = current_adorned_idb.tail
    }

    // println("result:" + adorned_clauses.mkString("\n"))
    adorned_clauses
  }

  def magic_predicates_generate(rules: Seq[Rule]): Seq[Rule] = {
    var magic_predicate_rules = Seq[Rule]()
    for (r <- rules) {
      val magic_head = magic_predicate_generate(r.head)
      for (i <- 0 until r.bodies.length; if (r.bodies(i).isInstanceOf[Predicate] == true)) {
        // idb predicates
        if (idb_array.contains(r.bodies(i).original_name) == true) {
          if (r.bodies(i).bindFree.filter(_.equals('b')).isEmpty == false) {
            val newHead = magic_predicate_generate(r.bodies(i))
            val newBodies: Seq[Literal] = magic_head +: r.bodies.take(i)
            val newRule = r.copy(head = newHead, body = newBodies)
            magic_predicate_rules = magic_predicate_rules :+ newRule
          }
        }
      }
    }
    magic_predicate_rules
  }

  def magic_predicate_generate(p: Predicate): Predicate = {
    val name = "m_" + p.name
    val args: Seq[Term] = for (i <- 0 until p.bindFree.length; if p.bindFree(i).equals('b')) yield p.args(i)
    val mp = Predicate(Identifier(name), args)
    mp.original_name = p.original_name
    mp
  }

  def add_magic_predicate(rules: Seq[Rule]): Seq[Rule] = {
    var magic_rules = Seq[Rule]()
    for (r <- rules) {
      if (r.head.bindFree.filter(_.equals('b')).isEmpty == false) {
        val newBodies = magic_predicate_generate(r.head) +: r.body
        val newRule = r.copy(body = newBodies)
        magic_rules = magic_rules :+ newRule
      }

    }
    magic_rules
  }

  /*def evaluate(datalogProgram: DatalogProgram)= {
    val evaluator = new Evaluator(datalogProgram,spark)
    evaluator.naive()
  }*/


}

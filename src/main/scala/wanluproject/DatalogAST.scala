package wanluproject


import org.apache.spark.sql.Row
import org.apache.spark.sql.types._


trait DatalogAST

case class DatalogProgram(clauses: Seq[Clause], query: Option[Query]) extends DatalogAST {

  val hasQuery = query.isEmpty

  val ruleList = clauses.filter(_.isInstanceOf[Rule]).map(_.asInstanceOf[Rule])

  val idbList: List[(String, StructType)] = ruleList.map(p => (p.head.name, p.headSchema)).toSet.toList

  val fList: List[(String, StructType)] = clauses.filter(_.isInstanceOf[Fact]).map(_.asInstanceOf[Fact]).map(f => (f.name, f.schema)).toSet.toList

  val hasFact: Boolean = !clauses.filter(_.isInstanceOf[Fact]).isEmpty

  val factList: Map[String, Seq[(String, StructType, Row)]] =
    clauses.filter(_.isInstanceOf[Fact]).map(_.asInstanceOf[Fact]).map(f => (f.name, f.schema, f.value)).groupBy(_._1)

  // val ruleList: Seq[Rule] = clauses.filter(_.isInstanceOf[Rule]).map(_.asInstanceOf[Rule])

  //begin from 0  the list of rule( ruleHead, ruleLineNumber, ruleIfIsIterate)
  /* val lineOfRule: Seq[(String, Int, Boolean)] = clauses.zipWithIndex.filter(_._1.isInstanceOf[Rule]).
     map(pair => (pair._1.asInstanceOf[Rule].head.name, pair._2, pair._1.asInstanceOf[Rule].isRecursive))*/
  //++ clauses.zipWithIndex.filter(_._1.isInstanceOf[Fact]).map(pair => (pair._1.asInstanceOf[Fact].name, pair._2, "fact"))

  // val ruleIndex: Map[String, Seq[(String, Int, Boolean)]] = lineOfRule.groupBy(_._1)

  //(name, line number, if is recursive)


  /*  def predicateBoundCheck = {
      if ((ruleList.flatMap(_.unknown).toSet -- idbList -- edbList -- fList).isEmpty == true) true
      else false
    }*/

}


case class Query(predicate: Predicate) extends DatalogAST {

  val name = predicate.name
  //find the constraint arg
  val bind: Seq[Constant] = predicate.args.filter(_.isInstanceOf[Constant]).map(_.asInstanceOf[Constant])
  val free: Seq[Variable] = predicate.args.filter(_.isInstanceOf[Variable]).map(_.asInstanceOf[Variable])
  val bindIndex: Seq[Int] = predicate.args.zipWithIndex.filter(_._1.isInstanceOf[Constant]).map(_._2)

  var queryV = Seq[Variable]()
  for (i <- 0 until predicate.args.length) {
    if (predicate.args(i).isInstanceOf[Variable]) queryV = queryV :+ predicate.args(i).asInstanceOf[Variable]
    else queryV = queryV :+ Variable("Variable_" + i)
  }

  val queryP = Predicate(predicate.id, queryV)
  val queryCondition = for (i <- 0 until predicate.args.length; if (predicate.args(i).isInstanceOf[Constant]))
    yield Condition(Variable("Variable_" + i), "==", predicate.args(i).asInstanceOf[Constant])

  val rwBody: Seq[Literal] = Seq(queryP) ++ queryCondition
}

trait Clause extends DatalogAST

case class Fact(id: Identifier, args: Seq[Constant]) extends Clause {

  val name = id.value
  val argArray: Array[String] = args.map(_.asInstanceOf[Constant].value).toArray
  val arity = argArray.length

  // : List[(String,StringType, Boolean)]
  val schema: StructType = StructType(argArray.zipWithIndex.map(p => StructField(p._2.toString, StringType, true)))
  val value: Row = Row(argArray: _*)

}

case class Rule(head: Predicate, body: Seq[Literal]) extends Clause {

  val bodies: Seq[Predicate] = body.filter(_.isInstanceOf[Predicate]).map(_.asInstanceOf[Predicate])

  val headSchema: StructType = StructType(head.argArray.zipWithIndex.map(p => StructField(p._2.toString, StringType, true)))

  var hasIdbSubgoal = false

  // collect all the Variable in the RHS
  // var bodyVariable: Set[String] = body.filter(_.isInstanceOf[Predicate]).map(_.asInstanceOf[Predicate]).flatMap(_.argArray.toSeq).toSet

  //val headVarBoundCheck: Boolean = if ((head.argArray.toSet -- bodyVariable).isEmpty) true else false

  val selectCondition: Seq[Expr] = body.filter(_.isInstanceOf[Expr]).map(_.asInstanceOf[Expr])

}

trait Literal extends DatalogAST

case class Predicate(id: Identifier, args: Seq[Term]) extends Literal {

  var name = id.value
  var original_name = id.value
  val argArray: Array[String] = args.map(_ match {
    case Variable(str) => str.toString
    case Constant(str) => str.toString
  }).toArray
  val bindFree = argArray.map(_ => 'f')

}

trait Expr extends Literal

case class Condition(lhs: Variable, op: String, rhs: Term) extends Expr

//case class TermExpr(lhs: Term, op: String, rhs: Term) extends Expr

trait Term extends Expr

case class Constant(const: Term) extends Term {

  val value = const match {
    case Identifier(v) => v
    case Str(v) => v
  }
}

case class Identifier(value: String) extends Term

case class Variable(value: String) extends Term

case class Str(str: String) extends Term




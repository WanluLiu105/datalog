package wanluproject


import org.apache.spark.sql.Row
import org.apache.spark.sql.types._


trait DatalogAST

case class DatalogProgram(clauses: Seq[Clause]) extends DatalogAST {

  var ruleOrder: List[(String, Int, String)] = Nil

  val ruleList = clauses.filter(_.isInstanceOf[Rule]).map(_.asInstanceOf[Rule])

  val idbList: List[(String, StructType)] = ruleList.map(p=>(p.head.name, p.headSchema)).toSet.toList

  val fList: List[(String, StructType)]= clauses.filter(_.isInstanceOf[Fact]).map(_.asInstanceOf[Fact]).map(f=>(f.name, f.schema)).toSet.toList

  val edbList = (ruleList.flatMap(_.bodyList).toSet -- idbList).toList

 /* def order = {
    ruleForPredicate(qr.name)
  }*/

  val hasFact: Boolean = !clauses.filter(_.isInstanceOf[Fact]).isEmpty
  val factList: Map[String, Seq[(String, StructType, Row)]] =
    clauses.filter(_.isInstanceOf[Fact]).map(_.asInstanceOf[Fact]).map(f => (f.name, f.schema, f.value)).groupBy(_._1)

  // val ruleList: Seq[Rule] = clauses.filter(_.isInstanceOf[Rule]).map(_.asInstanceOf[Rule])

  //begin from 0  the list of rule( ruleHead, ruleLineNumber, ruleIfIsIterate)

  val lineOfRule: Seq[(String, Int, Boolean)] = clauses.zipWithIndex.filter(_._1.isInstanceOf[Rule]).
    map(pair => (pair._1.asInstanceOf[Rule].head.name, pair._2, pair._1.asInstanceOf[Rule].isRecursive))
  //++ clauses.zipWithIndex.filter(_._1.isInstanceOf[Fact]).map(pair => (pair._1.asInstanceOf[Fact].name, pair._2, "fact"))

  val ruleIndex: Map[String, Seq[(String, Int, Boolean)]] = lineOfRule.groupBy(_._1)


  //(name, line number, if is recursive)
 /* def ruleForPredicate(name: String): Any = {

    ruleIndex.get(name) match {
      case Some(index) =>
        index.filter(_._3 == "base").isEmpty && index.filter(_._3 == "fact").isEmpty match {
          case true =>
            SemanticException("no base")
          case false =>
            index.length match {

              case 1 =>
                if (!ruleOrder.contains(index.head)) {
                  if (index.head._3 != "fact") {
                    ruleOrder = index.head :: ruleOrder
                    if (clauses(index.head._2).asInstanceOf[Rule].unknown.isEmpty == false)
                      clauses(index.head._2).asInstanceOf[Rule].unknown.map(ruleForPredicate(_))
                  }
                }

              case _ =>
                if (!ruleOrder.contains(index.head)) {
                  val num = index.length
                  if ((index.filter(_._3 == "recursive").length == 1) && (index.filter(_._3 == "base").length == 1)) {
                    val i = index.indexWhere(_._3 == "recursive")
                    val j = index.indexWhere(_._3 == "base")
                    ruleOrder = index(j) :: (index(i) :: ruleOrder)

                    if (clauses(index(j)._2).asInstanceOf[Rule].unknown.isEmpty == false)
                      clauses(index(j)._2).asInstanceOf[Rule].unknown.map(ruleForPredicate(_))
                    if (clauses(index(i)._2).asInstanceOf[Rule].unknown.isEmpty == false) {
                      clauses(index(i)._2).asInstanceOf[Rule].unknown.map(ruleForPredicate(_))
                    }
                  }
                  else if (index.filter(_._3 == "recursive").length == 1 && index.filter(_._3 == "fact").length == (num - 1)) {
                    val i = index.indexWhere(_._3 == "recursive")
                    if (!ruleOrder.contains(index(i)))
                      ruleOrder = index(i) :: ruleOrder
                    if (clauses(index(i)._2).asInstanceOf[Rule].unknown.isEmpty == false)
                      clauses(index(i)._2).asInstanceOf[Rule].unknown.map(ruleForPredicate(_))
                  }
                  else SemanticException
                }


            }
        }
      case None => SemanticException("can't find rule for it")
    }

  }*/

  /*  def predicateBoundCheck = {
      if ((ruleList.flatMap(_.unknown).toSet -- idbList -- edbList -- fList).isEmpty == true) true
      else false
    }*/

}


case class Query(predicate: Predicate) extends DatalogAST {

  val name = predicate.name
  //find the constraint arg
  val argType = predicate.args.map(_.isInstanceOf[Constant]).toArray
  //the indice(column)-value pair
  val constraint: Seq[(Int, String)] = predicate.args.zipWithIndex.filter(_._1.isInstanceOf[Constant]).
    map(pair => (pair._2, pair._1.asInstanceOf[Constant].value))

  val hasConstraint = !constraint.isEmpty

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

  val bodyList: Seq[String] = body.filter(_.isInstanceOf[Predicate]).map(_.asInstanceOf[Predicate].name)

  val headSchema: StructType = StructType(head.argArray.zipWithIndex.map(p => StructField(p._2.toString, StringType, true)))

  val unknown: Seq[String] = body.filter(_.isInstanceOf[Predicate]).map(_.asInstanceOf[Predicate]).filter(_.isBase == false).
    filter(!_.name.equals(head.name)).map(_.name)

  var hasIdbSubgoal = false

  val isRecursive: Boolean = !bodyList.filter(_.equals(head.name)).isEmpty

  val recursive = if (isRecursive) "recursive" else "base"

  // collect all the Variable in the RHS
  var bodyVariable: Set[String] = body.filter(_.isInstanceOf[Predicate]).map(_.asInstanceOf[Predicate]).flatMap(_.argArray.toSeq).toSet

  val headVarBoundCheck: Boolean = if ((head.argArray.toSet -- bodyVariable).isEmpty) true else false

  val selectCondition: Seq[Expr] = body.filter(_.isInstanceOf[Expr]).map(_.asInstanceOf[Expr])

}

trait Literal extends DatalogAST


case class Predicate(id: Identifier, args: Seq[Term]) extends Literal {

  var name = id.value
  val isBase: Boolean = Database.relationList.contains(name)
  val argArray: Array[String] = args.map(_ match {
    case Variable(str) => str.toString
    case Constant(str) => str.toString
  }).toArray

  val arity: Int = argArray.length

}


trait Expr extends Literal


case class Condition(lhs: Expr, op: String, rhs: Expr) extends Expr


case class TermExpr(lhs: Term, op: String, rhs: Term) extends Expr

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




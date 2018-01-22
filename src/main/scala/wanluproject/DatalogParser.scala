package wanluproject

import scala.util.parsing.combinator.RegexParsers


object DatalogParser extends RegexParsers {

  def apply(code: String): Either[DatalogException, DatalogProgram] = {
    parse(datalogProgram, code) match {
      case NoSuccess(msg, next) =>
        Left(SyntacticException(msg))
      case Success(result, next) =>
        Right(result)
    }
  }

  // override def skipWhitespace: Boolean = true

  /*def datalogProgram: Parser[DatalogProgram] = clauseList ~ query ^^ {
    case rl ~ qr => DatalogProgram(qr, rl)
  }*/

  def datalogProgram: Parser[DatalogProgram] = clauseList ^^ { DatalogProgram(_)}

  def query: Parser[Query] = predicate <~ "?" ^^ {
    Query(_)
  }

  def clause: Parser[Clause] = rule | fact

  def clauseList: Parser[Seq[Clause]] = rep1(clause)

  // def ruleList: Parser[Seq[Rule]] = rep1(rule)

  def rule: Parser[Rule] = predicate ~ ":-" ~ literalList ~ "." ^^ {
    case l ~ _ ~ ll ~ _ => Rule(l, ll)
  }

  def fact: Parser[Fact] = identifier ~ "(" ~ constList ~ ")" ~ "." ^^ {
    case id ~ "(" ~ cl ~ ")" ~ "." => Fact(id, cl)
  }

  def constList: Parser[Seq[Constant]] = rep1sep(constant, ",")

  def literalList: Parser[Seq[Literal]] = rep1sep(literal, ",")

  def literal: Parser[Literal] = predicate | condition

  def predicate: Parser[Predicate] = identifier ~ "(" ~ terms ~ ")" ^^ {
    case id ~ _ ~ ts ~ _ => Predicate(id, ts)
  }

  def condition: Parser[Expr] = termExpr ~ ("==" | "!=" | ">" | "<" | ">=" | "<=" | "=") ~ termExpr ^^ {
    case t1 ~ sym ~ t2 => Condition(t1, sym, t2)
  }

  def termExpr: Parser[Expr] = term ~ ("+" | "-") ~ term ^^ {
    case t1 ~ sym ~ t2 => TermExpr(t1, sym, t2)
  } | term

  def terms: Parser[Seq[Term]] = rep1sep(term, ",")

  def term: Parser[Term] = variable | constant

  def constant: Parser[Constant] = identifier ^^ { id => Constant(id) } | string ^^ { str => Constant(str) }

  def identifier: Parser[Identifier] = "[a-z][a-zA-Z0-9_]*".r ^^ {
    Identifier(_)
  }

  def variable: Parser[Variable] = "[A-Z][a-zA-Z0-9_]*".r ^^ {
    Variable(_)
  }

  def string: Parser[Str] =
    """'[\w\s]*'""".r ^^ {
      case str =>
        val str1 = str.drop(1)
        val str2 = str1.dropRight(1)
        Str(str2)
    }
}
package wanluproject

import scala.util.parsing.combinator.RegexParsers
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

import org.apache.spark.sql.types._

/**
  * @author ${user.name}
  */
object test {


  case class Par(parent: String, child: String)

  case class Person(person: String)

  case class Path(from: String, to: String)


  def main(args: Array[String]) {


    val start = System.currentTimeMillis()
    val spark = SparkSession.builder().appName("test").config("spark.master", "local").getOrCreate()


    import spark.implicits._

    import org.apache.log4j.Logger
    import org.apache.log4j.Level

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)


    //val start = System.currentTimeMillis()

    val par = Seq(Par("george", "dorothy"), Par("george", "evelyn"), Par("dorothy", "bertrand"), Par("hilary", "ann"), Par("evelyn", "charles"), Par("dorothy", "ann")).toDF()
   // val par = Seq(Par("george", "dorothy"), Par("dorothy", "bertrand"), Par("hilary", "ann"), Par("dorothy", "ann")).toDF()
    val person = Seq(Person("ann"), Person("bertrand"), Person("charles"), Person("dorothy"), Person("evelyn"), Person("fred"), Person("george"), Person("hilary")).toDF()

    //val magic = Seq(Person("ann"), Person("hilary"), Person("dorothy"), Person("george")).toDF
   // broadcast(magic)
   // magic.cache()
   // magic.createOrReplaceTempView("magic")

    broadcast(par)
    broadcast(person)

    par.cache()
    person.cache()

    par.createOrReplaceTempView("par")
    person.createOrReplaceTempView("person")


    val path = Seq(Path("1", "2"), Path("2", "3"), Path("3", "4"),Path("4","5"), Path("5","6")).toDF()

    path.createOrReplaceTempView("path")
    broadcast(path)

    path.cache()

    Database.addRelation("path")

    val s1 =
        "sgc(X,X) :- person(X). " +
        "sgc(X,Y) :- par(XP,X), sgc(XP,YP), par(YP, Y), magic(XP)."  +
        "magic('ann')." +
        "magic(XP) :- magic(X),par(XP, X). "

    val s5 = "sgc(X,X) :- person(X). " +
             "sgc(X,Y) :- par(XP,X), sgc(XP,YP), par(YP, Y). "



    val s3 = "tc(X,Y) :- path(X , Y). "  +
             "tc(X,Y) :- tc(X,Z), path(Z,Y)." +
             "tc1(X,Y) :- tc(Y, X)." +
             "tc1(X,Y) :- tc1(X,Y), tc1(X,Y). "

    val s4 = "tc(X , Y)?"

    val s2 = " sgc(X, Y)? "

    val s6 = "sgc(X,X) :- person(X). " +
             "sgc(X,Y) :- par(XP,X), sgc(XP,YP), par(YP, Y) . "

    val s7 = "tc1(X,Y) :- path(X , Y). " +
      "tc1(X,Y) :- tc1(X,Z), path(Z,Y)." +
      "tc2(X,Y) :- tc1(X,Y). " +
      "tc2(X,Y) :- tc2(X,Z), path(Z,Y)."


    val datalogProgram: DatalogProgram =
      DatalogParser(s7) match {
        case Right(dp) => dp
        case Left(ex) => throw ex
      }



    //println(datalogProgram)
    val exe = new Evaluator(datalogProgram, spark)
    exe.semi_naive()

    spark.table("tc2").show()
   // spark.table("magic").show()


    println("time:" + ((System.currentTimeMillis()- start)/1000.0)  + "," + spark.table("tc2").count() )

  }

}
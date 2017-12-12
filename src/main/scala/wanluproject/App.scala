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
    val person = Seq(Person("ann"), Person("bertrand"), Person("charles"), Person("dorothy"), Person("evelyn"), Person("fred"), Person("george"), Person("hilary")).toDF()

    broadcast(par)
    broadcast(person)
    Database.addRelation("par")
    Database.addRelation("person")

    par.cache()
    person.cache()

    par.createOrReplaceTempView("par")
    person.createOrReplaceTempView("person")


    val path = Seq(Path("1", "2"), Path("2", "3"), Path("3", "4"),Path("4","1")).toDF()

    path.createOrReplaceTempView("path")
    broadcast(path)

    path.cache()



    Database.addRelation("path")

    val s1 =
      "sgc(X,X) :- magic(X). " +
      "sgc(X,Y):- par(XP,X), sgc(XP,YP), par(YP, Y), magic(XP)."  +
        "magic('ann')." +
        "magic(XP) :- magic(X),par(XP, X). "

    val s5 = "sgc(X,X) :- person(X). " +
             "sgc(X,Y) :- par(XP,X), sgc(XP,YP), par(YP, Y) . "



    val s3 = "tc(X,Y) :- path(X , Y). " +
             "tc(X,Y) :- tc(X,Z), tc(Z,Y)."

    val s4 = "tc(X , Y)?"

    val s2 = " sgc(X, Y)? "


    val schemaString = "name age"

   /* val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))

    val schema = StructType(fields)*/


    val datalogProgram: DatalogProgram =
      DatalogParser(s3+s4) match {
        case Right(dp) => dp
        case Left(ex) => throw ex
    }

   // datalogProgram.order
    println(datalogProgram.ruleOrder)

    val exe = new Executor(datalogProgram, spark)


    //spark.catalog.listTables().show()
    exe.evaluate()


    spark.table("tc").show()

    println("time:" + (System.currentTimeMillis()- start) + "count:" + spark.table("tc").count())



  }

}
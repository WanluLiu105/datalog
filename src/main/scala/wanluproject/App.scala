package wanluproject

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * @author ${user.name}
  */
object test {


  case class Par(parent: String, child: String)

  case class Person(person: String)

  case class Path(from: String, to: String)

  case class Mother(m: String, c: String)

  case class Wife(w: String, h: String)


  def main(args: Array[String]) {


    val start = System.currentTimeMillis()
    val spark = SparkSession.builder().appName("test").config("spark.master", "local").config("spark.memory.fraction","0.2").config("spark.memory.storageFraction","0.2").getOrCreate()
    spark.sparkContext.setCheckpointDir("/usr/local/Cellar/spark-2.2.0/checkpoint/")



    import org.apache.log4j.{Level, Logger}
    import spark.implicits._

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)


    //val start = System.currentTimeMillis()

    val par = Seq(Par("george", "dorothy"), Par("george", "evelyn"), Par("dorothy", "bertrand"), Par("hilary", "ann"), Par("evelyn", "charles"), Par("dorothy", "ann")).toDF()
    // val par = Seq(Par("george", "dorothy"), Par("dorothy", "bertrand"), Par("hilary", "ann"), Par("dorothy", "ann")).toDF()
    val person = Seq(Person("ann"), Person("bertrand"), Person("charles"), Person("dorothy"), Person("evelyn"), Person("fred"), Person("george"), Person("hilary")).toDF()

    //val person = Seq(Person("ann"), Person("ed"), Person("jack"), Person("jeff"), Person("john"), Person("lena"), Person("mary"), Person("tony")).toDF()
    val wife = Seq(Wife("ann", "ed"), Wife("lena", "jeff"), Wife("mary", "john")).toDF()
    val mother = Seq(Mother("ann", "tony"), Mother("ann", "lena"), Mother("mary", "ann"), Mother("mary", "jack")).toDF()
    person.cache()
    person.createOrReplaceTempView("person")
    wife.cache()
    wife.createOrReplaceTempView("wife")
    mother.cache()
    mother.createOrReplaceTempView("mother")


    par.cache()
    par.createOrReplaceTempView("par")
    broadcast(par)

    val path = Seq(Path("1", "2"), Path("2", "3"), Path("3", "4"), Path("4", "5"), Path("5","6"),
      Path("6","7"), Path("7","8"), Path("8","9"), Path("9","10"),Path("10","11"),Path("11","12"), Path("12","13"),Path("13","14"),Path("14","15"), Path("15","16"), Path("16","17")).toDF()

    path.createOrReplaceTempView("path")
      //broadcast(path)

    // path.cache()

    val s1 =
      "sgc(X,X) :-  magic(X), person(X). " +
        "sgc(X,Y) :- magic(X),par(XP,X), sgc(XP,YP), par(YP, Y)." +
        "magic('ann')." +
        "magic(XP) :- magic(X),par(XP, X). "

    val s5 = "sgc(X,X) :- person(X). " +
      "sgc(X,Y) :- par(XP,X), sgc(XP,YP), par(YP, Y). "

    val s5q = "sgc('ann',X)?"

    val s3 = "tc(X,Y) :- path(X , Y). " +
      "tc(X,Y) :- tc(X,Z), path(Z,Y)."

    val s4 = "tc(X , Y)?"

    val s2 = " samegen(X, 'jack')? "

    val s6 = "sgc(X,X) :- person(X). " +
      "sgc(X,Y) :- par(XP,X), sgc(XP,YP), par(YP, Y) . "


    val s7 = "samegen(X,X) :- person(X) ." +
      "samegen(X,Y) :- parent(U,Y), samegen(Z,U), parent(Z,X) ." +
      "parent(X,Y) :- father(X,Y)." +
      "parent(X,Y) :- mother(X,Y)." +
      "father(X,Y) :- mother(Z,Y), wife(Z,X)."

    val s8 = "m_sgc_bf(XP) :- m_sgc_bf(X), par(XP,X) ." +
      "sgc_bf(X,X) :- m_sgc_bf(X), person(X) ." +
      "sgc_bf(X,Y) :- m_sgc_bf(X), par(XP,X), sgc_bf(XP,YP), par(YP,Y) ." +
      "m_sgc_bf('ann') ."


    val datalogProgram: DatalogProgram =
      DatalogParser(s1) match {
        case Right(dp) => dp
        case Left(ex) => throw ex
      }


    println(datalogProgram)

    val exe = new Evaluator(datalogProgram, spark)
    //val magic = new MagicSet(datalogProgram,spark)
    exe.semi_naive()
    // magic.magic_set()
    //  spark.catalog.listTables()
   // spark.table("tc").show()
    // spark.table("query").show()
    spark.table("sgc").show()

    println("time:" + ((System.currentTimeMillis() - start) / 1000.0) +"," + spark.table("sgc").count() )

  }

}
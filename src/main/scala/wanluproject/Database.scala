package wanluproject

import scala.collection.mutable.ArrayBuffer

object Database {

  /*val relationsSchema = HashMap[String, StructType]()

  val relations = HashMap[String, DataFrame]()

  def addRelation(name: String, df: DataFrame) = {

    relations += (name -> df)

    if (!relationsSchema.contains(name))
      relationsSchema += (name -> df.schema)
  } */

  val relationList = ArrayBuffer[String]()

  def addRelation(name: String) = {
    relationList += name

  }

}

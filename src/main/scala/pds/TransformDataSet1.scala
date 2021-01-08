package pds


import com.mongodb.spark.MongoSpark
import com.mongodb.spark.sql.toMongoDataFrameWriterFunctions
import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

import scala.util.Random



object TransformDataSet1 {

  def associateMetier: String ={


    val random = new Random
    val possibleRepliesToComputer = Seq(
      "Administration",
      "Cuisinier",
      "Jardinier",
      "Médecin",
      "Infirmier",
      "Front office",
      "Agent de maintenance",
      "Gardien",
      "Agents de nettoyage ")


    val reply = possibleRepliesToComputer(
      random.nextInt(possibleRepliesToComputer.length)
    )
    return reply
  }

  def generateRandom(): Int={
    val random = new Random
    val possibleRepliesToComputer = Seq(
      2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015)



    val reply = possibleRepliesToComputer(
      random.nextInt(possibleRepliesToComputer.length)
    )
    return reply
  }



  def generateRandom2(): Int={

    val random = new Random
    val possibleRepliesToComputer = Seq(
      2015,2016,2017,2018,2019,2020)

    val reply = possibleRepliesToComputer(
      random.nextInt(possibleRepliesToComputer.length)
    )
    return reply

  }
  def tranformData(spark:SparkSession): Unit ={

    val df = spark.read.csv("adult.data")

    //rename columns
    val newNames = Seq("age", "workclass", "fnlwgt", "education","educationNum","maritalStatus", "occupation","relationship","race","sex","capitalgain","capitalloss","hoursPerWeek","nativeCountry","salary")
    val dfRenamed = df.toDF(newNames: _*)

    //multiplier le dataset * 3
    val dfRenamed2 = df.toDF(newNames: _*)
    val dfRenamed3 = df.toDF(newNames: _*)

    val dfMultipliate1 = dfRenamed.union(dfRenamed2)
    val dfMultipliate2 = dfMultipliate1.union(dfRenamed3)

    import spark.implicits._



    val dfg = (1 to 97683)
      .map(id => (generateRandom(),generateRandom2(),associateMetier))
      .toDF("startDate","endDate","job")




    val finalDF = dfMultipliate2.crossJoin(dfg)

    println(dfRenamed.count())
    println(dfMultipliate2.count())
    //println(finalDF.count())
    finalDF.show()


    //Send to mongoDB
    MongoSpark.save(finalDF)

  }

}

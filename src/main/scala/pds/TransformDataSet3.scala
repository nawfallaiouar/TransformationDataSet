package pds

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.monotonically_increasing_id

import scala.util.Random

object TransformDataSet3 {

  def associateMaladies: String ={


    val random = new Random
    val possibleRepliesToComputer = Seq(
      "maladies cardiovasculaires",
      "maladies respiratoires",
      "maladies et troubles oculaires",
      "Mobilité réduite + maladies respiratoires ",
      "Mobilité réduite",
      "maladies cardiovasculaires + maladies respiratoires",
      "troubles oculaires + maladies cardiovasculaires",
      "maladies cardiovasculaires + Mobilité réduite",
      "maladies respiratoires + Mobilité réduite")


    val reply = possibleRepliesToComputer(
      random.nextInt(possibleRepliesToComputer.length)
    )
    return reply
  }
  def generateRandom(): Int ={

    val r = Random.nextInt(15000)
    return r

  }



  def tranformData(spark:SparkSession, file: String): Unit = {


    val df = spark.read.option("header",true).csv(file)
    //df.show()
    println(df.count())

    val df1 = df.toDF()
    val df2 = df.toDF()



    //Multiplier le DataFrame
    val dfMult = df1.union(df)
    val dfMult2 = dfMult.union(df2)
    val dfMult3 = dfMult2.union(dfMult)
    val dfMult4 = dfMult3.union(dfMult2)
    val dfMult5 = dfMult4.union(dfMult3)
    val dfMult6 = dfMult5.union(dfMult4)
    val dfMult7 = dfMult6.union(dfMult5)
    val dfMult8 = dfMult7.union(dfMult6)

    import spark.implicits._

    println(dfMult8.count())

    //Associer une maladie
    val dfg = (1 to 16665)
      .map(id => (generateRandom(), associateMaladies))
      .toDF("Appartment","Diseases")


    val finalDF = dfMult8.crossJoin(dfg)


    finalDF.show()

    //Send to mongoDB
    MongoSpark.save(finalDF)


  }

}

package pds

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.SparkSession

import scala.util.Random

object TransformDataSet2 {

  def generateRandom(): Int ={

    val r = Random.nextInt(15000)
    return r

  }

  def tranformData(spark:SparkSession, file: String): Unit = {

    val df = spark.read.option("header",true).csv(file)

    //Drop column

    val df2 = df.drop ("Home office [kW]")

    val df3 = df2.drop ("Garage door [kW]")

    val df4 = df3.drop ("Wine cellar [kW]")

    val df5 = df4.drop ("Barn [kW]")

    //Multiplier le DataFRame pour avoir un nombre suffisant de capteurs

    val df6 = df5.toDF()
    val df7 = df5.toDF()

    val dfMult = df6.union(df7)
    val dfMult2 = dfMult.union(df7)
    val dfMulti = dfMult2.union(dfMult)

    //println(dfMulti.count())

    import spark.implicits._

    // Associer un appartement à chaque message
    val dfg = (1 to 2519555)
      .map(id => (generateRandom()))
      .toDF("Appartment")


    val dfFinal = dfMulti.crossJoin(dfg)
    dfFinal.show(3)

    //Send to mongoDB
    MongoSpark.save(dfFinal)

  }

}

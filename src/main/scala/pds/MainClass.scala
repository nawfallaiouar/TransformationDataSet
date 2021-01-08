package pds

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object MainClass {

  def main(args: Array[String]) {

    val db_url="172.31.249.102:27017"
    val db_name="test"
    val db_user="smartdata"
    val db_pwd="smartdata"
    val db_collection = "personnel"


    Logger.getLogger("org").setLevel(Level.OFF)

    val spark : SparkSession = new ConnectionToMongoDB().Session(db_url,db_name, db_user , db_pwd,db_collection)
    //implicit val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()
    //val conf = new SparkConf().setAppName("TransformationDataSet").setMaster("local")
    //val sc = new SparkContext(conf)

    TransformDataSet1.tranformData(spark)


  }

}

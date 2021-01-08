package pds

import org.apache.spark.sql.SparkSession

class ConnectionToMongoDB {
  def Session(db_url : String, db_name: String, db_user : String, db_pwd : String , collection_name:String): SparkSession ={

    val spark = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnector")
      .config("spark.mongodb.input.uri", "mongodb://"+db_user+":"+db_pwd+"@"+db_url+"/"+db_name+"."+ collection_name)
      .config("spark.mongodb.output.uri", "mongodb://"+db_user+":"+db_pwd+"@"+db_url+"/"+db_name+"."+ collection_name)
      .getOrCreate()
      return  spark

  }
}

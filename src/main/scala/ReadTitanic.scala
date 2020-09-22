import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object ReadTitanic {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession
  .builder()
  .appName("Spark Titanic")
  .getOrCreate()

//   val conf = new SparkConf()
//   .setAppName("firstMlProject")
//   .setMaster("master")

//   val sc = new SparkContext(conf)

// For implicit conversions like converting RDDs to DataFrames
import spark.implicits._

loadTrainSample(spark)

    }
    
    private def loadTrainSample(spark: SparkSession): Unit = {
        val df = spark.read.format("csv")
        .option("sep", ",")
        .option("inferSchema", "true")
        .option("header", "true")
        .load("data/train.csv")

        df.show()
        import spark.implicits._

        df.printSchema()

        df.select("Name").show()

        df.filter($"Sex" ==="female").show()

        df.groupBy("Survived").count().show()

        df.createOrReplaceTempView("titanic")
        val sqlSurvivorsDf = spark.sql("SELECT Pclass, Sex, Age, SibSp, Parch, Fare, Cabin, Embarked FROM titanic WHERE Survived=1")
        sqlSurvivorsDf.show()

        //sqlSurvivorsDf.map(survivor => "Sex: " + survivor.getAs[String]("Sex"))
        //sqlSurvivorsDf.map(survivor => survivor.getValuesMap[String](List("name", "age"))).collect()

    }
}

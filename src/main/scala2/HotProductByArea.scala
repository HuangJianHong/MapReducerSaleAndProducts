import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


/**
  * 商品表：原则：用不到的数据，不要导入
  */
case class ProductInfo(product_id: String, product_name: String)

//地区表
case class AreaInfo(area_id: String, area_name: String)

//经过清洗后的，用户点击日志信息
case class LogInfo(user_id: String, user_ip: String, product_id: String, click_time: String, action_type: String, area_id: String)

/**
  * 另外的示例：
  *
  * 使用SparkSession： 包含sqlContext， SparkContext
  */
object HotProductByArea {

  def main(args: Array[String]): Unit = {
    //Fixme 去除掉resources目录下的 log4j.properties文件，不然日志过多影响Spark信息查看

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //创建一个SparkSession
    val sparkSession: SparkSession = SparkSession.builder().master("local").appName("HotProductByArea").getOrCreate()

    import sparkSession.sqlContext.implicits._

    //获取地区数据
    val areaDF = sparkSession.sparkContext.textFile("D:\\temp\\project04\\area\\areainfo.txt")
      .map(_.split(",")).map(x => new AreaInfo(x(0), x(1))).toDF()
    areaDF.createOrReplaceTempView("area")


    //获取商品数据
    val productDF = sparkSession.sparkContext.textFile("D:\\temp\\project04\\product\\productinfo.txt")
      .map(_.split(",")).map(x => new ProductInfo(x(0), x(1))).toDF()
    productDF.createOrReplaceTempView("product")

    //获取点击日志
    val clickLogDF = sparkSession.sparkContext.textFile("D:\\temp\\project04\\clicklog\\userclicklog.txt")
      .map(_.split(",")).map(x => new LogInfo(x(0), x(1), x(2).substring(x(2).indexOf("=") + 1), x(3), x(4), x(5)))
      .toDF()
    clickLogDF.createTempView("clicklog")

    //执行SQL
    val sql = "select a.area_id,a.area_name,p.product_id,product_name,count(c.product_id) " +
      " from area a,product p,clicklog c where a.area_id=c.area_id " +
      " and p.product_id=c.product_id " +
      " group by a.area_id,a.area_name,p.product_id,p.product_name " +
      " order by p.product_id"
    sparkSession.sql(sql).show()

    sparkSession.stop()
  }


}

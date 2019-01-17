import java.lang.Double

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}


//定义case class代表Schema
//商品表
case class Product(prod_id: Int, prod_name: String)

//订单表
case class SaleOrder(prod_id: Int, year_id: Int, amount: Double)

object ProductSalesInfo {


  def main(args: Array[String]): Unit = {
    //无用的日志过滤
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //初始化相关sparkContext
    val config: SparkConf = new SparkConf().setAppName("ProductSalesInfo").setMaster("local")
    val sc = new SparkContext(config)
    val sqlContext = new SQLContext(sc)


    import sqlContext.implicits._

    //第一步：取出商品的数据
    val productDF = sc.textFile("D:\\temp\\products").map(line => {
      // 13,5MP Telephoto Digital Camera,5MP Telephoto Digital Camera,Cameras,2044,Cameras,Photo,204,Photo,1,U,P,1,STATUS,899.99,899.99,TOTAL,1,,01-JAN-98,,A

      val strings: Array[String] = line.split(",")
      val product_id: Int = Integer.parseInt(strings(0))
      val product_name: String = strings(1)
      //返回： 商品的ID和商品的名称
      (product_id, product_name)
    }).map(p => Product(p._1, p._2)).toDF()

    //第二步：取出订单的数据
    val salesDF = sc.textFile("D:\\temp\\sales").map(line => {
      //13,987,1998-01-10,3,999,1,1232.16
      //返回： 商品的ID、年份、金额
      val strings: Array[String] = line.split(",")
      val product_id: Int = Integer.parseInt(strings(0))
      val year_id: Int = Integer.parseInt(strings(2).substring(0, 4))
      val amount = Double.parseDouble(strings(5))

      (product_id, year_id, amount)
    }).map(s => SaleOrder(s._1, s._2, s._3)).toDF()


    //注册表视图
    productDF.createOrReplaceTempView("productsView")
    salesDF.createOrReplaceTempView("salesView")

    //使用SparkSql进行分析,查询
    var sql = "select prod_name, year_id, sum(amount) total" +
      " from productsView, salesView" +
      " where productsView.prod_id = salesView.prod_id" +
      " group by prod_name, year_id"

    val result: DataFrame = sqlContext.sql(sql).toDF("prod_name","year_id","total")
    result.createOrReplaceTempView("resultView")
    //查询出结果,不同年份的，然后行转列
    val sql2 = "select prod_name ,  " +
      " sum(case year_id when 1998 then total else 0 end) year_1998 , " +
      " sum(case year_id when 1999 then total else 0 end) year_1999 , " +
      " sum(case year_id when 2000 then total else 0 end) year_2000 ," +
      " sum(case year_id when 2001 then total else 0 end) year_2001 " +
      " from resultView group by prod_name"
    sqlContext.sql(sql2).show()

    sc.stop()
  }



  /**
    * Oracle语法
    * 需求2：求每年每种商品的销售总额，要求显示结果:
    * 商品名称     年份和金额
    * 1、复习：多表查询、子查询
    * 2、SQL中的条件表达式
    * 3、行转列
    **/
  /** --1、得到每种商品的名字、年份、订单的金额
    * --2、实现行转列: SQL中的条件表达式 if  ...else 语句 ----> decode函数是Oracle中来实现
    * select prod_name,
    * sum(decode(year_id,1998,total,0)) "1998",
    * sum(decode(year_id,1999,total,0)) "1999",
    * sum(decode(year_id,2000,total,0)) "2000",
    * sum(decode(year_id,2001,total,0)) "20001"
    * from (select prod_name,year_id,sum(amount_sold) total
    * from (select products.prod_name,to_char(time_id,'yyyy') year_id,sales.amount_sold
    * from products,sales
    * where products.prod_id = sales.prod_id)
    * group by prod_name,year_id)
    * group by prod_name;
    */

}

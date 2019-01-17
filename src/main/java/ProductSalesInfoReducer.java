import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

//                                                                      k4商品名称   v4（年份+金额）
public class ProductSalesInfoReducer extends Reducer<IntWritable, Text, Text, Text> {

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        //定义一个HashMap保存每一年的订单金额
        HashMap<String, Double> hashMap = new HashMap<String, Double>();
        //商品的名称
        String productName = "";

        for (Text value : values) {
            //判断是来自哪个表的数据
            if (value.toString().contains("name:")) {  //products信息
                productName = value.toString().substring(5);

            } else {   //sales信息
                //表示订单信息  订单的年份、金额    1998:1232.16
                String data = value.toString();
                String[] datas = data.split(":");

                String year = datas[0];
                String money = datas[1];

                if (hashMap.containsKey(year)) {
                    //如果已经有订单，累计;
                    //因为这个的金额，来源都是同一个key，也就是同一个商品，不同的年份
                    hashMap.put(year, hashMap.get(year) + Double.parseDouble(money));
                } else {
                    hashMap.put(year, Double.parseDouble(money));
                }
            }
        }
        //输出
        context.write(new Text(productName), new Text(hashMap.toString()));


        /**
         * 输出结果示例：
         * 5MP Telephoto Digital Camera	{1998=936197.5299999929, 2001=2205836.659999986, 2000=2128961.350000002, 1999=1041272.8600000041}
         * 17" LCD w/built-in HDTV Tuner	{1998=2733887.4300000593, 2001=1874621.9599999986, 2000=930537.1500000068, 1999=1650125.2299999825}
         * Envoy 256MB - 40GB	{1998=1368317.879999989, 2001=2230713.3900000085, 2000=758428.6899999911, 1999=1278503.1199999822}
         * Y Box	{1998=11.99, 2001=1205027.3499999733, 2000=604389.7799999866, 1999=272901.1799999997}
         * Mini DV Camcorder with 3.5" Swivel LCD	{1998=2239127.8799999924, 2001=2819074.980000001, 2000=1219843.9499999916, 1999=2036768.5899999968}
         * Envoy Ambassador	{1998=5477218.0400001, 2001=3453656.6199999684, 2000=3578027.7099998747, 1999=2502740.1500000274}
         * Laptop carrying case	{1998=182670.34999999622, 2001=182362.7699999944, 2000=54956.20999999924, 1999=203891.79000000143}
         * Home Theatre Package with DVD-Audio/Video Play	{1998=990525.9499999924, 2001=1675991.8499999181, 2000=2091346.2700000284, 1999=1934132.7400000459}
         * 18" Flat Panel Graphics Monitor	{1998=1535187.439999983, 2001=1316903.970000001, 2000=1755049.8499999866, 1999=891586.5499999969}
         * Envoy External Keyboard	{1998=31853.110000000255, 2001=40583.31000000032, 2000=8264.989999999998, 1999=8055.390000000003}
         * External 101-key keyboard	{1998=85211.28000000266, 2001=134081.61000000028, 2000=115571.33999999659, 1999=121704.4700000035}
         * PCMCIA modem/fax 28800 baud	{1998=163929.2699999959, 2001=501192.9900000126, 2000=161240.0399999957, 1999=307714.1199999922}
         * SIMM- 8MB PCMCIAII card	{1998=522713.70999997616, 2001=581958.0999999965, 2000=596100.2000000051, 1999=674725.9099999985}
         * ...
         */
    }
}

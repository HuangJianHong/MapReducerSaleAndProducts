import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

//                                                                    k2是商品的ID   v2是商品的信息或者订单信息
public class ProductSalesInfoMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 输入的数据：订单、商品
        /**
         * 使用判断文件名的方式
         * 正式环境可以使用判断数据库表名的方式
         */
        //得到输入的HDFS的路径 :------>  /input/sh/sales
        String path = ((FileSplit) context.getInputSplit()).getPath().getName();
        //得到文件名
        String fileName = path.substring(path.lastIndexOf("/") + 1);


        String data = value.toString();
        String[] split = data.split(",");

        //输出
        if (fileName.contains("products")) {
            //输出商品信息
            context.write(new IntWritable(Integer.parseInt(split[0])), new Text("name:" + split[1]));
        } else {
            //输出订单信息       商品的ID            订单的年份、金额
            context.write(new IntWritable(Integer.parseInt(split[0])), new Text(split[2].substring(0, 4) + ":" + split[6]));
        }
    }
}

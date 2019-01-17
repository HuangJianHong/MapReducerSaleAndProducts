import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * MapReducer多表查询
 * <p>
 * 输入数据
 * products：
 * 13,5MP Telephoto Digital Camera,5MP Telephoto Digital Camera,Cameras,2044,Cameras,Photo,204,Photo,1,U,P,1,STATUS,899.99,899.99,TOTAL,1,,01-JAN-98,,A
 * sales:
 * 13,987,1998-01-10,3,999,1,1232.16
 */
public class ProductSalesInfoMain {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1、创建Job
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(ProductSalesInfoMain.class);

        //2、指定任务的Mapper和输出的类型
        job.setMapperClass(ProductSalesInfoMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        //3、指定任务的Reducer和输出的类型
        job.setReducerClass(ProductSalesInfoReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //输入： temp目录下的所有文件
        //输出： temp目录下的output文件夹
        args = new String[]{"D:\\temp", "D:\\temp\\output"};

        //4、任务的输入和输出
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //5、执行
        job.waitForCompletion(true);
    }
}

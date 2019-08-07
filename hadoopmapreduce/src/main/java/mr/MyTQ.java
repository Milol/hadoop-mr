package mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import tq.*;

import java.io.IOException;

/**
 * @ClassName MyTQ
 * @Description:TODO
 * @Auther: liuwenshuai@100tal.com
 * @Date: 2019/8/5 16:05
 * @Version 1.0
 */
public class MyTQ {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1 配置
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(MyTQ.class);
        job.setJobName("tq");

        //2 设置输入输出路径
        Path inPath = new Path("/tq/input");
        FileInputFormat.addInputPath(job, inPath);
        Path outPath = new Path("/tq/output");
        if(outPath.getFileSystem(conf).exists(outPath)) {
            outPath.getFileSystem(conf).delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, outPath);

        //设置mapper
        job.setMapperClass(Tmapper.class);
        job.setMapOutputKeyClass(Tq.class);
        job.setMapOutputValueClass(IntWritable.class);


        //4 自定义比较器
        job.setSortComparatorClass(TSortComparator.class);

        //5.自定义分区器
        job.setPartitionerClass(TPartitioner.class);


        //6 自定义组排序器
        job.setGroupingComparatorClass(TGroupingComparator.class);

        //7 设置reducetask数量
        job.setNumReduceTasks(2);

        //8设置reducer
        job.setReducerClass(Treduce.class);

        job.waitForCompletion(true);

    }
}

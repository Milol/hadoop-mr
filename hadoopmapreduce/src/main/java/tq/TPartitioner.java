package tq;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @ClassName TPartitioner
 * @Description:TODO
 * @Auther: liuwenshuai@100tal.com
 * @Date: 2019/8/5 16:55
 * @Version 1.0
 */
public class TPartitioner extends Partitioner<Tq, IntWritable> {
    @Override
    public int getPartition(Tq key, IntWritable val, int i) {
        return key.getYear() % i;
    }
}

package tq;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName Treduce
 * @Description:TODO
 * @Auther: liuwenshuai@100tal.com
 * @Date: 2019/8/5 17:07
 * @Version 1.0
 */
public class Treduce extends Reducer<Tq, IntWritable, Text, IntWritable> {

    Text tkey = new Text();
    IntWritable tval = new IntWritable();
    @Override
    protected void reduce(Tq key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int flag = 0;
        int day = 0;
        for (IntWritable val :values) {
            if(flag == 0) {
                tkey.set(key.toString());
                tval.set(val.get());
                context.write(tkey, tval);
                flag++;
                day = key.getDay();
            }
            if(flag != 0 && day != key.getDay()) {
                tkey.set(key.toString());
                tval.set(val.get());
                context.write(tkey, tval);
                return;
            }
        }
    }
}

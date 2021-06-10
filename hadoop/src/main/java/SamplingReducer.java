import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SamplingReducer extends Reducer<IntWritable, PointWritable, NullWritable, PointWritable> {

    static int K;
    static int meansCount;

    public void setup(Context context){
        Configuration conf = context.getConfiguration();
        K = Integer.parseInt(conf.get("k"));

        meansCount = 0;
    }

    public void reduce(IntWritable key, Iterable<PointWritable> values, Context context) throws IOException, InterruptedException {
        for (PointWritable p: values){
            if (meansCount > K)
                return;

            context.write(null, p);
            meansCount++;
        }
    }
}

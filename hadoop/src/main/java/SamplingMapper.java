import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.PriorityQueue;
import java.util.Random;

public class SamplingMapper extends Mapper<LongWritable, Text, IntWritable, PointWritable> {
    static int K;
    final static Random rand = new Random();
    final static IntWritable outputKey = new IntWritable();
    final static PointWritable outputValue = new PointWritable();

    final static PriorityQueue<PriorityPoint> priorityQueue = new PriorityQueue<>();

    public void setup(Context context){
        Configuration conf = context.getConfiguration();
        K = Integer.parseInt(conf.get("k"));
        rand.setSeed(System.currentTimeMillis());
    }

    public void map(LongWritable key, Text value, Context context) {
        priorityQueue.add(new PriorityPoint(rand.nextInt(), value.toString()));

        if (priorityQueue.size() > K)
            priorityQueue.poll();
    }

    public void cleanup(Context context) throws IOException, InterruptedException {

        for (PriorityPoint priorityPoint: priorityQueue){
            outputKey.set(priorityPoint.getPriority());
            outputValue.set(priorityPoint .getCoordinates());
            context.write(outputKey, outputValue);
        }
    }
}
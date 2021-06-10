import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class ErrorComputingMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    final static Text outputKey = new Text();
    final static DoubleWritable outputValue = new DoubleWritable();
    final static List<PointWritable> meansList = new ArrayList<>() ;
    static double distanceAccumulator;

    public void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        distanceAccumulator = 0.0;
        meansList.clear();
        URI[] cacheFiles = context.getCacheFiles();
        FileSystem fs = FileSystem.get(conf);

        for (URI file: cacheFiles) {
            InputStream is = fs.open(new Path(file));
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
            String line;

            while ((line = reader.readLine()) != null) {
                PointWritable mean = new PointWritable(line);
                meansList.add(mean);
            }

            reader.close();
        }
    }

    public void map(LongWritable key, Text value, Context context) {

        double mininumDistance = Double.POSITIVE_INFINITY;

        PointWritable p = new PointWritable(value.toString());

        for (PointWritable m: meansList){
            double distance = p.getSquaredDistance(m);
            if (distance < mininumDistance){
                mininumDistance = distance;
            }
        }

        distanceAccumulator += mininumDistance;
    }

    public void cleanup(Context context) throws IOException, InterruptedException {

        outputKey.set("key");
        outputValue.set(distanceAccumulator);
        context.write(outputKey, outputValue);
    }
}
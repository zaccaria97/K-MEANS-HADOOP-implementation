import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class ClusteringMapper extends Mapper<LongWritable, Text, PointWritable, PointsBinder> {

    final static Map<PointWritable, PointsBinder> centroidAccumulatorMap = new HashMap<>();
    static int dimension;


    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        dimension = Integer.parseInt(conf.get("d"));
        centroidAccumulatorMap.clear();

        /* Get the means from cache, either sampled or computed in the previous step */
        URI[] cacheFiles = context.getCacheFiles();
        FileSystem fs = FileSystem.get(conf);

        for (URI f: cacheFiles) {
            InputStream is = fs.open(new Path(f));
            BufferedReader br = new BufferedReader(new InputStreamReader(is));

            String line;
            while ((line = br.readLine()) != null) {
                PointWritable mean = new PointWritable(line);
                centroidAccumulatorMap.put(mean, new PointsBinder(dimension));
            }

            br.close();
        }
    }

    public void map(LongWritable key, Text value, Context context) {

        double minDistance = Double.POSITIVE_INFINITY;
        PointWritable closestMean = null;

        PointWritable p = new PointWritable(value.toString());

        for (PointWritable m: centroidAccumulatorMap.keySet()){
            double d = p.getSquaredDistance(m);
            if (d < minDistance){
                minDistance = d;
                closestMean = m;
            }
        }

        PointsBinder ap = centroidAccumulatorMap.get(closestMean);
        ap.add(p);
        centroidAccumulatorMap.put(closestMean, ap);
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<PointWritable, PointsBinder> entry: centroidAccumulatorMap.entrySet()){
            context.write(entry.getKey(), entry.getValue());
        }
    }
}
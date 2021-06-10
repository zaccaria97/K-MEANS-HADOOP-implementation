import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ClusteringReducer extends Reducer<PointWritable, PointsBinder, NullWritable, PointWritable> {

    static int dimension;

    public void setup(Context context){
        Configuration conf = context.getConfiguration();
        dimension = Integer.parseInt(conf.get("d"));
    }

    public void reduce(PointWritable key, Iterable<PointsBinder> values, Context context) throws IOException, InterruptedException {

        PointWritable centroid = new PointWritable(dimension);
        int n = 0;

        for (PointsBinder ap: values){
            centroid.add(ap);
            n += ap.getSize();
        }
        centroid.divide(n);
        context.write(null, centroid);
    }
}


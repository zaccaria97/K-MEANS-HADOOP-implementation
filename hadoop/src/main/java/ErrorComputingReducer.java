import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ErrorComputingReducer extends Reducer<Text, DoubleWritable, NullWritable, DoubleWritable> {

    final static DoubleWritable outputValue = new DoubleWritable();
    static double ErrorSum;

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

        ErrorSum = 0.0;
        for (DoubleWritable value: values)
            ErrorSum += value.get();

        outputValue.set(ErrorSum);
        context.write(null, outputValue);
    }
}

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import java.io.*;
import java.net.URI;

public class kMeans {

    static String ROOT_DIR;
    static FileSystem fileSystem;
    static Configuration config;
    static double threshold;
    static int maxIterations;


    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        config = new Configuration();
        Configurator conf = new Configurator("conf.ini");

        maxIterations = conf.getMaxIterations();
        threshold = conf.getThreshold();
        ROOT_DIR = conf.getOutputPath() + "/";
        config.set("input", conf.getInputPath());
        config.set("randomCentroids", ROOT_DIR + "random-centroids");
        config.set("tmpCentroids", ROOT_DIR + "tmp-centroids");
        config.set("centroids", ROOT_DIR + "centroids");
        config.set("convergence", ROOT_DIR + "convergence");
        config.setInt("d", conf.getPointDimension());
        config.setInt("k", conf.getK());
        config.setInt("maxNumberOfReduceTasks", conf.getMaxNumberOfReduceTasks());


        fileSystem = FileSystem.get(config);
        fileSystem.delete(new Path(ROOT_DIR), true);

        long beginInstant = System.nanoTime();
        Job sampling = Job.getInstance(config, "centroids sampling");

        if (!configureSamplingJob(sampling)) {
            fileSystem.close();
            System.exit(1);
        }


        double cumulativeError = Double.POSITIVE_INFINITY;
        double prevCumulativeError = Double.POSITIVE_INFINITY;
        double percentageVariation=Double.POSITIVE_INFINITY;
        int currentIteration = 0;

        while ((percentageVariation > threshold && currentIteration < maxIterations) || prevCumulativeError == Double.POSITIVE_INFINITY ){
            prevCumulativeError = cumulativeError;

            Path srcDir = (currentIteration == 0) ? new Path(config.get("randomCentroids")) : new Path(config.get("centroids"));
            Path dstDir = new Path(config.get("tmpCentroids"));

            fileSystem.mkdirs(dstDir);
            copyDir(srcDir, dstDir);
            deleteFiles();

            Job clustering = Job.getInstance(config, "clustering");
            addCacheDirectory(new Path(config.get("tmpCentroids")), clustering);
            if ( !configureClusteringJob(clustering) ) {
                fileSystem.close();
                System.exit(1);
            }

            Job errorComputing = Job.getInstance(config, "error computing");
            addCacheDirectory(new Path(config.get("centroids")), errorComputing);
            if ( !configureErrorComputingJob(errorComputing) ) {
                fileSystem.close();
                System.exit(1);
            }
            //      System.out.printf("FINAL CENTROIDS: %s\n\n", getFinalCentroids());
            cumulativeError = getCumulativeError();

            percentageVariation = (prevCumulativeError - cumulativeError)/prevCumulativeError * 100;

            System.out.printf("\nSTEP: %d - PREV_OBJ_FUNCTION: %f - OBJ_FUNCTION: %f - CHANGE: %.2f%%\n\n", currentIteration, prevCumulativeError, cumulativeError, percentageVariation);
            currentIteration++;
        }
        long endInstant = System.nanoTime();

        System.out.printf("FINAL CENTROIDS:\n %s\n\n", getFinalCentroids());
        //compute execution time for statistics
        long executionTime = (endInstant - beginInstant);
        saveExecutionTime(executionTime, config);
        fileSystem.close();

    }


    //Save execution time for Statistics
    public static void saveExecutionTime(long executionTime, Configuration conf) throws IOException {
        Path pathStats = new Path(ROOT_DIR + "executionTime.txt");
        FileSystem fsStats = FileSystem.get(URI.create(pathStats.toString()), conf);

        FSDataOutputStream outStats;

        if (!fsStats.exists(pathStats)) {
            outStats = fsStats.create(pathStats);
        }
        else{
            outStats = fsStats.append(pathStats);
        }

        BufferedWriter brStats = new BufferedWriter(new OutputStreamWriter(outStats, "UTF-8" ));
        long executionTimeInSeconds = executionTime / 1_000_000_000;
        brStats.append(Long.toString(executionTimeInSeconds) + " seconds\n");
        brStats.close();
        fsStats.close();
    }

    public static boolean configureSamplingJob(Job job) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = job.getConfiguration();

        // Set JAR class
        job.setJarByClass(kMeans.class);

        // Set Mapper class
        job.setMapperClass(SamplingMapper.class);

        // Set Reducer class
        job.setReducerClass(SamplingReducer.class);

        // Set key-value output format
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(PointWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(PointWritable.class);

        // Define input and output path file
        FileInputFormat.addInputPath(job, new Path(conf.get("input")));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("randomCentroids")));

        // Exit
        return job.waitForCompletion(true);
    }

    public static boolean configureClusteringJob(Job job) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = job.getConfiguration();
        int K = Integer.parseInt(conf.get("k"));
        int numReduceTasks = Math.min(K, Integer.parseInt(conf.get("maxNumberOfReduceTasks")));

        job.setJarByClass(kMeans.class);

        job.setMapperClass(ClusteringMapper.class);
        job.setReducerClass(ClusteringReducer.class);

        // The best solution would be having one reducer per mean
        job.setNumReduceTasks(numReduceTasks);

        job.setMapOutputKeyClass(PointWritable.class);
        job.setMapOutputValueClass(PointsBinder.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(PointWritable.class);

        // Define input and output path file
        FileInputFormat.addInputPath(job, new Path(conf.get("input")));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("centroids")));

        // Exit
        return job.waitForCompletion(true);
    }

    public static boolean configureErrorComputingJob(Job job) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = job.getConfiguration();

        // Set JAR class
        job.setJarByClass(kMeans.class);

        // Set Mapper class
        job.setMapperClass(ErrorComputingMapper.class);

        // Set Reducer class
        job.setReducerClass(ErrorComputingReducer.class);

        // Set key-value output format
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Define input and output path file
        FileInputFormat.addInputPath(job, new Path(conf.get("input")));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("convergence")));

        // Exit
        return job.waitForCompletion(true);
    }

    public static void deleteFiles() throws IOException {
        fileSystem.delete(new Path(config.get("centroids")),  true);
        fileSystem.delete(new Path(config.get("convergence")),  true);
    }

    public static void copyDir(Path srcPath, Path dstPath) throws IOException {
        RemoteIterator<LocatedFileStatus> fileIter = fileSystem.listFiles(srcPath, true);
        while (fileIter.hasNext()){
            FileUtil.copy(fileSystem, fileIter.next(), fileSystem, dstPath, false, true, config);
        }
    }

    public static void addCacheDirectory(Path dir, Job job) throws IOException {
        RemoteIterator<LocatedFileStatus> fileIter = fileSystem.listFiles(dir, true);
        while(fileIter.hasNext()) {
            job.addCacheFile(fileIter.next().getPath().toUri());
        }
    }

    public static String getFinalCentroids() throws IOException {
        String objFunction = "";
        RemoteIterator<LocatedFileStatus> fileIter = fileSystem.listFiles(new Path(config.get("centroids")), true);
        while (fileIter.hasNext()){
            InputStream is = fileSystem.open(fileIter.next().getPath());
            BufferedReader br = new BufferedReader(new InputStreamReader(is));

            String line;
            while ((line = br.readLine()) != null ) {
                objFunction += line + "; \n ";

            }
            br.close();
        }
        return objFunction;

    }

    public static double getCumulativeError() throws IOException {
        double objFunction;
        InputStream is = fileSystem.open(new Path(config.get("convergence") + "/part-r-00000"));
        BufferedReader br = new BufferedReader(new InputStreamReader(is));

        String line;
        if ((line = br.readLine()) == null ) {
            br.close();
            System.exit(1);
        }

        objFunction = Double.parseDouble(line);
        br.close();
        return objFunction;
    }


}

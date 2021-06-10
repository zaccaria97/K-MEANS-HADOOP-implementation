import org.apache.log4j.BasicConfigurator;
import org.ini4j.Wini;

import java.io.File;
import java.io.IOException;


public class Configurator {

    private int clusteringNumberOfReduceTasks;
    private double threshold;
    private int maxIterations;
    private int pointDimension;
    private int k;
    private String inputPath;
    private String outputPath;


    public Configurator(String configPath) {
        BasicConfigurator.configure();

        try{
            Wini config = new Wini(new File(configPath));

            clusteringNumberOfReduceTasks = Integer.parseInt(config.get("K-means", "maxNumberOfReduceTasks"));
            threshold = Double.parseDouble(config.get("K-means", "threshold"));
            maxIterations = Integer.parseInt(config.get("K-means", "maxIterations"));
            pointDimension = Integer.parseInt(config.get("Dataset", "pointDimension"));
            k = Integer.parseInt(config.get("Dataset", "k"));
            inputPath = config.get("Dataset", "inputPath");
            outputPath = config.get("Dataset", "outputPath");

        } catch(IOException e){
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }
    public int getMaxNumberOfReduceTasks() {
        return clusteringNumberOfReduceTasks;
    }

    public double getThreshold() {
        return threshold;
    }

    public int getMaxIterations() {
        return maxIterations;
    }

    public int getPointDimension() {
        return pointDimension;
    }

    public int getK() {
        return k;
    }

    public String getInputPath() {
        return inputPath;
    }

    public String getOutputPath() {
        return outputPath;
    }

}

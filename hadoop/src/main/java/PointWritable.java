

import java.io.*;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
/* Multiple Dimension Point custom structure */

public class PointWritable implements WritableComparable<Object> {
    private ArrayList<Double> coordinates;

    public PointWritable(){
        this.coordinates = new ArrayList<>();
    }

    public PointWritable(String value){
        this();

        String[] indicesAndValues = value.split(",");
        for (String v: indicesAndValues) {
            coordinates.add(Double.parseDouble(v));
        }
    }

    public PointWritable(int d){
        this();

        for (int i = 0; i < d; i++)
            coordinates.add(0.0);
    }

    public void set(ArrayList<Double> coordinates){
        this.coordinates = coordinates;
    }

    public ArrayList<Double> getCoordinates(){
        return coordinates;
    }

    public double getSquaredDistance(PointWritable that){
        double sum = 0;
        ArrayList<Double> thisCoordinates = this.getCoordinates();
        ArrayList<Double> thatCoordinates = that.getCoordinates();

        for (int i = 0; i < thisCoordinates.size(); i++){
            sum += (thisCoordinates.get(i) - thatCoordinates.get(i))*(thisCoordinates.get(i) - thatCoordinates.get(i));
        }

        return sum;
    }

    public void add(PointWritable that){
        ArrayList<Double> thisCoordinates = this.getCoordinates();
        ArrayList<Double> thatCoordinates = that.getCoordinates();

        for (int i = 0; i < thisCoordinates.size(); i++){
            thisCoordinates.set(i, thisCoordinates.get(i) + thatCoordinates.get(i));
        }
    }

    public void divide(int n){
        for (int i = 0; i < coordinates.size(); i++){
            coordinates.set(i, coordinates.get(i)/n);
        }
    }

    public int hashCode(){
        return new Text(toString()).hashCode();
    }

    public String toString(){
        String[] tmp = new String[coordinates.size()];
        for (int i = 0; i < tmp.length; i++)
            tmp[i] = Double.toString(coordinates.get(i));
        return String.join(",", tmp);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(coordinates.size());
        for (Double c: coordinates)
            out.writeDouble(c);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();

        coordinates = new ArrayList<>();
        for (int i = 0; i < size; i++)
            coordinates.add(in.readDouble());
    }

    @Override
    public int compareTo(Object o) {
        ArrayList<Double> thisCoordinates = this.getCoordinates();
        ArrayList<Double> thatCoordinates = ((PointWritable)o).getCoordinates();

        for (int i = 0; i < thisCoordinates.size(); i++){
            if (thisCoordinates.get(i) < thatCoordinates.get(i))
                return -1;

            if (thisCoordinates.get(i) > thatCoordinates.get(i))
                return 1;
        }

        return 0;
    }
}

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PointsBinder extends PointWritable {
    private int size;

    public PointsBinder(){
        super();
    }

    public PointsBinder(int d){
        super(d);
        size = 0;
    }

    public int getSize() {
        return size;
    }

    public void add(PointWritable that){
        super.add(that);
        size++;
    }

    public String toString(){
        return super.toString() + " " + this.size;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeInt(size);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        size = in.readInt();
    }
}

public class PriorityPoint extends PointWritable implements Comparable<Object> {
    private final int priority;

    public PriorityPoint(int priority, String value){
        super(value);
        this.priority = priority;
    }

    public int getPriority() {
        return priority;
    }

    public int compareTo(Object o) {
        return Integer.compare(((PriorityPoint)o).priority, this.priority);
    }

    public String toString(){
        return super.toString() + " " + this.priority;
    }
}

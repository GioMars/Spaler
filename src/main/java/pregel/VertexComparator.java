package pregel;

import java.util.Comparator;
import scala.Serializable;
import scala.Tuple2;

public class VertexComparator implements Serializable, Comparator<Tuple2<Long, Tuple2<String, Vertex>>>{

    public int compare(Tuple2<Long, Tuple2<String, Vertex>> o1, Tuple2<Long, Tuple2<String, Vertex>> o2) {
        Vertex v1 = o1._2._2;
        Vertex v2 = o2._2._2;
        if (v1.getIndex() < v2.getIndex()){
            return -1;
        } else {
            return +1;
        }
    }
}

package pregel;
import scala.Serializable;
import java.util.Comparator;

public class ContigsMaxLen implements Serializable, Comparator<Contig> {

    public int compare(Contig o1, Contig o2) {
        if (o1.getLen() < o2.getLen()){
            return -1;
        } else {
            return +1;
        }
    }
}


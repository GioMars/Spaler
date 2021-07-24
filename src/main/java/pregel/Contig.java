package pregel;

import java.io.Serializable;

public class Contig implements Serializable {

    long id;
    String contig;
    int len;

    public Contig(long id, String contig, int len){
        this.id = id;
        this.contig = contig;
        this.len = len;
    }

    public Contig(){}

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getContig() {
        return contig;
    }

    public void setContig(String contig) {
        this.contig = contig;
    }

    public int getLen() {
        return len;
    }

    public void setLen(int len) {
        this.len = len;
    }
}

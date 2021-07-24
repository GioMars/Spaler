package pregel;

import java.io.Serializable;

public class Edge implements Serializable{

	long src, dst;
	int freq;
	double val;
	String fvp, svp;			
	// First (Second) Vertex Polarity
	
	public Edge() {}
	public Edge(long src, long dst, int freq, String fvp, String svp) {
		super();
		this.src = src;
		this.dst = dst;
		this.freq = freq;
		this.fvp = fvp;
		this.svp = svp;
	}

	public long getSrc() {
		return src;
	}
	public void setSrc(long src) {
		this.src = src;
	}
	public long getDst() {
		return dst;
	}
	public void setDst(long dst) {
		this.dst = dst;
	}
	public int getFreq() {
		return freq;
	}
	public void setFreq(int freq) {
		this.freq = freq;
	}
	public String getFvp() {
		return fvp;
	}
	public void setFvp(String fvp) {
		this.fvp = fvp;
	}
	public String getSvp() {
		return svp;
	}
	public void setSvp(String svp) {
		this.svp = svp;
	}
	public double getVal() {
		return val;
	}
	public void setVal(double val) {
		this.val = val;
	}

}

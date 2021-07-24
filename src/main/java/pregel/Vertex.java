package pregel;

import java.io.Serializable;

public class Vertex implements Serializable {
	private long id, gid;

	private int index, degree;
	private String pol;

	public Vertex(long id, long gid, int index, int degree, String pol) {
		this.id = id;
		this.gid = gid;
		this.index = index;
		this.degree = degree;
		this.pol = pol;
	}

	public Vertex() {}

	public long getId() {
		return id;
	}
	public void setId(long id) {
		this.id = id;
	}
	public long getGid() {
		return gid;
	}
	public void setGid(long gid) {
		this.gid = gid;
	}
	public int getIndex() {
		return index;
	}
	public void setIndex(int index) {
		this.index = index;
	}
	public String getPol() {
		return pol;
	}
	public void setPol(String pol) {
		this.pol = pol;
	}
	public int getDegree() {
		return degree;
	}
	public void setDegree(int degree) {
		this.degree = degree;
	}
}

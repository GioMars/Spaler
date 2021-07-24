package pregel;
/*
 * Representation object
 * It contains a method useful to implement the 
 * reverse complement for a generic k-mer
 * Rep returns following the lexicographic order
 */

public class Rep {
	private String s;
	
	public Rep(String s) {
		this.s = s;
	}
	
	public String rep() {
		StringBuilder rc = new StringBuilder();
		String output = s;
		char c;
		char charA='A';  
		char charC='C'; 
		char charG='G'; 
		for(int i=s.length()-1; i>=0; i--) {
			c = s.charAt(i);
			if(c == charA) {
				rc.append("T");
			} else if (c == charC) {
				rc.append("G");
			} else if (c == charG) {
				rc.append("C");
			} else {
				rc.append("A");
			}
		}
		
		if(s.compareTo(rc.toString())>0) {
			output = rc.toString();
		}
		
		return output;
	}


	public String repNotOrdered() {
		StringBuilder rc = new StringBuilder();
		char c;
		char charA='A';
		char charC='C';
		char charG='G';
		for(int i=s.length()-1; i>=0; i--) {
			c = s.charAt(i);
			if(c == charA) {
				rc.append("T");
			} else if (c == charC) {
				rc.append("G");
			} else if (c == charG) {
				rc.append("C");
			} else {
				rc.append("A");
			}
		}

		return rc.toString();
	}

}

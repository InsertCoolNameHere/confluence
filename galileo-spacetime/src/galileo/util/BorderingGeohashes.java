package galileo.util;

import java.util.ArrayList;
import java.util.List;

public class BorderingGeohashes {
	
	List<String> n;
	List<String> s;
	List<String> e;
	List<String> w;
	String ne;
	String se;
	String nw;
	String sw;
	
	public BorderingGeohashes() {
		n = new ArrayList<String>();
		s = new ArrayList<String>();
		e = new ArrayList<String>();
		w = new ArrayList<String>();
	}

	public List<String> getN() {
		return n;
	}

	public void setN(List<String> n) {
		this.n = n;
	}

	public List<String> getS() {
		return s;
	}

	public void setS(List<String> s) {
		this.s = s;
	}

	public List<String> getE() {
		return e;
	}

	public void setE(List<String> e) {
		this.e = e;
	}

	public List<String> getW() {
		return w;
	}

	public void setW(List<String> w) {
		this.w = w;
	}

	public String getNe() {
		return ne;
	}

	public void setNe(String ne) {
		this.ne = ne;
	}

	public String getSe() {
		return se;
	}

	public void setSe(String se) {
		this.se = se;
	}

	public String getNw() {
		return nw;
	}

	public void setNw(String nw) {
		this.nw = nw;
	}

	public String getSw() {
		return sw;
	}

	public void setSw(String sw) {
		this.sw = sw;
	}
	
	public void addE(String str) {
		e.add(str);
	}
	
	public void addW(String str) {
		w.add(str);
	}
	
	public void addN(String str) {
		n.add(str);
	}
	
	public void addS(String str) {
		s.add(str);
	}
	
	
 
}

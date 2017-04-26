package galileo.util;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

public class BorderingProperties {
	
	private List<String> n;
	private List<String> s;
	private List<String> e;
	private List<String> w;
	private String ne;
	private String se;
	private String nw;
	private String sw;
	
	private long up1;
	private long up2;
	private long down1;
	private long down2;
	
	private BigInteger totalRecords;
	
	private List<Integer> upTimeEntries;
	private List<Integer> downTimeEntries;
	
	private List<Integer> northEntries;
	private List<Integer> southEntries;
	private List<Integer> eastEntries;
	private List<Integer> westEntries;
	private List<Integer> neEntries;
	private List<Integer> nwEntries;
	private List<Integer> seEntries;
	private List<Integer> swEntries;
	
	public BorderingProperties() {
		
		n = new ArrayList<String>();
		s = new ArrayList<String>();
		e = new ArrayList<String>();
		w = new ArrayList<String>();
		
		totalRecords = new BigInteger("0");
		
		northEntries = new ArrayList<Integer>();
		southEntries = new ArrayList<Integer>();
		eastEntries = new ArrayList<Integer>();
		westEntries = new ArrayList<Integer>();
		neEntries = new ArrayList<Integer>();
		nwEntries = new ArrayList<Integer>();
		seEntries = new ArrayList<Integer>();
		swEntries = new ArrayList<Integer>();
		
		upTimeEntries = new ArrayList<Integer>();
		downTimeEntries = new ArrayList<Integer>();
	}
	
	public void updateRecordCount(int n) {
		BigInteger bi = BigInteger.valueOf(n);
		totalRecords.add(bi);
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

	public List<Integer> getNorthEntries() {
		return northEntries;
	}

	public void setNorthEntries(List<Integer> northEntries) {
		this.northEntries = northEntries;
	}

	public List<Integer> getSouthEntries() {
		return southEntries;
	}

	public void setSouthEntries(List<Integer> southEntries) {
		this.southEntries = southEntries;
	}

	public List<Integer> getEastEntries() {
		return eastEntries;
	}

	public void setEastEntries(List<Integer> eastEntries) {
		this.eastEntries = eastEntries;
	}

	public List<Integer> getWestEntries() {
		return westEntries;
	}

	public void setWestEntries(List<Integer> westEntries) {
		this.westEntries = westEntries;
	}

	public List<Integer> getNeEntries() {
		return neEntries;
	}

	public void setNeEntries(List<Integer> neEntries) {
		this.neEntries = neEntries;
	}

	public List<Integer> getNwEntries() {
		return nwEntries;
	}

	public void setNwEntries(List<Integer> nwEntries) {
		this.nwEntries = nwEntries;
	}

	public List<Integer> getSeEntries() {
		return seEntries;
	}

	public void setSeEntries(List<Integer> seEntries) {
		this.seEntries = seEntries;
	}

	public List<Integer> getSwEntries() {
		return swEntries;
	}

	public void setSwEntries(List<Integer> swEntries) {
		this.swEntries = swEntries;
	}
	
	public void addNorthEntries(int n) {
		northEntries.add(n);
	}
	
	public void addSouthEntries(int n) {
		southEntries.add(n);
	}
	
	public void addEastEntries(int n) {
		eastEntries.add(n);
	}
	
	public void addWestEntries(int n) {
		westEntries.add(n);
	}
	
	public void addNEEntries(int n) {
		neEntries.add(n);
	}
	
	public void addNWEntries(int n) {
		nwEntries.add(n);
	}
	
	public void addSEEntries(int n) {
		seEntries.add(n);
	}
	
	public void addSWEntries(int n) {
		swEntries.add(n);
	}

	public long getUp1() {
		return up1;
	}

	public void setUp1(long up1) {
		this.up1 = up1;
	}

	public long getUp2() {
		return up2;
	}

	public void setUp2(long up2) {
		this.up2 = up2;
	}

	public long getDown1() {
		return down1;
	}

	public void setDown1(long down1) {
		this.down1 = down1;
	}

	public long getDown2() {
		return down2;
	}

	public void setDown2(long down2) {
		this.down2 = down2;
	}

	public List<Integer> getUpTimeEntries() {
		return upTimeEntries;
	}

	public void setUpTimeEntries(List<Integer> upTimeEntries) {
		this.upTimeEntries = upTimeEntries;
	}
	

	public void addUpTimeEntries(int n) {
		upTimeEntries.add(n);
	}

	public List<Integer> getDownTimeEntries() {
		return downTimeEntries;
	}

	public void setDownTimeEntries(List<Integer> downTimeEntries) {
		this.downTimeEntries = downTimeEntries;
	}
	
	public void addDownTimeEntries(int n) {
		downTimeEntries.add(n);
	}
	
	
 
}

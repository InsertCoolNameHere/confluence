package galileo.util;

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
	
	/* Remember Up refers to upper time, so essentially the end part of this time span */
	private long up1;
	private long up2;
	private long down1;
	private long down2;
	
	private long totalRecords;
	
	private List<Long> upTimeEntries;
	private List<Long> downTimeEntries;
	
	private List<Long> northEntries;
	private List<Long> southEntries;
	private List<Long> eastEntries;
	private List<Long> westEntries;
	private List<Long> neEntries;
	private List<Long> nwEntries;
	private List<Long> seEntries;
	private List<Long> swEntries;
	
	public BorderingProperties() {
		
		n = new ArrayList<String>();
		s = new ArrayList<String>();
		e = new ArrayList<String>();
		w = new ArrayList<String>();
		
		totalRecords = 0;
		
		northEntries = new ArrayList<Long>();
		southEntries = new ArrayList<Long>();
		eastEntries = new ArrayList<Long>();
		westEntries = new ArrayList<Long>();
		neEntries = new ArrayList<Long>();
		nwEntries = new ArrayList<Long>();
		seEntries = new ArrayList<Long>();
		swEntries = new ArrayList<Long>();
		
		upTimeEntries = new ArrayList<Long>();
		downTimeEntries = new ArrayList<Long>();
	}
	
	public void updateRecordCount(long n) {
		totalRecords+=n;
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

	public List<Long> getNorthEntries() {
		return northEntries;
	}

	public void setNorthEntries(List<Long> northEntries) {
		this.northEntries = northEntries;
	}

	public List<Long> getSouthEntries() {
		return southEntries;
	}

	public void setSouthEntries(List<Long> southEntries) {
		this.southEntries = southEntries;
	}

	public List<Long> getEastEntries() {
		return eastEntries;
	}

	public void setEastEntries(List<Long> eastEntries) {
		this.eastEntries = eastEntries;
	}

	public List<Long> getWestEntries() {
		return westEntries;
	}

	public void setWestEntries(List<Long> westEntries) {
		this.westEntries = westEntries;
	}

	public List<Long> getNeEntries() {
		return neEntries;
	}

	public void setNeEntries(List<Long> neEntries) {
		this.neEntries = neEntries;
	}

	public List<Long> getNwEntries() {
		return nwEntries;
	}

	public void setNwEntries(List<Long> nwEntries) {
		this.nwEntries = nwEntries;
	}

	public List<Long> getSeEntries() {
		return seEntries;
	}

	public void setSeEntries(List<Long> seEntries) {
		this.seEntries = seEntries;
	}

	public List<Long> getSwEntries() {
		return swEntries;
	}

	public void setSwEntries(List<Long> swEntries) {
		this.swEntries = swEntries;
	}
	
	public void addNorthEntries(long n) {
		northEntries.add(n);
	}
	
	public void addSouthEntries(long n) {
		southEntries.add(n);
	}
	
	public void addEastEntries(long n) {
		eastEntries.add(n);
	}
	
	public void addWestEntries(long n) {
		westEntries.add(n);
	}
	
	public void addNEEntries(long n) {
		neEntries.add(n);
	}
	
	public void addNWEntries(long n) {
		nwEntries.add(n);
	}
	
	public void addSEEntries(long n) {
		seEntries.add(n);
	}
	
	public void addSWEntries(long n) {
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

	public List<Long> getUpTimeEntries() {
		return upTimeEntries;
	}

	public void setUpTimeEntries(List<Long> upTimeEntries) {
		this.upTimeEntries = upTimeEntries;
	}
	

	public void addUpTimeEntries(long n) {
		upTimeEntries.add(n);
	}

	public List<Long> getDownTimeEntries() {
		return downTimeEntries;
	}

	public void setDownTimeEntries(List<Long> downTimeEntries) {
		this.downTimeEntries = downTimeEntries;
	}
	
	public void addDownTimeEntries(long n) {
		downTimeEntries.add(n);
	}

	public long getTotalRecords() {
		return totalRecords;
	}

	public void setTotalRecords(long totalRecords) {
		this.totalRecords = totalRecords;
	}
	
	
 
}

package galileo.util;

import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

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
	//private List<Integer> fringeEntries;
	
	private List<Long> northEntries;
	private List<Long> southEntries;
	private List<Long> eastEntries;
	private List<Long> westEntries;
	private List<Long> neEntries;
	private List<Long> nwEntries;
	private List<Long> seEntries;
	private List<Long> swEntries;
	
	public String toString() {
		String ret = "";
		ret+=n+" "+s+" "+e+" "+w+" "+down1+" "+down2+" "+up1+" "+up2+" "+northEntries+" "
				+southEntries+" "+eastEntries+" "+westEntries+" "+neEntries+" "+nwEntries+" "+seEntries+" "+swEntries+" ";
		return ret;
	}
	
	public JSONObject getJsonStringRepresentation(String path) {
		JSONObject bmap = new JSONObject();
		
		bmap.put("blockName", path);
		bmap.put("n",n);
		bmap.put("s",s);
		bmap.put("e",e);
		bmap.put("w",w);
		bmap.put("ne",ne);
		bmap.put("nw",nw);
		bmap.put("se",se);
		bmap.put("sw",sw);
		
		bmap.put("northEntries",northEntries);
		bmap.put("southEntries",southEntries);
		bmap.put("eastEntries",eastEntries);
		bmap.put("westEntries",westEntries);
		bmap.put("neEntries",neEntries);
		bmap.put("nwEntries",nwEntries);
		bmap.put("seEntries",seEntries);
		bmap.put("swEntries",swEntries);
		
		bmap.put("up1", up1);
		bmap.put("up2", up2);
		bmap.put("down1", down1);
		bmap.put("down2", down2);
		
		bmap.put("totalRecords", totalRecords);
		
		bmap.put("upTimeEntries",upTimeEntries);
		bmap.put("downTimeEntries",downTimeEntries);
		//bmap.put("fringeEntries",fringeEntries);
		
		
		return bmap;
	}
	
	public void populateObject(JSONObject jsonObj) {
		this.up1 = jsonObj.getLong("up1");
		this.up2 = jsonObj.getLong("up2");
		this.down1 = jsonObj.getLong("down1");
		this.down2 = jsonObj.getLong("down2");
		this.totalRecords = jsonObj.getLong("totalRecords");
		
		JSONArray ns = jsonObj.getJSONArray("n");
		for (int i = 0; i < ns.length(); i++)
			n.add(ns.getString(i));
		
		JSONArray ss = jsonObj.getJSONArray("s");
		for (int i = 0; i < ss.length(); i++)
			s.add(ss.getString(i));
		
		JSONArray es = jsonObj.getJSONArray("e");
		for (int i = 0; i < es.length(); i++)
			e.add(es.getString(i));
		
		JSONArray ws = jsonObj.getJSONArray("w");
		for (int i = 0; i < ws.length(); i++)
			w.add(ws.getString(i));
		
		
		this.ne = jsonObj.getString("ne");
		this.nw = jsonObj.getString("nw");
		this.se = jsonObj.getString("se");
		this.sw = jsonObj.getString("sw");
		
		
		JSONArray nEntries = jsonObj.getJSONArray("northEntries");
		for (int i = 0; i < nEntries.length(); i++)
			northEntries.add(nEntries.getLong(i));
		
		JSONArray sEntries = jsonObj.getJSONArray("southEntries");
		for (int i = 0; i < sEntries.length(); i++)
			southEntries.add(sEntries.getLong(i));
		
		JSONArray eEntries = jsonObj.getJSONArray("eastEntries");
		for (int i = 0; i < eEntries.length(); i++)
			eastEntries.add(eEntries.getLong(i));
		
		JSONArray wEntries = jsonObj.getJSONArray("westEntries");
		for (int i = 0; i < wEntries.length(); i++)
			westEntries.add(wEntries.getLong(i));
		
		JSONArray neEntriess = jsonObj.getJSONArray("neEntries");
		for (int i = 0; i < neEntriess.length(); i++)
			neEntries.add(neEntriess.getLong(i));
		
		JSONArray nwEntriess = jsonObj.getJSONArray("nwEntries");
		for (int i = 0; i < nwEntriess.length(); i++)
			nwEntries.add(nwEntriess.getLong(i));
		
		JSONArray seEntriess = jsonObj.getJSONArray("seEntries");
		for (int i = 0; i < seEntriess.length(); i++)
			seEntries.add(seEntriess.getLong(i));
		
		JSONArray swEntriess = jsonObj.getJSONArray("swEntries");
		for (int i = 0; i < swEntriess.length(); i++)
			swEntries.add(swEntriess.getLong(i));
		
		JSONArray upTimeEntriess = jsonObj.getJSONArray("upTimeEntries");
		for (int i = 0; i < upTimeEntriess.length(); i++)
			upTimeEntries.add(upTimeEntriess.getLong(i));
		
		JSONArray downTimeEntriess = jsonObj.getJSONArray("downTimeEntries");
		for (int i = 0; i < downTimeEntriess.length(); i++)
			downTimeEntries.add(downTimeEntriess.getLong(i));
		/*
		
		JSONArray fringeEntriess = jsonObj.getJSONArray("fringeEntries");
		for (int i = 0; i < fringeEntriess.length(); i++)
			fringeEntries.add(fringeEntriess.getInt(i));
		*/
	}
	
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
		//fringeEntries = new ArrayList<Integer>();
	}
	
	public synchronized void updateRecordCount(long n) {
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

	/*public List<Integer> getFringeEntries() {
		return fringeEntries;
	}

	public void setFringeEntries(List<Integer> fringeEntries) {
		this.fringeEntries = fringeEntries;
	}
	
	public void addFringeEntries(int n) {
		this.fringeEntries.add(n);
	}*/
 
}

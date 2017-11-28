package galileo.util;

import java.util.ArrayList;
import java.util.List;

public class BlockFragments {
	
	// denotes if all that is needed is the full block
	private boolean ignore = false;
	// denotes all the spatial combinations that this block needed
	private List<String> spatial = new ArrayList<>();
	// denotes all the temporal combinations that this block needed
	
	private List<String> temporal = new ArrayList<>();
	private List<Integer> chunks = new ArrayList<Integer>();
	
	public boolean isIgnore() {
		return ignore;
	}
	public void setIgnore(boolean ignore) {
		this.ignore = ignore;
	}
	public List<Integer> getChunks() {
		return chunks;
	}
	public void setChunks(List<Integer> chunks) {
		this.chunks = chunks;
	}
	public List<String> getSpatial() {
		return spatial;
	}
	public void setSpatial(List<String> spatial) {
		this.spatial = spatial;
	}
	public List<String> getTemporal() {
		return temporal;
	}
	public void setTemporal(List<String> temporal) {
		this.temporal = temporal;
	}

}

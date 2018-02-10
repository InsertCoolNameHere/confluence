package galileo.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * The parts of blocks of a path that are necessary to be loaded
 * @author sapmitra
 *
 */
public class PathFragments {
	
	// denotes if all that is needed is the full block
	private boolean ignore = false;
	private List<String> orientations = new ArrayList<String>();
	// denotes all the spatial combinations that this block needed
	private List<String> spatial = new ArrayList<>();
	// denotes all the temporal combinations that this block needed
	
	private List<String> temporal = new ArrayList<>();
	private Set<Integer> chunks = new TreeSet<Integer>();
	
	public boolean isIgnore() {
		return ignore;
	}
	public void setIgnore(boolean ignore) {
		this.ignore = ignore;
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
	public List<String> getOrientations() {
		return orientations;
	}
	public void setOrientations(List<String> orientations) {
		this.orientations = orientations;
	}
	public void addOrientation(String orientation) {
		this.orientations.add(orientation);
	}
	public Set<Integer> getChunks() {
		return chunks;
	}
	public void setChunks(Set<Integer> chunks) {
		this.chunks = chunks;
	}
	public void addChunks(List<Integer> chunkList) {
		this.chunks.addAll(chunkList);
	}
	
	public String toString() {
		String ret = ignore + " " +  chunks;
		return ret;
	}
	

}

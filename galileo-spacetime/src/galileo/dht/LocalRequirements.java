package galileo.dht;

import java.util.ArrayList;
import java.util.List;

public class LocalRequirements {
	
	private int pathIndex;
	private boolean isReceived;
	/* TO be used later*/
	private String pathName;
	private List<Integer> fragments;
	
	public LocalRequirements(int pathIndex, String[] fragments2, boolean isReceived) {
		fragments = new ArrayList<Integer>();
		for(String str : fragments2) {
			fragments.add(Integer.valueOf(str));
			
		}
		
		this.pathIndex = pathIndex;
		this.isReceived = isReceived;
	}
	public int getPathIndex() {
		return pathIndex;
	}
	public void setPathIndex(int pathIndex) {
		this.pathIndex = pathIndex;
	}
	public boolean isReceived() {
		return isReceived;
	}
	public void setReceived(boolean isReceived) {
		this.isReceived = isReceived;
	}
	public String getPathName() {
		return pathName;
	}
	public void setPathName(String pathName) {
		this.pathName = pathName;
	}
	public List<Integer> getFragments() {
		return fragments;
	}
	public void setFragments(List<Integer> fragments) {
		this.fragments = fragments;
	}
	

}

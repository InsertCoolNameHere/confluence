package galileo.util;

import java.util.ArrayList;
import java.util.List;

import galileo.dataset.feature.Feature;
import galileo.graph.Path;

public class Requirements {
	
	private Path<Feature, String> path;
	private List<Integer> chunks = new ArrayList<Integer>();
	
	public Path<Feature, String> getPath() {
		return path;
	}
	public void setPath(Path<Feature, String> path) {
		this.path = path;
	}
	public List<Integer> getChunks() {
		return chunks;
	}
	public void setChunks(List<Integer> chunks) {
		this.chunks = chunks;
	}
	

}

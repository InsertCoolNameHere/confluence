package galileo.util;

import java.util.List;
import java.util.Map;

import galileo.dataset.feature.Feature;
import galileo.graph.Path;

public class PathsAndOrientations {
	
	private List<Path<Feature, String>> paths;
	private Map<Path<Feature, String>, PathFragments> pathToFragmentsMap;
	private Map<SuperCube,List<Requirements>> supercubeRequirementsMap;
	private int totalBlocks;
	
	public PathsAndOrientations(List<Path<Feature, String>> paths, Map<Path<Feature, String>, PathFragments> pathToFragmentsMap, Map<SuperCube,List<Requirements>> supercubeRequirementsMap, int totalBlocks) {
		
		this.paths = paths;
		this.pathToFragmentsMap = pathToFragmentsMap;
		this.supercubeRequirementsMap = supercubeRequirementsMap;
		this.totalBlocks = totalBlocks;
		
	}

	public List<Path<Feature, String>> getPaths() {
		return paths;
	}

	public void setPaths(List<Path<Feature, String>> paths) {
		this.paths = paths;
	}

	public Map<Path<Feature, String>, PathFragments> getPathToFragmentsMap() {
		return pathToFragmentsMap;
	}

	public void setPathToFragmentsMap(Map<Path<Feature, String>, PathFragments> pathToFragmentsMap) {
		this.pathToFragmentsMap = pathToFragmentsMap;
	}

	public Map<SuperCube,List<Requirements>> getSupercubeRequirementsMap() {
		return supercubeRequirementsMap;
	}

	public void setSupercubeRequirementsMap(Map<SuperCube,List<Requirements>> supercubeRequirementsMap) {
		this.supercubeRequirementsMap = supercubeRequirementsMap;
	}

	public int getTotalBlocks() {
		return totalBlocks;
	}

	public void setTotalBlocks(int totalBlocks) {
		this.totalBlocks = totalBlocks;
	}

}

package galileo.comm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import galileo.event.Event;
import galileo.serialization.SerializationException;
import galileo.serialization.SerializationInputStream;
import galileo.serialization.SerializationOutputStream;
import galileo.util.Requirements;
import galileo.util.SuperCube;

public class NeighborDataResponse implements Event {
	
	private boolean isActualData = true;
	private String nodeString;
	
	/* supercube Ids*/
	private List<Integer> supercubeIDList;
	
	/*All requirements of a single supercube is in the form of a string like:
	 * pathIndex-0,1,...,27\npathIndex-0,1,...,27\n  or something like that   */
	private List<String> requirementsList;
	private int totalPaths;
	
	
	private List<String> resultRecordLists;
	private int pathIndex;
	private String pathInfo;
	
	
	
	/* CONTROL MESSAGE */
	public NeighborDataResponse(Map<SuperCube, List<Requirements>> supercubeRequirementsMap, int totalPaths, String nodeString) {
		// TODO Auto-generated constructor stub
		
		// There is one Requirement String for each supercube
		supercubeIDList = new ArrayList<Integer>();
		requirementsList = new ArrayList<String>();
		this.nodeString = nodeString;
		for(SuperCube sc : supercubeRequirementsMap.keySet()) {
			supercubeIDList.add((int)sc.getId());
			
			String reqStr = "";
			List<Requirements> requirementsForThisCube = supercubeRequirementsMap.get(sc);
			
			if(requirementsForThisCube != null) {
				for(Requirements r: requirementsForThisCube ) {
					if(r.getChunks()!=null && r.getChunks().size() > 0) {
						String temp = r.getChunks().toString();
						reqStr+=r.getPathIndex()+"-"+temp.substring(1, temp.length() - 1)+"\n";
						
					}
				}
			}
			
			requirementsList.add(reqStr);
			
		}
		this.totalPaths = totalPaths;
		this.isActualData = false;
		
	}
	
	/**
	 * THIS IS FOR WHEN NOTHING IS BEING RETURNED FROM THE NEIGHBOR,
	 * BUT WE STILL NEED TO RETURN THE SUPERCUBES TO DECREMENT THE COUNT ON THE SENDER SIDE
	 * 
	 * @param supercubes
	 * @param totalPaths
	 * @param nodeString
	 */
	public NeighborDataResponse(List<SuperCube> supercubes, int totalPaths, String nodeString) {
		
		// There is one Requirement String for each supercube
		supercubeIDList = new ArrayList<Integer>();
		requirementsList = new ArrayList<String>();
		this.nodeString = nodeString;
		
		for(SuperCube sc : supercubes) {
			supercubeIDList.add((int)sc.getId());
		}
		
		this.totalPaths = totalPaths;
		this.isActualData = false;
		
	}
	
	

	/* DATA MESSAGE */
	public NeighborDataResponse(List<String> resultRecordLists, int pathIndex, String pathInfo, String nodeString) {
		// TODO Auto-generated constructor stub
		isActualData = true;
		this.resultRecordLists = resultRecordLists;
		this.pathIndex = pathIndex;
		this.pathInfo = pathInfo;
		this.nodeString  = nodeString;
		
	}
	

	@Deserialize
	public NeighborDataResponse(SerializationInputStream in) throws IOException, SerializationException {
		this.isActualData = in.readBoolean();
		if(!isActualData) {
			this.nodeString = in.readString();
			supercubeIDList = new ArrayList<Integer>();
			requirementsList = new ArrayList<String>();
			boolean hasList = in.readBoolean();
			if(hasList) {
				in.readIntegerCollection(supercubeIDList);
				in.readStringCollection(requirementsList);
				this.totalPaths = in.readInt();
			}
		} else {
			this.nodeString = in.readString();
			boolean hasList = in.readBoolean();
			resultRecordLists = new ArrayList<String>();
			if(hasList) {
				in.readStringCollection(resultRecordLists);
			}
			
			this.pathIndex = in.readInt();
			this.pathInfo = in.readString();
		}
			
	}


	@Override
	public void serialize(SerializationOutputStream out) throws IOException {
		out.writeBoolean(isActualData);
		if(!isActualData) {
			out.writeString(nodeString);
			if(supercubeIDList != null && supercubeIDList.size() > 0) {
				out.writeBoolean(true);
				out.writeIntegerCollection(supercubeIDList);
				out.writeStringCollection(requirementsList);
				out.writeInt(totalPaths);
			} else {
				out.writeBoolean(false);
			}
			
		} else {
			out.writeString(nodeString);
			if(resultRecordLists != null && resultRecordLists.size() > 0) {
				out.writeBoolean(true);
				out.writeStringCollection(resultRecordLists);
			} else {
				out.writeBoolean(false);
			}
			out.writeInt(pathIndex);
			out.writeString(pathInfo);
			
		}
			
	}

	public boolean isActualData() {
		return isActualData;
	}

	public void setActualData(boolean isActualData) {
		this.isActualData = isActualData;
	}

	public List<Integer> getSupercubeIDList() {
		return supercubeIDList;
	}

	public void setSupercubeIDList(List<Integer> supercubeIDList) {
		this.supercubeIDList = supercubeIDList;
	}

	public List<String> getRequirementsList() {
		return requirementsList;
	}

	public void setRequirementsList(List<String> requirementsList) {
		this.requirementsList = requirementsList;
	}

	public int getTotalPaths() {
		return totalPaths;
	}

	public void setTotalPaths(int totalPaths) {
		this.totalPaths = totalPaths;
	}

	public List<String> getResultRecordLists() {
		return resultRecordLists;
	}

	public void setResultRecordLists(List<String> resultRecordLists) {
		this.resultRecordLists = resultRecordLists;
	}

	public int getPathIndex() {
		return pathIndex;
	}

	public void setPathIndex(int pathIndex) {
		this.pathIndex = pathIndex;
	}

	public String getPathInfo() {
		return pathInfo;
	}

	public void setPathInfo(String pathInfo) {
		this.pathInfo = pathInfo;
	}

	public String getNodeString() {
		return nodeString;
	}

	public void setNodeString(String nodeString) {
		this.nodeString = nodeString;
	}
}

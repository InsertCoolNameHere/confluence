package galileo.dht;

import java.io.IOException;
import java.util.List;

import galileo.serialization.ByteSerializable;
import galileo.serialization.SerializationException;
import galileo.serialization.SerializationInputStream;
import galileo.serialization.SerializationOutputStream;

public class TrainingRequirements implements ByteSerializable{
	
	private List<String> blockPath;
	private List<String> pathInfo;
	private List<Integer> numPoints;
	
	public TrainingRequirements(List<String> blockPath, List<Integer> numPoints, List<String> pathInfo) {
		this.blockPath = blockPath;
		this.numPoints = numPoints;
		this.pathInfo = pathInfo;
	}

	
	
	@Override
	public void serialize(SerializationOutputStream out) throws IOException {
		out.writeStringCollection(blockPath);
		out.writeIntegerCollection(numPoints);
		out.writeStringCollection(pathInfo);
		
	}
	
	@Deserialize
	public TrainingRequirements(SerializationInputStream in) throws IOException, SerializationException {
		in.readStringCollection(this.blockPath);
		in.readIntegerCollection(this.numPoints);
		in.readStringCollection(this.pathInfo);
		
	}



	public List<String> getBlockPath() {
		return blockPath;
	}



	public void setBlockPath(List<String> blockPath) {
		this.blockPath = blockPath;
	}
	
	public void addBlockPath(String block) {
		this.blockPath.add(block);
	}



	public List<Integer> getNumPoints() {
		return numPoints;
	}



	public void setNumPoints(List<Integer> numPoints) {
		this.numPoints = numPoints;
	}
	
	public void addNumPoints(int numPoint) {
		this.numPoints.add(numPoint);
	}



	public List<String> getPathInfo() {
		return pathInfo;
	}

	public void addPathInfo(String pathInfo) {
		this.pathInfo.add(pathInfo);
	}


	public void setPathInfo(List<String> pathInfo) {
		this.pathInfo = pathInfo;
	}
	
	
	
	

}

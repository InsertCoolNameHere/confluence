package galileo.dht;

import java.io.IOException;
import java.util.List;

import galileo.serialization.ByteSerializable;
import galileo.serialization.SerializationException;
import galileo.serialization.SerializationInputStream;
import galileo.serialization.SerializationOutputStream;

public class TrainingRequirements implements ByteSerializable{
	
	private List<String> blockPath;
	private List<Integer> numPoints;
	
	public TrainingRequirements(List<String> blockPath, List<Integer> numPoints) {
		this.blockPath = blockPath;
		this.numPoints = numPoints;
	}

	
	
	@Override
	public void serialize(SerializationOutputStream out) throws IOException {
		out.writeStringCollection(blockPath);
		out.writeIntegerCollection(numPoints);
		
	}
	
	@Deserialize
	public TrainingRequirements(SerializationInputStream in) throws IOException, SerializationException {
		in.readStringCollection(this.blockPath);
		in.readIntegerCollection(this.numPoints);
		
		
		
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
	
	
	
	

}

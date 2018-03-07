package galileo.comm;

import java.io.IOException;
import java.util.List;

import galileo.dht.TrainingRequirements;
import galileo.event.Event;
import galileo.serialization.SerializationException;
import galileo.serialization.SerializationInputStream;
import galileo.serialization.SerializationOutputStream;

public class TrainingDataEvent implements Event{
	
	private List<String> blockPath;
	private List<Integer> numPoints;
	private String fsName;
	private String featureName;
	
	public TrainingDataEvent(TrainingRequirements tr, String fsName, String featureName) {
		
		this.blockPath = tr.getBlockPath();
		this.numPoints = tr.getNumPoints();
		this.fsName = fsName;
		this.featureName = featureName;
	}
	
	@Override
	public void serialize(SerializationOutputStream out) throws IOException {
		
		out.writeStringCollection(blockPath);
		out.writeIntegerCollection(numPoints);
		out.writeString(fsName);
		out.writeString(featureName);
		
	}
	
	@Deserialize
	public TrainingDataEvent(SerializationInputStream in) throws IOException, SerializationException {
		
		in.readStringCollection(this.blockPath);
		in.readIntegerCollection(this.numPoints);
		this.fsName = in.readString();
		this.featureName = in.readString();
		
	}

	public List<String> getBlockPath() {
		return blockPath;
	}

	public void setBlockPath(List<String> blockPath) {
		this.blockPath = blockPath;
	}

	public List<Integer> getNumPoints() {
		return numPoints;
	}

	public void setNumPoints(List<Integer> numPoints) {
		this.numPoints = numPoints;
	}

	public String getFsName() {
		return fsName;
	}

	public void setFsName(String fsName) {
		this.fsName = fsName;
	}

	public String getFeatureName() {
		return featureName;
	}

	public void setFeatureName(String featureName) {
		this.featureName = featureName;
	}

}

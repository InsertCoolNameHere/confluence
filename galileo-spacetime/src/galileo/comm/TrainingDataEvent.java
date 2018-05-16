package galileo.comm;

import java.io.IOException;
import java.util.ArrayList;
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
	private List<String> pathInfo;
	private double latEps,lonEps,timeEps;
	private boolean hasModel;
	private String model;
	
	public TrainingDataEvent(TrainingRequirements tr, String fsName, String featureName, double latEps, double lonEps, double timeEps, boolean hasModel, String model) {
		
		this.blockPath = tr.getBlockPath();
		this.numPoints = tr.getNumPoints();
		this.pathInfo = tr.getPathInfo();
		this.fsName = fsName;
		this.featureName = featureName;
		this.latEps = latEps;
		this.lonEps = lonEps;
		this.timeEps = timeEps;
		this.hasModel = hasModel;
		this.model = model;
		
	}
	
	@Override
	public void serialize(SerializationOutputStream out) throws IOException {
		
		out.writeStringCollection(blockPath);
		out.writeIntegerCollection(numPoints);
		out.writeString(fsName);
		out.writeString(featureName);
		out.writeStringCollection(pathInfo);
		out.writeDouble(latEps);
		out.writeDouble(lonEps);
		out.writeDouble(timeEps);
		out.writeBoolean(hasModel);
		if(hasModel)
			out.writeString(model);
		
	}
	
	@Deserialize
	public TrainingDataEvent(SerializationInputStream in) throws IOException, SerializationException {
		blockPath = new ArrayList<String>();
		numPoints = new ArrayList<Integer>();
		pathInfo = new ArrayList<String>();
		
		in.readStringCollection(this.blockPath);
		in.readIntegerCollection(this.numPoints);
		this.fsName = in.readString();
		this.featureName = in.readString();
		in.readStringCollection(this.pathInfo);
		latEps = in.readDouble();
		lonEps = in.readDouble();
		timeEps = in.readDouble();
		hasModel = in.readBoolean();
		if(hasModel)
			model = in.readString();
		
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

	public List<String> getPathInfo() {
		return pathInfo;
	}

	public void setPathInfo(List<String> pathInfo) {
		this.pathInfo = pathInfo;
	}

	public double getLatEps() {
		return latEps;
	}

	public void setLatEps(double latEps) {
		this.latEps = latEps;
	}

	public double getLonEps() {
		return lonEps;
	}

	public void setLonEps(double lonEps) {
		this.lonEps = lonEps;
	}

	public double getTimeEps() {
		return timeEps;
	}

	public void setTimeEps(double timeEps) {
		this.timeEps = timeEps;
	}

	public boolean isHasModel() {
		return hasModel;
	}

	public void setHasModel(boolean hasModel) {
		this.hasModel = hasModel;
	}

	public String getModel() {
		return model;
	}

	public void setModel(String model) {
		this.model = model;
	}

}

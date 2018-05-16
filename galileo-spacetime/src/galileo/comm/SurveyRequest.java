package galileo.comm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import galileo.dataset.Coordinates;
import galileo.event.Event;
import galileo.serialization.SerializationException;
import galileo.serialization.SerializationInputStream;
import galileo.serialization.SerializationOutputStream;

public class SurveyRequest implements Event {
	
	private String fsName;
	private int numTrainingPoints;
	private String featureName;
	private double latEps;
	private double lonEps;
	private double timeEps;
	private String time;
	private List<Coordinates> polygon;
	
	private boolean hasModel;
	private String model;
	

	@Deserialize
	public SurveyRequest(SerializationInputStream in) throws IOException, SerializationException {
		fsName = in.readString();
		numTrainingPoints = in.readInt();
		featureName = in.readString();
		latEps = in.readDouble();
		lonEps = in.readDouble();
		timeEps = in.readDouble();
		
		boolean isTemporal = in.readBoolean();
		if (isTemporal)
			time = in.readString();
		boolean isSpatial = in.readBoolean();
		
		if (isSpatial) {
			List<Coordinates> poly = new ArrayList<Coordinates>();
			in.readSerializableCollection(Coordinates.class, poly);
			polygon = poly;
		}
		
		hasModel = in.readBoolean();
		if(hasModel)
			model = in.readString();
	}

	@Override
	public void serialize(SerializationOutputStream out) throws IOException {
		out.writeString(fsName);
		out.writeInt(numTrainingPoints);
		out.writeString(featureName);
		out.writeDouble(latEps);
		out.writeDouble(lonEps);
		out.writeDouble(timeEps);
		
		out.writeBoolean(isTemporal());
		if (isTemporal())
			out.writeString(time);
		out.writeBoolean(isSpatial());
		if (isSpatial())
			out.writeSerializableCollection(polygon);
		
		out.writeBoolean(hasModel);
		if(hasModel)
			out.writeString(model);
		
	}

	public SurveyRequest(String fsName, int numTrainingPoints, String featureName, 
			double latEps, double lonEps, double timeEps, boolean hasModel, String model) {
		super();
		this.fsName = fsName;
		this.numTrainingPoints = numTrainingPoints;
		this.featureName = featureName;
		this.latEps = latEps;
		this.lonEps = lonEps;
		this.timeEps = timeEps;
		this.hasModel = hasModel;
		this.model = model;
	}

	
	public boolean isSpatial() {
		return polygon != null;
	}

	public boolean isTemporal() {
		return time != null;
	}
	
	public String getFsName() {
		return fsName;
	}

	public void setFsName(String fsName) {
		this.fsName = fsName;
	}

	public int getNumTrainingPoints() {
		return numTrainingPoints;
	}

	public void setNumTrainingPoints(int numTrainingPoints) {
		this.numTrainingPoints = numTrainingPoints;
	}

	public String getFeatureName() {
		return featureName;
	}

	public void setFeatureName(String featureName) {
		this.featureName = featureName;
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

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
		this.time = time;
	}

	public List<Coordinates> getPolygon() {
		return polygon;
	}

	public void setPolygon(List<Coordinates> polygon) {
		this.polygon = polygon;
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

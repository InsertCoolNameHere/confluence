package galileo.comm;

import java.io.IOException;

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

	@Deserialize
	public SurveyRequest(SerializationInputStream in) throws IOException, SerializationException {
		fsName = in.readString();
		numTrainingPoints = in.readInt();
		featureName = in.readString();
		latEps = in.readDouble();
		lonEps = in.readDouble();
		timeEps = in.readDouble();
	}

	@Override
	public void serialize(SerializationOutputStream out) throws IOException {
		out.writeString(fsName);
		out.writeInt(numTrainingPoints);
		out.writeString(featureName);
		out.writeDouble(latEps);
		out.writeDouble(lonEps);
		out.writeDouble(timeEps);
	}

	public SurveyRequest(String fsName, int numTrainingPoints, String featureName, 
			double latEps, double lonEps, double timeEps) {
		super();
		this.fsName = fsName;
		this.numTrainingPoints = numTrainingPoints;
		this.featureName = featureName;
		this.latEps = latEps;
		this.lonEps = lonEps;
		this.timeEps = timeEps;
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
}

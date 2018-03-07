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

	@Deserialize
	public SurveyRequest(SerializationInputStream in) throws IOException, SerializationException {
		fsName = in.readString();
		numTrainingPoints = in.readInt();
		featureName = in.readString();
	}

	@Override
	public void serialize(SerializationOutputStream out) throws IOException {
		out.writeString(fsName);
		out.writeInt(numTrainingPoints);
		out.writeString(featureName);
	}

	public SurveyRequest(String fsName, int numTrainingPoints, String featureName) {
		super();
		this.fsName = fsName;
		this.numTrainingPoints = numTrainingPoints;
		this.featureName = featureName;
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
}

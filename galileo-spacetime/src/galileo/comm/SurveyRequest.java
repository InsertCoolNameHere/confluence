package galileo.comm;

import java.io.IOException;

import galileo.event.Event;
import galileo.serialization.SerializationException;
import galileo.serialization.SerializationInputStream;
import galileo.serialization.SerializationOutputStream;

public class SurveyRequest implements Event {
	
	private String fsName;
	private int numTrainingPoints;

	@Deserialize
	public SurveyRequest(SerializationInputStream in) throws IOException, SerializationException {
		fsName = in.readString();
		numTrainingPoints = in.readInt();
		
	}

	@Override
	public void serialize(SerializationOutputStream out) throws IOException {
		out.writeString(fsName);
		out.writeInt(numTrainingPoints);
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
}

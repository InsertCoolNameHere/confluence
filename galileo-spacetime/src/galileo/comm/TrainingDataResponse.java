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

public class TrainingDataResponse implements Event {
	
	private String dataPoints;
	private String nodeString;
	
	public TrainingDataResponse(String dataPoints, String nodeString) {

		this.dataPoints = dataPoints;
		this.nodeString = nodeString;
	}

	

	@Deserialize
	public TrainingDataResponse(SerializationInputStream in) throws IOException, SerializationException {
		this.dataPoints = in.readString();
		this.nodeString = in.readString();
			
			
	}


	@Override
	public void serialize(SerializationOutputStream out) throws IOException {
		out.writeString(dataPoints);
		out.writeString(nodeString);
			
	}

	
	public String getNodeString() {
		return nodeString;
	}

	public void setNodeString(String nodeString) {
		this.nodeString = nodeString;
	}



	public String getDataPoints() {
		return dataPoints;
	}



	public void setDataPoints(String dataPoints) {
		this.dataPoints = dataPoints;
	}


}

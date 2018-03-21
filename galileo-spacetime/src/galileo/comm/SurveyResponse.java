package galileo.comm;

import java.io.IOException;
import java.util.List;

import galileo.event.Event;
import galileo.serialization.SerializationException;
import galileo.serialization.SerializationInputStream;
import galileo.serialization.SerializationOutputStream;
import galileo.serialization.ByteSerializable.Deserialize;

public class SurveyResponse implements Event{
	
	private String outputPath;
	private String nodeString;
	public SurveyResponse() {}

	@Override
	public void serialize(SerializationOutputStream out) throws IOException {
		out.writeString(outputPath);
		out.writeString(nodeString);
		
	}
	@Deserialize
	public SurveyResponse(SerializationInputStream in) throws IOException, SerializationException {
		outputPath = in.readString();
		nodeString = in.readString();
	}

	public String getOutputPath() {
		return outputPath;
	}

	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}

	public String getNodeString() {
		return nodeString;
	}

	public void setNodeString(String nodeString) {
		this.nodeString = nodeString;
	}

}

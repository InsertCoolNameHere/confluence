package galileo.comm;

import java.io.IOException;

import galileo.event.Event;
import galileo.serialization.SerializationException;
import galileo.serialization.SerializationInputStream;
import galileo.serialization.SerializationOutputStream;

public class SurveyEvent implements Event {
	
	private String fsName;

	@Deserialize
	public SurveyEvent(SerializationInputStream in) throws IOException, SerializationException {
		fsName = in.readString();
		
	}

	public SurveyEvent(String fsName) {

		this.fsName = fsName;
		
	}

	@Override
	public void serialize(SerializationOutputStream out) throws IOException {
		out.writeString(fsName);
		
	}

	public String getFsName() {
		return fsName;
	}

	public void setFsName(String fsName) {
		this.fsName = fsName;
	}
}

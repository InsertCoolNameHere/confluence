package galileo.comm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import galileo.event.Event;
import galileo.serialization.SerializationException;
import galileo.serialization.SerializationInputStream;
import galileo.serialization.SerializationOutputStream;

public class DataIntegrationFinalResponse implements Event{

	String eventId;
	List<String> resultPaths; 
	
	public DataIntegrationFinalResponse(String eventId) {
		// TODO Auto-generated constructor stub
		this.eventId = eventId;
	}
	
	
	
	@Override
	public void serialize(SerializationOutputStream out) throws IOException {
		out.writeString(eventId);
		boolean hasResults = false;
		if(resultPaths !=null && resultPaths.size() > 0)
			hasResults = true;
		
		out.writeBoolean(hasResults);
		if(hasResults)
			out.writeStringCollection(resultPaths);
	}
	@Deserialize
	public DataIntegrationFinalResponse(SerializationInputStream in) throws IOException, SerializationException {
		
		eventId = in.readString();
		
		boolean hasResults = in.readBoolean();
		
		if(hasResults)
			in.readStringCollection(resultPaths);
		
	}
	
	
	public String getEventId() {
		return eventId;
	}
	public void setEventId(String eventId) {
		this.eventId = eventId;
	}
	public List<String> getResultPaths() {
		return resultPaths;
	}
	public void setResultPaths(List<String> resultPaths) {
		this.resultPaths = resultPaths;
	}
	
	public void addResultPath(String path) {
		
		if(resultPaths == null) 
			resultPaths = new ArrayList<String>();
		resultPaths.add(path);
	}

}

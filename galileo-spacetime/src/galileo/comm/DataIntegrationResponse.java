package galileo.comm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import galileo.event.Event;
import galileo.serialization.SerializationException;
import galileo.serialization.SerializationInputStream;
import galileo.serialization.SerializationOutputStream;

public class DataIntegrationResponse implements Event{

	private String eventId;
	private List<String> resultPaths;
	private String nodeName;
	private String nodePort;
	
	public DataIntegrationResponse(String eventId) {
		// TODO Auto-generated constructor stub
		this.eventId = eventId;
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
	
	@Deserialize
	public DataIntegrationResponse(SerializationInputStream in) throws IOException, SerializationException {
		eventId = in.readString();
		resultPaths = new ArrayList<String>();
		boolean hasPaths = in.readBoolean();
		if(hasPaths)
			in.readStringCollection(resultPaths);
		
		nodeName = in.readString();
		nodePort = in.readString();
		
	}

	@Override
	public void serialize(SerializationOutputStream out) throws IOException {
		out.writeString(eventId);
		boolean hasPaths = false;
		if(resultPaths != null && resultPaths.size() > 0)
			hasPaths = true;
		out.writeBoolean(hasPaths);
		if(hasPaths)
			out.writeStringCollection(resultPaths);
		out.writeString(nodeName);
		out.writeString(nodePort);
	}
	public String getNodeName() {
		return nodeName;
	}
	public void setNodeName(String nodeName) {
		this.nodeName = nodeName;
	}
	public String getNodePort() {
		return nodePort;
	}
	public void setNodePort(String nodePort) {
		this.nodePort = nodePort;
	}

}

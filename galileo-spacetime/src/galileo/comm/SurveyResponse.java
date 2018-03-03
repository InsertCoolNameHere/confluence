package galileo.comm;

import java.io.IOException;
import java.util.List;

import galileo.event.Event;
import galileo.serialization.SerializationOutputStream;

public class SurveyResponse implements Event{
	
	// metadata info for each path
	private List<String> pathInfos;
	// nodename:port$$pathstring
	private List<String> pathStrings;
	private List<Integer> counts;

	@Override
	public void serialize(SerializationOutputStream out) throws IOException {
		// TODO Auto-generated method stub
		
	}

	public List<String> getPathInfos() {
		return pathInfos;
	}

	public void setPathInfos(List<String> pathInfos) {
		this.pathInfos = pathInfos;
	}

	public List<String> getPathStrings() {
		return pathStrings;
	}

	public void setPathStrings(List<String> pathStrings) {
		this.pathStrings = pathStrings;
	}

	public List<Integer> getCounts() {
		return counts;
	}

	public void setCounts(List<Integer> counts) {
		this.counts = counts;
	}

}

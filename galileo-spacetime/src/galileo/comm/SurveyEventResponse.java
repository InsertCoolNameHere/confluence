package galileo.comm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import galileo.event.Event;
import galileo.serialization.SerializationException;
import galileo.serialization.SerializationInputStream;
import galileo.serialization.SerializationOutputStream;
import galileo.serialization.ByteSerializable.Deserialize;

public class SurveyEventResponse implements Event{
	
	// nodename:port$$time$space
	private List<String> pathInfos;
	private List<String> blocks;
	private List<Long> recordCounts;
	private String errorMsg;
	//private static final Logger logger = Logger.getLogger("galileo");

	public SurveyEventResponse(List<String> pathInfos, List<String> blocks, List<Long> recordCounts) {
		this.pathInfos = pathInfos;
		this.blocks = blocks;
		this.recordCounts = recordCounts;
	}
	
	public SurveyEventResponse(String errorMsg) {
		this.errorMsg = errorMsg;
	}

	@Deserialize
	public SurveyEventResponse(SerializationInputStream in) throws IOException, SerializationException {
		
		boolean hasData = in.readBoolean();
		if(hasData) {
			pathInfos = new ArrayList<String>();
			recordCounts = new ArrayList<Long>();
			blocks = new ArrayList<String>();
			
			in.readStringCollection(pathInfos);
			in.readStringCollection(blocks);
			in.readLongCollection(recordCounts);
		}
		
		boolean hasError = in.readBoolean();
		if(hasError) {
			this.errorMsg = in.readString();
		}
	}


	@Override
	public void serialize(SerializationOutputStream out) throws IOException {
		
		boolean hasData = false;
		
		if(pathInfos != null && pathInfos.size() > 0 && blocks != null && blocks.size() > 0
				&& recordCounts != null && recordCounts.size() > 0) {
			hasData = true;
		}
		out.writeBoolean(hasData);
		
		if(hasData) {
			
			out.writeStringCollection(pathInfos);
			out.writeStringCollection(blocks);
			out.writeLongCollection(recordCounts);
			
		}
		
		boolean hasError = false;
		
		if(errorMsg != null && !errorMsg.isEmpty()) {
			hasError = true;
		}
		
		out.writeBoolean(hasError);
		if(hasError) {
			out.writeString(errorMsg);
		}
			
	}

	public List<String> getPathInfos() {
		return pathInfos;
	}

	public void setPathInfos(List<String> pathInfos) {
		this.pathInfos = pathInfos;
	}

	public List<String> getBlocks() {
		return blocks;
	}

	public void setBlocks(List<String> pathStrings) {
		this.blocks = pathStrings;
	}

	public List<Long> getRecordCounts() {
		return recordCounts;
	}

	public void setRecordCounts(List<Long> counts) {
		this.recordCounts = counts;
	}

}

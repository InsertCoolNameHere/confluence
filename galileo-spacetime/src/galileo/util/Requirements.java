package galileo.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import galileo.dataset.feature.Feature;
import galileo.graph.Path;
import galileo.serialization.ByteSerializable;
import galileo.serialization.SerializationException;
import galileo.serialization.SerializationInputStream;
import galileo.serialization.SerializationOutputStream;
import galileo.serialization.ByteSerializable.Deserialize;

public class Requirements implements ByteSerializable{
	
	private int pathIndex;
	private List<Integer> chunks = new ArrayList<Integer>();
	
	public Requirements(int pathIndex,  List<Integer> chunks) {
		this.chunks = chunks;
		this.pathIndex = pathIndex;
	}

	public List<Integer> getChunks() {
		return chunks;
	}
	public void setChunks(List<Integer> chunks) {
		this.chunks = chunks;
	}

	public int getPathIndex() {
		return pathIndex;
	}

	public void setPathIndex(int pathIndex) {
		this.pathIndex = pathIndex;
	}

	@Override
	public void serialize(SerializationOutputStream out) throws IOException {
		out.writeInt(pathIndex);
		out.writeIntegerCollection(chunks);
		
	}
	
	@Deserialize
	public Requirements(SerializationInputStream in) throws IOException, SerializationException {
		this.pathIndex = in.readInt();
		in.readIntegerCollection(chunks);
	}
	

}

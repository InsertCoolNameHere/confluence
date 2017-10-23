package galileo.comm;

import java.io.IOException;
import java.util.List;

import galileo.event.Event;
import galileo.serialization.SerializationOutputStream;
import galileo.util.SuperCube;

public class NeighborDataEvent implements Event{
	
	private List<SuperCube> supercubes;
	private String reqFs;
	
	
	@Override
	public void serialize(SerializationOutputStream out) throws IOException {
		out.writeString(reqFs);
		out.writeSerializableCollection(supercubes);
		
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	public List<SuperCube> getSupercubes() {
		return supercubes;
	}

	public void setSupercubes(List<SuperCube> supercubes) {
		this.supercubes = supercubes;
	}

	public String getReqFs() {
		return reqFs;
	}

	public void setReqFs(String reqFs) {
		this.reqFs = reqFs;
	}

}

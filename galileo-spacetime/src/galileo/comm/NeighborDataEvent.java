package galileo.comm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import galileo.dataset.Coordinates;
import galileo.event.Event;
import galileo.query.Query;
import galileo.serialization.SerializationException;
import galileo.serialization.SerializationInputStream;
import galileo.serialization.SerializationOutputStream;
import galileo.serialization.ByteSerializable.Deserialize;
import galileo.util.SuperCube;

public class NeighborDataEvent implements Event{
	
	private List<SuperCube> supercubes;
	private String reqFs;
	private String srcFs;
	private List<Coordinates> superPolygon;
	private String queryTime;
	
	public NeighborDataEvent(List<SuperCube> supercubes, String reqFs, String srcFs, List<Coordinates> superPolygon, String queryTime) {
		
		this.supercubes = supercubes;
		this.reqFs = reqFs;
		this.srcFs = srcFs;
		this.superPolygon = superPolygon;
		this.queryTime = queryTime;
	}
	
	
	@Override
	public void serialize(SerializationOutputStream out) throws IOException {
		if(queryTime != null && queryTime.length() > 0) {
			out.writeBoolean(true);
			out.writeString(queryTime);
		} else {
			out.writeBoolean(false);
		}
		
		out.writeString(reqFs);
		out.writeString(srcFs);
		out.writeSerializableCollection(supercubes);
		out.writeSerializableCollection(superPolygon);
		
	}
	
	@Deserialize
	public NeighborDataEvent(SerializationInputStream in) throws IOException, SerializationException {
		boolean av = in.readBoolean();
		
		if(av) {
			queryTime = in.readString();
		} else {
			queryTime = "";
		}
		
		reqFs = in.readString();
		srcFs = in.readString();
		
		List<SuperCube> cubes = new ArrayList<SuperCube>();
		in.readSerializableCollection(SuperCube.class, cubes);
		supercubes = cubes;
		
		List<Coordinates> poly = new ArrayList<Coordinates>();
		in.readSerializableCollection(Coordinates.class, poly);
		superPolygon = poly;
		
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

	public List<Coordinates> getSuperPolygon() {
		return superPolygon;
	}

	public void setSuperPolygon(List<Coordinates> superPolygon) {
		this.superPolygon = superPolygon;
	}


	public String getQueryTime() {
		return queryTime;
	}


	public void setQueryTime(String queryTime) {
		this.queryTime = queryTime;
	}


	public String getSrcFs() {
		return srcFs;
	}


	public void setSrcFs(String srcFs) {
		this.srcFs = srcFs;
	}

}

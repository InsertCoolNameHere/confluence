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

public class DataIntegrationRequest implements Event{
	
	/* time is a - separated string till hour. to specify magnification, leave the fields xx */
	private String time;
	private List<Coordinates> polygon;
	private int timeRelaxation;
	private int spaceRelaxation;
	
	/* FS1 is the primary file system */
	private String fsname1;
	private String fsname2;
	private String interpolatingFeature;
	private int primaryFS;
	private Query featureQuery;
	private double latRelax;
	private double longRelax;
	private boolean fixedBeta;
	private String model;
	
	public boolean isSpatial() {
		return polygon != null;
	}

	public boolean isTemporal() {
		return time != null;
	}
	
	public boolean hasFeatureQuery() {
		return this.featureQuery != null;
	}
	
	public String getFeatureQueryString() {
		if (this.featureQuery != null)
			return featureQuery.toString();
		return "";
	}
	public DataIntegrationRequest() {
	}

	@Deserialize
	public DataIntegrationRequest(SerializationInputStream in) throws IOException, SerializationException {
		//fsName = in.readString();
		boolean isTemporal = in.readBoolean();
		if (isTemporal)
			time = in.readString();
		boolean isSpatial = in.readBoolean();
		
		if (isSpatial) {
			List<Coordinates> poly = new ArrayList<Coordinates>();
			in.readSerializableCollection(Coordinates.class, poly);
			polygon = poly;
		}
		
		boolean hasFeatureQuery = in.readBoolean();
		if (hasFeatureQuery)
			this.featureQuery = new Query(in);
		
		timeRelaxation = in.readInt();
		spaceRelaxation = in.readInt();
		fsname1 = in.readString();
		fsname2 = in.readString();
		interpolatingFeature = in.readString();
		primaryFS = in.readInt();
		latRelax = in.readDouble();
		longRelax = in.readDouble();
		
		fixedBeta = in.readBoolean();
		if(!fixedBeta)
			model = in.readString();
		
		
		
	}

	@Override
	public void serialize(SerializationOutputStream out) throws IOException {
		
		out.writeBoolean(isTemporal());
		if (isTemporal())
			out.writeString(time);
		out.writeBoolean(isSpatial());
		if (isSpatial())
			out.writeSerializableCollection(polygon);
		
		out.writeBoolean(hasFeatureQuery());
		if (hasFeatureQuery())
			out.writeSerializable(this.featureQuery);
		
		out.writeInt(timeRelaxation);
		out.writeInt(spaceRelaxation);
		out.writeString(fsname1);
		out.writeString(fsname2);
		out.writeString(interpolatingFeature);
		out.writeInt(primaryFS);
		out.writeDouble(latRelax);
		out.writeDouble(longRelax);
		
		out.writeBoolean(fixedBeta);
		if(!fixedBeta) 
			out.writeString(model);
		
		
	}

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
		this.time = time;
	}

	public List<Coordinates> getPolygon() {
		return polygon;
	}

	public void setPolygon(List<Coordinates> polygon) {
		this.polygon = polygon;
	}

	public int getTimeRelaxation() {
		return timeRelaxation;
	}

	public void setTimeRelaxation(int timeRelaxation) {
		this.timeRelaxation = timeRelaxation;
	}

	public int getSpaceRelaxation() {
		return spaceRelaxation;
	}

	public void setSpaceRelaxation(int spaceRelaxation) {
		this.spaceRelaxation = spaceRelaxation;
	}

	public String getFsname1() {
		return fsname1;
	}

	public void setFsname1(String fsname1) {
		this.fsname1 = fsname1;
	}

	public String getFsname2() {
		return fsname2;
	}

	public void setFsname2(String fsname2) {
		this.fsname2 = fsname2;
	}

	public int getPrimaryFS() {
		return primaryFS;
	}

	public void setPrimaryFS(int primaryFS) {
		this.primaryFS = primaryFS;
	}

	public Query getFeatureQuery() {
		return featureQuery;
	}

	public void setFeatureQuery(Query featureQuery) {
		this.featureQuery = featureQuery;
	}

	public double getLatRelax() {
		return latRelax;
	}

	public void setLatRelax(double latRelax) {
		this.latRelax = latRelax;
	}

	public double getLongRelax() {
		return longRelax;
	}

	public void setLongRelax(double longRelax) {
		this.longRelax = longRelax;
	}

	public String getInterpolatingFeature() {
		return interpolatingFeature;
	}

	public void setInterpolatingFeature(String interpolatingFeature) {
		this.interpolatingFeature = interpolatingFeature;
	}

	public boolean isFixedBeta() {
		return fixedBeta;
	}

	public void setFixedBeta(boolean fixedBeta) {
		this.fixedBeta = fixedBeta;
	}

	public String getModel() {
		return model;
	}

	public void setModel(String model) {
		this.model = model;
	}

}

package galileo.bq;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * This class is for test use only;
 * 
 * String row length in byte = 427
 * EDFDataRecord length in byte = 950
 *
 */
public class EDFDataRecord implements Serializable {

	private static final long serialVersionUID = -8100149313034860022L;
	
	// Warning! these variables must match fields of the table
	private String platform_id, date, time, datetime;
	private double frac_days_since_jan1, frac_hrs_since_jan1, julian_days, epoch_time;
	private double alarm_status, inst_status;
	private double cavity_pressure, cavity_temp;
	private double das_temp, etalon_temp, warm_box_temp;
	private double species, mvp_position, outlet_valve, solenoid_valves;
	private double h2o, co2, co2_dry;
	private double ch4, ch4_dry;
	private double gps_abs_lat, gps_abs_long;
	private double gps_fit, gps_time;
	private double ws_wind_lon, ws_wind_lat, ws_cos_heading, ws_sin_heading, wind_n, wind_e, wind_dir_sdev, ws_rotation;
	private double car_speed;
	private String postal_code, locality;
	private long s2_30_int;
	
	public EDFDataRecord(String commaSeparatedRow){
		try{
			parseAndStore(commaSeparatedRow);
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	private void parseAndStore(String data) throws Exception{
		String[] values = data.split(",");
		platform_id = values[0];
        date = values[1];
        time = values[2];
        datetime = values[3];
        frac_days_since_jan1 = Double.parseDouble(values[4]);
        frac_hrs_since_jan1 = Double.parseDouble(values[5]);
        julian_days = Double.parseDouble(values[6]);
        epoch_time = Double.parseDouble(values[7]);
        alarm_status = Double.parseDouble(values[8]);
        inst_status = Double.parseDouble(values[9]);
        cavity_pressure = Double.parseDouble(values[10]);
        cavity_temp = Double.parseDouble(values[11]);
        das_temp = Double.parseDouble(values[12]);
        etalon_temp = Double.parseDouble(values[13]);
        warm_box_temp = Double.parseDouble(values[14]);
        species = Double.parseDouble(values[15]);
        mvp_position = Double.parseDouble(values[16]);
        outlet_valve = Double.parseDouble(values[17]);
        solenoid_valves = Double.parseDouble(values[18]);
        co2 = Double.parseDouble(values[19]);
        co2_dry = Double.parseDouble(values[20]);
        ch4 = Double.parseDouble(values[21]);
        ch4_dry = Double.parseDouble(values[22]);
        h2o = Double.parseDouble(values[23]);
        gps_abs_lat = Double.parseDouble(values[24]);
        gps_abs_long = Double.parseDouble(values[25]);
        gps_fit = Double.parseDouble(values[26]);
        gps_time = Double.parseDouble(values[27]);
        ws_wind_lon = Double.parseDouble(values[28]);
        ws_wind_lat = Double.parseDouble(values[29]);
        ws_cos_heading = Double.parseDouble(values[30]);
        ws_sin_heading = Double.parseDouble(values[31]);
        wind_n = Double.parseDouble(values[32]);
        wind_e = Double.parseDouble(values[33]);
        wind_dir_sdev = Double.parseDouble(values[34]);
        ws_rotation = Double.parseDouble(values[35]);
        car_speed = Double.parseDouble(values[36]);
        postal_code = values[37];
        locality = values[38];
        s2_30_int = Long.parseLong(values[39]);
	}
	
	
	
	
	private void writeObject(ObjectOutputStream stream) throws IOException {
        stream.writeObject(platform_id);
        stream.writeObject(date);
        stream.writeObject(time);
        stream.writeObject(datetime);
        stream.writeDouble(frac_days_since_jan1);
        stream.writeDouble(frac_hrs_since_jan1);
        stream.writeDouble(julian_days);
        stream.writeDouble(epoch_time);
        stream.writeDouble(alarm_status);
        stream.writeDouble(inst_status);
        stream.writeDouble(cavity_pressure);
        stream.writeDouble(cavity_temp);
        stream.writeDouble(das_temp);
        stream.writeDouble(etalon_temp);
        stream.writeDouble(warm_box_temp);
        stream.writeDouble(species);
        stream.writeDouble(mvp_position);
        stream.writeDouble(outlet_valve);
        stream.writeDouble(solenoid_valves);
        stream.writeDouble(co2);
        stream.writeDouble(co2_dry);
        stream.writeDouble(ch4);
        stream.writeDouble(ch4_dry);
        stream.writeDouble(h2o);
        stream.writeDouble(gps_abs_lat);
        stream.writeDouble(gps_abs_long);
        stream.writeDouble(gps_fit);
        stream.writeDouble(gps_time);
        stream.writeDouble(ws_wind_lon);
        stream.writeDouble(ws_wind_lat);
        stream.writeDouble(ws_cos_heading);
        stream.writeDouble(ws_sin_heading);
        stream.writeDouble(wind_n);
        stream.writeDouble(wind_e);
        stream.writeDouble(wind_dir_sdev);
        stream.writeDouble(ws_rotation);
        stream.writeDouble(car_speed);
        stream.writeObject(postal_code);
        stream.writeObject(locality);
        stream.writeLong(s2_30_int);
    }

    private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
        platform_id = (String) stream.readObject();
        date = (String) stream.readObject();
        time = (String) stream.readObject();
        datetime = (String) stream.readObject();
        frac_days_since_jan1 = stream.readDouble();
        frac_hrs_since_jan1 = stream.readDouble();
        julian_days = stream.readDouble();
        epoch_time = stream.readDouble();
        alarm_status = stream.readDouble();
        inst_status = stream.readDouble();
        cavity_pressure = stream.readDouble();
        cavity_temp = stream.readDouble();
        das_temp = stream.readDouble();
        etalon_temp = stream.readDouble();
        warm_box_temp = stream.readDouble();
        species = stream.readDouble();
        mvp_position = stream.readDouble();
        outlet_valve = stream.readDouble();
        solenoid_valves = stream.readDouble();
        co2 = stream.readDouble();
        co2_dry = stream.readDouble();
        ch4 = stream.readDouble();
        ch4_dry = stream.readDouble();
        h2o = stream.readDouble();
        gps_abs_lat = stream.readDouble();
        gps_abs_long = stream.readDouble();
        gps_fit = stream.readDouble();
        gps_time = stream.readDouble();
        ws_wind_lon = stream.readDouble();
        ws_wind_lat = stream.readDouble();
        ws_cos_heading = stream.readDouble();
        ws_sin_heading = stream.readDouble();
        wind_n = stream.readDouble();
        wind_e = stream.readDouble();
        wind_dir_sdev = stream.readDouble();
        ws_rotation = stream.readDouble();
        car_speed = stream.readDouble();
        postal_code = (String) stream.readObject();
        locality = (String) stream.readObject();
        s2_30_int = stream.readLong();
    }
    
    public static byte[] byteSerialize(Object obj) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream os = new ObjectOutputStream(out);
        os.writeObject(obj);
        return out.toByteArray();
    }
    public static EDFDataRecord byteDeserialize(byte[] data) throws IOException, ClassNotFoundException {
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        ObjectInputStream is = new ObjectInputStream(in);
        return (EDFDataRecord) is.readObject();
    }

}

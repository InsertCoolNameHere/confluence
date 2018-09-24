package galileo.bq;
/*
 * Copyright (c) 2013, Colorado State University. Modified by Duck Keun Yang for CSU EDF Project 2015-08-02
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice, this
 *    list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *    
 * This software is provided by the copyright holders and contributors "as is" and
 * any express or implied warranties, including, but not limited to, the implied
 * warranties of merchantability and fitness for a particular purpose are
 * disclaimed. In no event shall the copyright holder or contributors be liable for
 * any direct, indirect, incidental, special, exemplary, or consequential damages
 * (including, but not limited to, procurement of substitute goods or services;
 * loss of use, data, or profits; or business interruption) however caused and on
 * any theory of liability, whether in contract, strict liability, or tort
 * (including negligence or otherwise) arising in any way out of the use of this
 * software, even if advised of the possibility of such damage.
 */

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import galileo.dataset.Block;
import galileo.dataset.Metadata;
import galileo.dataset.SpatialProperties;
import galileo.dataset.SpatialRange;
import galileo.dataset.TemporalProperties;
import galileo.dataset.feature.Feature;
import galileo.dataset.feature.FeatureSet;
import galileo.dataset.feature.FeatureType;
import galileo.util.GeoHash;
import galileo.util.Pair;

public class GalileoConnector extends GalileoConnectorInterface {
	

	// [START Constructor]
	/**
	 * Constructor of GalileoConnector. Use Superclass Constructor.
	 * 
	 * @param serverHostName	Hostname of galileo server
	 * @param serverPort		Portnumber of galileo server
	 * @throws IOException
	 */
	public GalileoConnector(String serverHostName, int serverPort) throws IOException {
		super(serverHostName, serverPort);
	}
	// [END Constructor]

	// [START store]
	/**
	 * stores a block to galileo server. Use Superclass Method
	 * 
	 * @param block		a galileo block to be sent and stored at galileo server
	 * @throws Exception
	 */
	public void store(Block fb) throws Exception {
		super.store(fb);
	}
	// [END store]

	// [START disconnect]
	/**
	 * disconnects from galileo server
	 */
	public void disconnect() {
		super.disconnect();
	}
	
	public String toString() {
		return server.toString();
	}
	// [END disconnect]

	// [START createBlock]
	/**
	 * returns a galileo block from csv formatted EDF data record.
	 * 
	 * @param EDFDataRecord		a single row of csv-formatted EDF data from BigQuery
	 * @throws UnsupportedEncodingException 
	 */
	public static Block createBlock(String edfRecord, String data) throws UnsupportedEncodingException {
		String[] values = edfRecord.split(",");
		TemporalProperties temporalProperties = new TemporalProperties(reformatDatetime(values[7]));
		SpatialProperties spatialProperties = new SpatialProperties(parseFloat(values[24]), parseFloat(values[25]));
		FeatureSet features = new FeatureSet();
		features.put(new Feature("platform_id", values[0]));		
		features.put(new Feature("date", values[1]));
		features.put(new Feature("time", values[2]));
		features.put(new Feature("datetime", values[3]));
		features.put(new Feature("frac_days_since_jan1", parseFloat(values[4])));
		features.put(new Feature("frac_hrs_since_jan1", parseFloat(values[5])));
		features.put(new Feature("julian_days", parseFloat(values[6])));
		features.put(new Feature("non_epoch_time", reformatDatetime(values[7])));
		features.put(new Feature("alarm_status", parseFloat(values[8])));
		features.put(new Feature("inst_status", parseFloat(values[9])));
		features.put(new Feature("cavity_pressure", parseFloat(values[10])));
		features.put(new Feature("cavity_temp", parseFloat(values[11])));
		features.put(new Feature("das_temp", parseFloat(values[12])));
		features.put(new Feature("etalon_temp", parseFloat(values[13])));
		features.put(new Feature("warm_box_temp", parseFloat(values[14])));
		features.put(new Feature("species", parseFloat(values[15])));
		features.put(new Feature("mvp_position", parseFloat(values[16])));
		features.put(new Feature("outlet_valve", parseFloat(values[17])));
		features.put(new Feature("solenoid_valves", parseFloat(values[18])));
		features.put(new Feature("co2", parseFloat(values[19])));
		features.put(new Feature("co2_dry", parseFloat(values[20])));
		features.put(new Feature("ch4", parseFloat(values[21])));
		features.put(new Feature("ch4_dry", parseFloat(values[22])));
		features.put(new Feature("h2o", parseFloat(values[23])));
		features.put(new Feature("gps_abs_lat", parseFloat(values[24])));
		features.put(new Feature("gps_abs_lon", parseFloat(values[25])));
		features.put(new Feature("gps_fit", parseFloat(values[26])));
		features.put(new Feature("gps_time", parseFloat(values[27])));
		features.put(new Feature("ws_wind_lon", parseFloat(values[28])));
		features.put(new Feature("ws_wind_lat", parseFloat(values[29])));
		features.put(new Feature("ws_cos_heading", parseFloat(values[30])));
		features.put(new Feature("ws_sin_heading", parseFloat(values[31])));
		features.put(new Feature("wind_n", parseFloat(values[32])));
		features.put(new Feature("wind_e", parseFloat(values[33])));
		features.put(new Feature("wind_dir_sdev", parseFloat(values[34])));
		features.put(new Feature("ws_rotation", parseFloat(values[35])));
		features.put(new Feature("car_speed", parseFloat(values[36])));
		features.put(new Feature("postal_code", values[37]));
		features.put(new Feature("locality", values[38]));
		features.put(new Feature("s2_30_int", parseLong(values[39])));
		features.put(new Feature("wind_speed", parseFloat(values[40])));
		features.put(new Feature("wind_dir", values[39]));
		features.put(new Feature("epoch_time", reformatDatetime(values[7])));
		
			
		Metadata metadata = new Metadata();
		metadata.setName(GeoHash.encode(parseFloat(values[24]), parseFloat(values[25]), 7));
		metadata.setTemporalProperties(temporalProperties);
		//metadata.setsIndex(new SearchIndex("24","25","7"));
		metadata.setSpatialProperties(spatialProperties);
		//metadata.setSpatialHint(new SpatialHint("gps_abs_lat", "gps_abs_lon"));
		metadata.setAttributes(features);
		
		return new Block("sensorfs", metadata, data.getBytes("UTF-8"));
	}
	
	
	
	public static Block createBlock1(String edfRecord, String data) throws UnsupportedEncodingException {
		String[] values = edfRecord.split(",");
		TemporalProperties temporalProperties = new TemporalProperties(reformatDatetime(values[0]));
		SpatialProperties spatialProperties = new SpatialProperties(parseFloat(values[1]), parseFloat(values[2]));
		FeatureSet features = new FeatureSet();
		features.put(new Feature("epoch_time", values[0]));		
		features.put(new Feature("gps_abs_lat", values[1]));
		features.put(new Feature("gps_abs_long", values[2]));
		features.put(new Feature("fsa_feature", values[3]));
		
			
		Metadata metadata = new Metadata();
		metadata.setName(GeoHash.encode(parseFloat(values[1]), parseFloat(values[2]), 7));
		metadata.setTemporalProperties(temporalProperties);
		//metadata.setsIndex(new SearchIndex("24","25","7"));
		metadata.setSpatialProperties(spatialProperties);
		//metadata.setSpatialHint(new SpatialHint("gps_abs_lat", "gps_abs_lon"));
		metadata.setAttributes(features);
		
		return new Block("testfs1", metadata, data.getBytes("UTF-8"));
	}
	
	public static Block createBlock2(String edfRecord, String data) throws UnsupportedEncodingException {
		String[] values = edfRecord.split(",");
		TemporalProperties temporalProperties = new TemporalProperties(reformatDatetime(values[0]));
		SpatialProperties spatialProperties = new SpatialProperties(parseFloat(values[1]), parseFloat(values[2]));
		FeatureSet features = new FeatureSet();
		features.put(new Feature("epoch_time", values[0]));		
		features.put(new Feature("gps_abs_lat", values[1]));
		features.put(new Feature("gps_abs_long", values[2]));
		features.put(new Feature("fsb_feature", values[3]));
		
			
		Metadata metadata = new Metadata();
		metadata.setName(GeoHash.encode(parseFloat(values[1]), parseFloat(values[2]), 7));
		metadata.setTemporalProperties(temporalProperties);
		//metadata.setsIndex(new SearchIndex("24","25","7"));
		metadata.setSpatialProperties(spatialProperties);
		//metadata.setSpatialHint(new SpatialHint("gps_abs_lat", "gps_abs_lon"));
		metadata.setAttributes(features);
		
		return new Block("testfs2", metadata, data.getBytes("UTF-8"));
	}
	
	
	public static Block createBlock(String edfRecord, String data, String fsName, int mode) throws UnsupportedEncodingException {
		String[] values = edfRecord.split(",");
		TemporalProperties temporalProperties = new TemporalProperties(reformatDatetime(values[7]));
		SpatialProperties spatialProperties = new SpatialProperties(parseFloat(values[24]), parseFloat(values[25]));
		
		FeatureSet features = getFeatures(values, mode);
		
		Metadata metadata = new Metadata();
		metadata.setName(GeoHash.encode(parseFloat(values[24]), parseFloat(values[25]), 7));
		metadata.setTemporalProperties(temporalProperties);
		//metadata.setsIndex(new SearchIndex("24","25","7"));
		metadata.setSpatialProperties(spatialProperties);
		//metadata.setSpatialHint(new SpatialHint("gps_abs_lat", "gps_abs_lon"));
		metadata.setAttributes(features);
		
		return new Block(fsName, metadata, data.getBytes("UTF-8"));
	}

	public static Block createBlockWind(String edfRecord, String data, String fsName) throws UnsupportedEncodingException {
		String[] values = edfRecord.split(",");
		TemporalProperties temporalProperties = new TemporalProperties(reformatDatetime(values[2]));
		SpatialProperties spatialProperties = new SpatialProperties(parseFloat(values[0]), parseFloat(values[1]));
		
		FeatureSet features = getFeaturesWind(values);
		
		Metadata metadata = new Metadata();
		metadata.setName(GeoHash.encode(parseFloat(values[0]), parseFloat(values[1]), 4));
		metadata.setTemporalProperties(temporalProperties);
		metadata.setSpatialProperties(spatialProperties);
		metadata.setAttributes(features);
		
		return new Block(fsName, metadata, data.getBytes("UTF-8"));
	}
	
	
	public static Block createBlockWindDummy(String edfRecord, String data, String fsName) throws UnsupportedEncodingException {
		String[] values = edfRecord.split(",");
		TemporalProperties temporalProperties = new TemporalProperties(reformatDatetime(values[0]));
		SpatialProperties spatialProperties = new SpatialProperties(parseFloat(values[1]), parseFloat(values[2]));
		
		FeatureSet features = getFeaturesWindDummy(values);
		
		Metadata metadata = new Metadata();
		metadata.setName(GeoHash.encode(parseFloat(values[1]), parseFloat(values[2]), 4));
		metadata.setTemporalProperties(temporalProperties);
		metadata.setSpatialProperties(spatialProperties);
		metadata.setAttributes(features);
		
		return new Block(fsName, metadata, data.getBytes("UTF-8"));
	}
	
	
	private static FeatureSet getFeaturesWindDummy(String[] values) {
		
		
		FeatureSet features = new FeatureSet();
		
		features.put(new Feature("epoch_time", values[0]));		
		features.put(new Feature("gps_abs_lat", values[1]));
		features.put(new Feature("gps_abs_lon", values[2]));
		features.put(new Feature("temperature", values[3]));
		features.put(new Feature("humidity", values[4]));
		features.put(new Feature("wind_speed", values[5]));
		features.put(new Feature("wind_dir", values[6]));
		
		return features;
	}
	
	private static FeatureSet getFeaturesSensorDummy(String[] values) {
		
		FeatureSet features = new FeatureSet();
		
		features.put(new Feature("epoch_time", values[0]));		
		features.put(new Feature("gps_abs_lat", values[1]));
		features.put(new Feature("gps_abs_lon", values[2]));
		features.put(new Feature("cavity_pressure", values[3]));
		features.put(new Feature("cavity_temp", values[4]));
		features.put(new Feature("ch4", values[5]));
		
		return features;
	}
	
	private static FeatureSet getFeaturesNoaa(String[] values) {
		
		FeatureSet features = new FeatureSet();
		
		features.put(new Feature("gps_abs_lat", values[0]));		
		features.put(new Feature("gps_abs_lon", values[1]));
		features.put(new Feature("report_type", values[2]));
		features.put(new Feature("elevation", values[3]));
		features.put(new Feature("epoch_time", values[4]));
		features.put(new Feature("weather_station_id", values[5]));
		
		features.put(new Feature("wind_dir_angle", values[6]));		
		features.put(new Feature("wind_dir_typecode", values[7]));
		features.put(new Feature("wind_speed", values[8]));
		features.put(new Feature("sky_ceiling_height", values[9]));
		features.put(new Feature("sky_ceiling_code", values[10]));
		features.put(new Feature("sky_cavoc", values[11]));
		
		features.put(new Feature("visibility_dist", values[12]));		
		features.put(new Feature("air_temp", values[13]));
		features.put(new Feature("air_dew_point", values[14]));
		features.put(new Feature("atm_pressure", values[15]));
		
		
		return features;
	}
	
	private static FeatureSet getFeaturesSensorDummyTB(String[] values) {
		
		FeatureSet features = new FeatureSet();
		
		features.put(new Feature("epoch_time", values[0]));		
		features.put(new Feature("gps_abs_lat", values[1]));
		features.put(new Feature("gps_abs_lon", values[2]));
		features.put(new Feature("cavity_pressure", values[3]));
		features.put(new Feature("cavity_temp", values[4]));
		features.put(new Feature("ch4", values[5]));
		features.put(new Feature("xtravar1", values[6]));
		features.put(new Feature("xtravar2", values[7]));
		features.put(new Feature("xtravar3", values[8]));
		features.put(new Feature("xtravar4", values[9]));
		features.put(new Feature("xtravar5", values[10]));
		
		return features;
	}
	
	/**
	 * @param values
	 * @return
	 */
	private static FeatureSet getFeaturesWind(String[] values) {
		
		FeatureSet features = new FeatureSet();
		
		features.put(new Feature("gps_abs_lat", values[0]));		
		features.put(new Feature("gps_abs_lon", values[1]));
		features.put(new Feature("epoch_time", values[2]));
		features.put(new Feature("wind_speed", values[3]));
		features.put(new Feature("wind_dir", values[4]));
		
		return features;
	}
	
	/**
	 * @param values
	 * @return
	 */
	private static FeatureSet getFeatures(String[] values, int mode) {
		
		FeatureSet features = new FeatureSet();
		
		features.put(new Feature("epoch_time", values[0]));		
		features.put(new Feature("gps_abs_lat", values[1]));
		features.put(new Feature("gps_abs_lon", values[2]));
		if(mode == 1) {
			features.put(new Feature("fsa_feature", values[3]));
		} else if( mode == 2) {
			features.put(new Feature("fsb_feature", values[3]));
		}
		return features;
	}
	
	
	private static float parseFloat(String input){
		try {
			return Float.parseFloat(input);
		} catch(Exception e){
			return 0.0f;
		}
	}
	
	private static Long parseLong(String input){
		try {
			return Long.parseLong(input);
		} catch(Exception e){
			return 0l;
		}
	}
	
	/*private static double parseDouble(String input){
		try {
			return Double.parseDouble(input);
		} catch(Exception e){
			return 0.0;
		}
	}*/
	// [END createBlock]
	
	// [START reformatDatetime]
	/**
	 * reformat epoch_time from BigQuery table into typical UNIX epoch time
	 * FROM: 1.43699552323E9 TO: 143699552323
	 */
	
	public static long reformatDatetime(String data){
		String tmp = data.replace(".", "").replace("E9", "");
		while(tmp.length()<13){
			tmp+="0";
		}
		return Long.parseLong(tmp); 
	}
	
	public static void main(String arg[]) throws UnsupportedEncodingException {
		
		String data = "Hello\nWorld";
		byte[] bytes = data.getBytes("UTF-8");
		String ret = new String(bytes);
		System.out.println(ret+"\n");
		
		SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
		//long d = Long.valueOf("1417463510.81");
		Calendar c = Calendar.getInstance();
		c.setTimeZone(TimeZone.getTimeZone("GMT"));
		
		c.setTimeInMillis(1417463510810L);
		System.out.println(c.getTime());
		
		
	}
	// [END reformatDatetime]

	public static Block createBlockSensorDummy(String edfRecord, String data, String fsName) throws UnsupportedEncodingException{
		
		String[] values = edfRecord.split(",");
		TemporalProperties temporalProperties = new TemporalProperties(reformatDatetime(values[0]));
		SpatialProperties spatialProperties = new SpatialProperties(parseFloat(values[1]), parseFloat(values[2]));
		
		FeatureSet features = getFeaturesSensorDummy(values);
		
		Metadata metadata = new Metadata();
		metadata.setName(GeoHash.encode(parseFloat(values[1]), parseFloat(values[2]), 4));
		metadata.setTemporalProperties(temporalProperties);
		metadata.setSpatialProperties(spatialProperties);
		metadata.setAttributes(features);
		
		return new Block(fsName, metadata, data.getBytes("UTF-8"));
	}
	
	public static Block createBlockNoaa(String edfRecord, String data, String fsName, int latPosn, int lonPosn, int timePosn) throws UnsupportedEncodingException{
		
		String[] values = edfRecord.split(",");
		TemporalProperties temporalProperties = new TemporalProperties(reformatDatetime(values[timePosn]));
		SpatialProperties spatialProperties = new SpatialProperties(parseFloat(values[latPosn]), parseFloat(values[lonPosn]));
		
		FeatureSet features = getFeaturesNoaa(values);
		
		Metadata metadata = new Metadata();
		metadata.setName(GeoHash.encode(parseFloat(values[latPosn]), parseFloat(values[lonPosn]), 2));
		metadata.setTemporalProperties(temporalProperties);
		metadata.setSpatialProperties(spatialProperties);
		metadata.setAttributes(features);
		
		return new Block(fsName, metadata, data.getBytes("UTF-8"));
	}
	
	public static Block createBlockNam(String edfRecord, String data, String fsName, int latPosn, int lonPosn, int timePosn) throws UnsupportedEncodingException{
		
		String[] values = edfRecord.split(",");
		TemporalProperties temporalProperties = new TemporalProperties(reformatDatetime(values[timePosn]));
		SpatialProperties spatialProperties = new SpatialProperties(parseFloat(values[latPosn]), parseFloat(values[lonPosn]));
		
		FeatureSet features = getFeaturesNAM(values);
		
		Metadata metadata = new Metadata();
		metadata.setName(GeoHash.encode(parseFloat(values[latPosn]), parseFloat(values[lonPosn]), 2));
		metadata.setTemporalProperties(temporalProperties);
		metadata.setSpatialProperties(spatialProperties);
		metadata.setAttributes(features);
		
		return new Block(fsName, metadata, data.getBytes("UTF-8"));
	}
	
	
	
	private static FeatureSet getFeaturesNAM(String[] values) {
		
		FeatureSet features = new FeatureSet();
		features.put(new Feature("gps_abs_lat", values[0]));
		features.put(new Feature("gps_abs_lon", values[1]));
		features.put(new Feature("epoch_time", values[2]));
		features.put(new Feature("geopotential_height_lltw", values[3]));
		features.put(new Feature("water_equiv_of_accum_snow_depth_surface", values[4]));
		features.put(new Feature("drag_coefficient_surface", values[5]));
		features.put(new Feature("sensible_heat_net_flux_surface", values[6]));
		features.put(new Feature("categorical_ice_pellets_yes1_no0_surface", values[7]));
		features.put(new Feature("visibility_surface", values[8]));
		features.put(new Feature("number_of_soil_layers_in_root_zone_surface", values[9]));
		features.put(new Feature("categorical_freezing_rain_yes1_no0_surface", values[10]));
		features.put(new Feature("pressure_reduced_to_msl_msl", values[11]));
		features.put(new Feature("upward_short_wave_rad_flux_surface", values[12]));
		features.put(new Feature("relative_humidity_zerodegc_isotherm", values[13]));
		features.put(new Feature("missing_pblri", values[14]));
		features.put(new Feature("categorical_snow_yes1_no0_surface", values[15]));
		features.put(new Feature("u-component_of_wind_tropopause", values[16]));
		features.put(new Feature("surface_wind_gust_surface", values[17]));
		features.put(new Feature("total_cloud_cover_entire_atmosphere", values[18]));
		features.put(new Feature("upward_long_wave_rad_flux_surface", values[19]));
		features.put(new Feature("land_cover_land1_sea0_surface", values[20]));
		features.put(new Feature("vegitation_type_as_in_sib_surface", values[21]));
		features.put(new Feature("v-component_of_wind_pblri", values[22]));
		features.put(new Feature("convective_precipitation_surface_1_hour_accumulation", values[23]));
		features.put(new Feature("albedo_surface", values[24]));
		features.put(new Feature("lightning_surface", values[25]));
		features.put(new Feature("ice_cover_ice1_no_ice0_surface", values[26]));
		features.put(new Feature("convective_inhibition_surface", values[27]));
		features.put(new Feature("pressure_surface", values[28]));
		features.put(new Feature("transpiration_stress-onset_soil_moisture_surface", values[29]));
		features.put(new Feature("soil_porosity_surface", values[30]));
		features.put(new Feature("vegetation_surface", values[31]));
		features.put(new Feature("categorical_rain_yes1_no0_surface", values[32]));
		features.put(new Feature("downward_long_wave_rad_flux_surface", values[33]));
		features.put(new Feature("planetary_boundary_layer_height_surface", values[34]));
		features.put(new Feature("soil_type_as_in_zobler_surface", values[35]));
		features.put(new Feature("geopotential_height_cloud_base", values[36]));
		features.put(new Feature("friction_velocity_surface", values[37]));
		features.put(new Feature("maximumcomposite_radar_reflectivity_entire_atmosphere", values[38]));
		features.put(new Feature("plant_canopy_surface_water_surface", values[39]));
		features.put(new Feature("v-component_of_wind_maximum_wind", values[40]));
		features.put(new Feature("geopotential_height_zerodegc_isotherm", values[41]));
		features.put(new Feature("mean_sea_level_pressure_nam_model_reduction_msl", values[42]));
		features.put(new Feature("total_precipitation_surface_1_hour_accumulation", values[43]));
		features.put(new Feature("temperature_surface", values[44]));
		features.put(new Feature("snow_cover_surface", values[45]));
		features.put(new Feature("geopotential_height_surface", values[46]));
		features.put(new Feature("convective_available_potential_energy_surface", values[47]));
		features.put(new Feature("latent_heat_net_flux_surface", values[48]));
		features.put(new Feature("surface_roughness_surface", values[49]));
		features.put(new Feature("pressure_maximum_wind", values[50]));
		features.put(new Feature("temperature_tropopause", values[51]));
		features.put(new Feature("geopotential_height_pblri", values[52]));
		features.put(new Feature("pressure_tropopause", values[53]));
		features.put(new Feature("snow_depth_surface", values[54]));
		features.put(new Feature("v-component_of_wind_tropopause", values[55]));
		features.put(new Feature("downward_short_wave_rad_flux_surface", values[56]));
		features.put(new Feature("u-component_of_wind_maximum_wind", values[57]));
		features.put(new Feature("wilting_point_surface", values[58]));
		features.put(new Feature("precipitable_water_entire_atmosphere", values[59]));
		features.put(new Feature("u-component_of_wind_pblri", values[60]));
		features.put(new Feature("direct_evaporation_cease_soil_moisture_surface", values[61]));
		
		return features;
	}
	public static Block createBlockSensorDummyTB(String edfRecord, String data, String fsName) throws UnsupportedEncodingException{
		
		String[] values = edfRecord.split(",");
		TemporalProperties temporalProperties = new TemporalProperties(reformatDatetime(values[0]));
		SpatialProperties spatialProperties = new SpatialProperties(parseFloat(values[1]), parseFloat(values[2]));
		
		FeatureSet features = getFeaturesSensorDummyTB(values);
		
		Metadata metadata = new Metadata();
		metadata.setName(GeoHash.encode(parseFloat(values[1]), parseFloat(values[2]), 4));
		metadata.setTemporalProperties(temporalProperties);
		metadata.setSpatialProperties(spatialProperties);
		metadata.setAttributes(features);
		
		return new Block(fsName, metadata, data.getBytes("UTF-8"));
	}
}
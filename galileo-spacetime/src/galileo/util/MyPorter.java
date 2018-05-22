package galileo.util;

import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

// Porting
// Author: Saptashwa Mitra
// Date:   Mar 23, 2018
// Class:  CS163
// Email:  eid@cs.colostate.edu

/**
 * @author Saptashwa
 *
 */
public class MyPorter {

	List<double[][]> weight_matrices;
	List<double[]> bias_matrices;
	
	double[] means = new double[3];
	double[] sigmas = new double[3];
	
	
	
	public static void main(String[] args) {
		
		//String conf = "{\"total_layers\":2,\"layer_shapes\":[\"(3, 2)\",\"(2, 1)\"],\"indv_layers\":[\"[[-1.00456745e+00 -3.46978328e-01] [-5.65508870e-01 -1.64969949e-01] [-1.11628716e-04 -5.23894543e-01]]\",\"[[-0.39979925] [-0.35616258]]\"],\"bias_shapes\":[2,1],\"bias_layers\":[\"[-0.97296882 -0.70836777]\",\"[0.1704212]\"]}";
		
		String conf = "";
		
		System.out.println(conf);
		MyPorter mp = new MyPorter(conf);
		double[] input = {45.885f,-82.568f,1.420203600034E12};
		System.out.println(mp.predict(input));
	}
	
	public MyPorter() {
		
	}
	
	public MyPorter(String layers_string) {
		
		JSONObject obj = new JSONObject(layers_string);
		
		int totalLayers = obj.getInt("total_layers");
		
		
		JSONArray stds = obj.getJSONArray("stds");
		
		for (int i = 0; i < stds.length(); i++)
		{
			//layer_shapes.get(0)
		    double std = stds.getDouble(i);
		    sigmas[i] = std;
		}
		
		JSONArray meansj = obj.getJSONArray("means");
		
		for (int i = 0; i < meansj.length(); i++)
		{
			//layer_shapes.get(0)
		    double mean = meansj.getDouble(i);
		    means[i] = mean;
		}
		
		JSONArray layer_shapes = obj.getJSONArray("layer_shapes");
		
		String[] layers_conf = new String[totalLayers];
		
		for (int i = 0; i < layer_shapes.length(); i++)
		{
			//layer_shapes.get(0)
		    String layr = layer_shapes.get(i).toString();
		    layr = layr.substring(1, layr.length() - 1);
		    layers_conf[i] = layr;
		}
		
		JSONArray bias_shapes = obj.getJSONArray("bias_shapes");
		
		int[] bias_conf = new int[totalLayers];
		
		for (int i = 0; i < bias_shapes.length(); i++)
		{
			//layer_shapes.get(0)
		    int layr = bias_shapes.getInt(i);
		    bias_conf[i] = layr;
		}
		
		List<double[][]> weight_matrices = new ArrayList<double[][]>();
		
		JSONArray layers = obj.getJSONArray("indv_layers");
		
		for (int i = 0; i < layers.length(); i++)
		{
			//layer_shapes.get(0)
		    String each_layer = layers.get(i).toString();
		    
		    String[] tokens = layers_conf[i].split(",");
		    int row = Integer.valueOf(tokens[0].trim());
		    int col = Integer.valueOf(tokens[1].trim());
		    
		    double[][] this_layer = new double[row][col];
		    
		    each_layer = each_layer.replace("]]", "");
		    each_layer = each_layer.replace("[[", "");
		    each_layer = each_layer.replace("[", "");
		    each_layer = each_layer.trim();
		    String[] lines = each_layer.split("]");
		    
		    for(int j = 0; j < row; j++) {
		    	String[] units = lines[j].trim().split("\\s+");
		    	for(int k = 0 ; k < col; k++) {
		    		this_layer[j][k] = Double.valueOf(units[k]);
		    		
		    	}
		    }
		    
		    
		    weight_matrices.add(this_layer);
		}
		
		this.weight_matrices = weight_matrices;
		List<double[]> bias_matrices = new ArrayList<double[]>();
		
		JSONArray bias_layers = obj.getJSONArray("bias_layers");
		
		for (int i = 0; i < bias_layers.length(); i++)
		{
			//layer_shapes.get(0)
		    String each_layer = bias_layers.get(i).toString();
		   
		    int col = bias_conf[i];
		    
		    double[] this_layer = new double[col];
		    
		    each_layer = each_layer.replace("[", "");
		    each_layer = each_layer.replace("]", "");
		    each_layer = each_layer.trim();
		    String[] lines = each_layer.split("\\s+");
		    
		    for(int j = 0; j < col; j++) {
		    	//System.out.println("RIKI "+lines[j].trim());
		    	this_layer[j] = Double.valueOf(lines[j].trim());
		    		
		    }
		    
			bias_matrices.add(this_layer);
		}
		this.bias_matrices = bias_matrices;
		
		
	}
	
	public double predict(double[] input) {
		
		input[0] = (input[0] - means[0]) / sigmas[0];
		input[1] = (input[1] - means[1]) / sigmas[1];
		input[2] = (input[2] - means[2]) / sigmas[2];
		
		double[][] new_input = new double[1][input.length];
		
		new_input[0] = input;
		
		int count = 0;
		for(double[][] layer: weight_matrices) {
			
			new_input = mat_mult(new_input, layer);
			double[] bias = bias_matrices.get(count);
			for(int i=0; i < bias.length; i++) {
				new_input[0][i]+=bias[i];
			}
			
			for (int i = 0; i < new_input[0].length; i++) {
				new_input[0][i] = java.lang.Math.max(0, new_input[0][i]);
            }
			
			count++;
			
		}
		//System.out.println(new_input[0][0]);
		return new_input[0][0];
		
	}
	
	
	public double[][] mat_mult(double[][] A, double[][] B) {

        int aRows = A.length;
        int aColumns = A[0].length;
        int bRows = B.length;
        int bColumns = B[0].length;

        if (aColumns != bRows) {
            throw new IllegalArgumentException("A:Rows: " + aColumns + " did not match B:Columns " + bRows + ".");
        }

        double[][] C = new double[aRows][bColumns];
        for (int i = 0; i < aRows; i++) {
            for (int j = 0; j < bColumns; j++) {
                C[i][j] = 0.00000;
            }
        }

        for (int i = 0; i < aRows; i++) { // aRow
            for (int j = 0; j < bColumns; j++) { // bColumn
                for (int k = 0; k < aColumns; k++) { // aColumn
                    C[i][j] += A[i][k] * B[k][j];
                }
            }
        }

        return C;
    }
	
	

}

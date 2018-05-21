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
	
	public static void main(String[] args) {
		
		//String conf = "{\"total_layers\":2,\"layer_shapes\":[\"(3, 2)\",\"(2, 1)\"],\"indv_layers\":[\"[[-1.00456745e+00 -3.46978328e-01] [-5.65508870e-01 -1.64969949e-01] [-1.11628716e-04 -5.23894543e-01]]\",\"[[-0.39979925] [-0.35616258]]\"],\"bias_shapes\":[2,1],\"bias_layers\":[\"[-0.97296882 -0.70836777]\",\"[0.1704212]\"]}";
		
		String conf = "{\"total_layers\":2,\"layer_shapes\":[\"(3, 50)\",\"(50, 1)\"],\"indv_layers\":[\"[[ 0.17492565  0.22624733 -0.18464305  0.38070968  0.18105495  0.23778005    0.28065916  0.01704412  0.08493054  0.24958979  0.23139697  0.05795779    0.32935246 -0.30619321 -0.21912054 -0.11259506 -0.33699406 -0.2135663    0.25579765 -0.00456527  0.00138487 -0.06591373 -0.06087076  0.16793783    0.2227542   0.15844133  0.159827    0.06642007  0.19689879  0.05658194   -0.05967005  0.07781188 -0.21085917 -0.35594173 -0.11784023  0.26991208   -0.13317925 -0.18176083  0.40164136 -0.34825561  0.11463529 -0.18667649    0.28059129  0.2551895   0.22733835 -0.24876592  0.20116048 -0.24394941   -0.2869695  -0.312238  ]  [-0.05088619  0.0756305   0.22670956  0.4486614   0.15899969 -0.33706017   -0.35499586  0.21893089 -0.1932309  -0.22995959  0.06473486  0.00822133   -0.19644407  0.36241621  0.25225688  0.21359332  0.3526046   0.1718769    0.04034226 -0.00430434 -0.02580464  0.2831264  -0.17162876 -0.15639765    0.13752189  0.10254916  0.10554794  0.01779452 -0.00261831  0.29044634    0.29651096 -0.21399241 -0.28086445 -0.065806    0.03014474  0.22382082    0.23205791  0.28091653  0.29090349 -0.39772417  0.26933476 -0.01350902   -0.01155705  0.14560441  0.04852306  0.24422378 -0.33423429 -0.42154985   -0.15078658 -0.37432615]  [ 0.17765493 -0.10806054 -0.27077089  0.15191285  0.16100224 -0.15475391    0.2686728  -0.17297076 -0.20020927 -0.1076173  -0.17341704 -0.02032962    0.34346486  0.36726627  0.22003767  0.03086237 -0.27084304 -0.04772064    0.20153048 -0.591659    0.47417108 -0.07769991  0.29538162 -0.07976259   -0.20415225 -0.01629138 -0.18082942 -0.61698466  0.18772717  0.03689228   -0.33657214 -0.36106811 -0.25584166 -0.34165612 -0.46406409 -0.20085234   -0.07516555 -0.10738182  0.31089054 -0.33151809 -0.12863499  0.04001519   -0.26144197 -0.35872754  0.07952786  0.24184806 -0.51324948  0.25806816   -0.15825469  0.09031019]]\",\"[[ 0.19223121]  [ 0.17302645]  [ 0.37443504]  [-0.69870977]  [ 0.20476691]  [ 0.20418506]  [ 0.07124232]  [ 0.19219916]  [-0.19627385]  [-0.05869205]  [ 0.36111923]  [-0.12227792]  [-0.45329717]  [ 0.38137411]  [ 0.01568987]  [-0.21310679]  [-0.05884025]  [ 0.51480443]  [-0.00842951]  [-0.81766433]  [ 0.52341633]  [-0.07443893]  [ 0.23993831]  [ 0.41866506]  [-0.18407721]  [ 0.1520756 ]  [ 0.21029796]  [-0.29708624]  [ 0.10966135]  [-0.04650071]  [ 0.2802638 ]  [ 0.38043071]  [ 0.32830003]  [ 0.20225542]  [ 0.33715671]  [ 0.30968407]  [-0.19859126]  [-0.22535783]  [ 0.34940433]  [ 0.37987289]  [-0.34299317]  [ 0.21779996]  [ 0.34994304]  [ 0.0896226 ]  [ 0.50255803]  [ 0.19522304]  [-0.57691681]  [ 0.10458143]  [ 0.1912279 ]  [ 0.08149094]]\"],\"bias_shapes\":[50,1],\"bias_layers\":[\"[ 2.44899036e-01  5.93262892e-01  3.53932195e-01 -1.52856605e-01   7.14173641e-01  2.44062642e-01  6.07145676e-02  5.87251521e-01   2.40415808e-02  1.61991586e-01  5.82854594e-01 -1.71935986e-01  -1.11519425e-01  1.37304122e-01  1.03752828e-01 -3.27385551e-04  -6.63323797e-02  5.19490654e-01  4.82184305e-02 -2.54242384e-01  -2.23053882e-01 -4.26193435e-02 -2.86665616e-02  3.06351880e-01   1.13738209e-01  3.89810613e-01  5.90042215e-01 -2.72155294e-01   3.75003317e-01  1.68693053e-01  2.30507764e-01  4.89539787e-01   1.04254356e-01  6.72857538e-01  3.78681302e-01  4.05026568e-01   4.85267082e-02  1.24186048e-01  4.73985509e-01  2.88798533e-01  -1.80274866e-01  5.67249180e-01  2.61768823e-01 -2.86944764e-02   5.67999333e-01  4.36712618e-01 -4.22880965e-01  2.66449560e-01   6.48534638e-01  1.91830061e-01]\",\"[-0.06177688]\"]}";
		
		System.out.println(conf);
		MyPorter mp = new MyPorter(conf);
		double[] input = {1.32233804f,0.80299488f,-1.58806025f};
		mp.predict(input);
	}
	
	public MyPorter() {
		
	}
	
	public MyPorter(String layers_string) {
		
		JSONObject obj = new JSONObject(layers_string);
		
		int totalLayers = obj.getInt("total_layers");
		
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
		System.out.println(new_input[0][0]);
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

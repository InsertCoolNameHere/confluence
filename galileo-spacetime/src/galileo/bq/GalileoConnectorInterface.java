package galileo.bq;
import java.io.IOException;
import java.util.List;

import galileo.client.EventPublisher;
import galileo.comm.DataIntegrationRequest;
import galileo.comm.FilesystemAction;
import galileo.comm.FilesystemRequest;
import galileo.comm.StorageRequest;
import galileo.comm.SurveyRequest;
import galileo.comm.TemporalType;
import galileo.dataset.Block;
import galileo.dataset.SpatialHint;
import galileo.dataset.feature.FeatureType;
import galileo.net.ClientMessageRouter;
import galileo.net.NetworkDestination;
import galileo.util.Pair;

abstract class GalileoConnectorInterface {
	private ClientMessageRouter messageRouter;
	private EventPublisher publisher;
	private NetworkDestination server;
	
	public GalileoConnectorInterface(String serverHostName, int serverPort) throws IOException {
		messageRouter = new ClientMessageRouter();
		publisher = new EventPublisher(messageRouter);
		server = new NetworkDestination(serverHostName, serverPort);
	}
	
	public void store(Block fb) throws Exception {
		StorageRequest store = new StorageRequest(fb);
		publisher.publish(server, store);
	}
	
	/*public void createFS(String name) throws IOException {
		
		FilesystemRequest fsRequest = new FilesystemRequest("nyc_yellow_taxi", FilesystemAction.CREATE, featureList, spatialHint);
		FilesystemRequest fsr = new FilesystemRequest(name, FilesystemAction.CREATE, pr);
		publisher.publish(server, fsr);
	}*/
	/*public void createFS(String name, float a, float b) throws IOException {
		PrecisionLimit pr = new PrecisionLimit(a, b);
		FileSystemRequest fsr = new FileSystemRequest(name, FileSystemAction.CREATE, pr);
		publisher.publish(server, fsr);
	}*/
	
	public void createFS(String name, SpatialHint sh,List<Pair<String, FeatureType>> featureList, String temporalHint, int mode) throws IOException {
		int spUnc = 0;
		int tempUnc = 0;
		if(mode == 1) {
			
			spUnc = 5;
			// 1hr
			tempUnc = 60*60*1000;
			
		} else if(mode == 2) {
			
			spUnc = 5;
			tempUnc = 60*60*1000;
			
		} else if (mode == 3) {
			// For Sensor FS
			spUnc = 5;
			
			// 5 mins
			tempUnc = 5*60*1000;
			// geohash precision for storage is preset to 4
		}
		FilesystemRequest fsRequest = new FilesystemRequest(name, FilesystemAction.CREATE, featureList, sh, spUnc, tempUnc,  false, temporalHint);
		
		fsRequest.setNodesPerGroup(2);
		//fsRequest.setPrecision(6);
		fsRequest.setTemporalType(TemporalType.DAY_OF_MONTH);
		
		publisher.publish(server, fsRequest);
	}
	
	public void integrate(DataIntegrationRequest dr) throws IOException {
		publisher.publish(server, dr);
	}
	
	public void survey(SurveyRequest sr) throws IOException {
		publisher.publish(server, sr);
	}
	
	public void disconnect() {
		messageRouter.shutdown();
	}

}

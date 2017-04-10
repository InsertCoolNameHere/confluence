package galileo.samples;

import galileo.client.EventPublisher;
import galileo.comm.DataIntegrationResponse;
import galileo.comm.GalileoEventMap;
import galileo.comm.QueryRequest;
import galileo.comm.QueryResponse;
import galileo.dataset.feature.Feature;
import galileo.event.BasicEventWrapper;
import galileo.event.EventWrapper;
import galileo.net.ClientMessageRouter;
import galileo.net.GalileoMessage;
import galileo.net.MessageListener;
import galileo.net.NetworkDestination;
import galileo.query.Expression;
import galileo.query.Operation;
import galileo.query.Query;

public class DataIntegrationDemo implements MessageListener {
	
	private static GalileoEventMap eventMap = new GalileoEventMap();
	private static EventWrapper wrapper = new BasicEventWrapper(eventMap);

	@Override
	public void onConnect(NetworkDestination endpoint) {
	}

	@Override
	public void onDisconnect(NetworkDestination endpoint) {
	}

	@Override
	public void onMessage(GalileoMessage message) {
		try {
			DataIntegrationResponse response = (DataIntegrationResponse) wrapper.unwrap(message);
			//System.out.println(response.getJSONResults().toString());
			
			
			/*System.out.println(response.getResults().size() + " results received");
			List<List<String>> results = response.getResults();
			for (int i = 0; i < Math.min(5, results.size()); i++) {
				System.out.println(results.get(i));
			}*/
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.out.println("Usage: galileo.samples.DataIntegrationDemo host port");
			System.exit(1);
		}
		String serverHostName = args[0];
		int serverPort = Integer.parseInt(args[1]);
		NetworkDestination server = new NetworkDestination(serverHostName, serverPort);

		DataIntegrationDemo qd = new DataIntegrationDemo();

		ClientMessageRouter messageRouter = new ClientMessageRouter();
		messageRouter.addListener(qd);

		/*
		 * Each expression chained together in an operation produces a logical
		 * AND. Each operation added to the query is like a logical OR.
		 */

		/* This query checks for total_precipitation values equal to 83.30055 */
		Query q = new Query();
		Operation o = new Operation(new Expression(">=", new Feature("ch4", 0.0f)));
		q.addOperation(o);

		System.out.println("Query 1: " + q);

		QueryRequest qr = new QueryRequest("samples", q, null);
		messageRouter.sendMessage(server, EventPublisher.wrapEvent(qr));
	}

}

package galileo.comm;

import java.io.IOException;
import java.util.List;

import galileo.dataset.Coordinates;
import galileo.event.Event;
import galileo.serialization.SerializationOutputStream;

public class DataIntegrationRequest implements Event{
	
	private List<Coordinates> polygon;
	private String time;

	@Override
	public void serialize(SerializationOutputStream out) throws IOException {
		// TODO Auto-generated method stub
		
	}

}

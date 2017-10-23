package galileo.dht;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import galileo.dht.hash.TemporalHash;

public class TestClass {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		String s = "1497808800000-1498291200000";
		
		TestClass sn = new TestClass();
		sn.handleTemporalRangeForEachBlock(s);
	}
	
	private void handleTemporalRangeForEachBlock(String time) {
		// TODO Auto-generated method stub
		String[] tokens = time.split("-");
		
		long start = Long.parseLong(tokens[0]);
		long end = Long.parseLong(tokens[1]);
		
		Calendar cs = Calendar.getInstance();
		cs.setTimeZone(TemporalHash.TIMEZONE);
		cs.setTimeInMillis(start);
		
		Calendar ce = Calendar.getInstance();
		ce.setTimeZone(TemporalHash.TIMEZONE);
		ce.setTimeInMillis(end);
		
		Date ds = cs.getTime();
		Date de = ce.getTime();
		
		List<Date> daysBetweenDates = getDaysBetweenDates(ds,de);
		
		System.out.println(daysBetweenDates);
	}

	private List<Date> getDaysBetweenDates(Date startdate, Date enddate) {
		
		Calendar cend = Calendar.getInstance();
	    cend.setTimeZone(TemporalHash.TIMEZONE);
	    cend.setTime(enddate);
	    
	    
	    List<Date> dates = new ArrayList<Date>();
	    Calendar calendar = Calendar.getInstance();
	    calendar.setTimeZone(TemporalHash.TIMEZONE);
	    calendar.setTime(startdate);

	    while (calendar.getTime().before(enddate))
	    {
	        Date result = calendar.getTime();
	        dates.add(result);
	        calendar.add(Calendar.DATE, 1);
	    }
	    
	    if(calendar.getTimeInMillis() > cend.getTimeInMillis()) {
	    	dates.add(cend.getTime());
	    }
	    return dates;
	}

}

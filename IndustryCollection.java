package industries;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import com.bloomberglp.blpapi.CorrelationID;
import com.bloomberglp.blpapi.Event;
import com.bloomberglp.blpapi.Message;
import com.bloomberglp.blpapi.MessageIterator;
import com.bloomberglp.blpapi.Request;
import com.bloomberglp.blpapi.Service;
import com.bloomberglp.blpapi.Session;

import gather.Api;
import gather.tools;

public class IndustryCollection {
	static File input = new File("allSecurities.csv");
	static File output = new File("industries.csv");
	public static void main(String[] args) {
		Set<String> securities = readSecurities();
		Map<String, String> pairings  = gather(securities);
		printMap(pairings);
			
	}
	public static Map<String, String> gather(Set<String> securities){
		Map<String,String> result = new HashMap<String, String>();
		Session session =Api.setupSession();
		Service service = session.getService("//blp/refdata");
		
		//form requests
		Request req = formRequest(service, securities);
		
		//send requests
		try {
			session.sendRequest(req, new CorrelationID());
		} catch (IOException e) {
			System.out.println("error in sending request");
		}
		
		//collect requests
		
		return result;
	}
	public static void collectEvents(Session sess){
		boolean keepLooping = true;
		while(keepLooping){
			Event event = null;
			try {
				event = sess.nextEvent(); 
			} 
			catch (InterruptedException e) {
				System.out.println("error in getting next event");
				e.printStackTrace();
			}
			switch (event.eventType().intValue()){
				case Event.EventType.Constants.RESPONSE: // final event
					keepLooping = false; // fall through
				case Event.EventType.Constants.PARTIAL_RESPONSE:
					handleResponseEvent(event);
					break;
				default:
					Api.handleOtherEvent(event);
					break;
			}
		}
	}
	public static void handleResponseEvent(Event e){
		MessageIterator it = e.messageIterator();
		while(it.hasNext()){
			Message m = it.next();
			try{
				m.print(System.out);
			}
			catch(Exception exc){
				System.out.println("error in processing message");
			}
		}
	}
	public static Request formRequest(Service service, Collection<String> foo){
		Request req = service.createRequest("HistoricalDataRequest");
		req.getElement("fields").appendValue("industry");
		for(String s: foo)
			req.getElement("securities").appendValue(s);
		req.set("startDate", "20000101");
		req.set("endDate", "20151231");
		return req;
	}
	public static void printMap(Map<String, String> foo){
		try {
			BufferedWriter write = new BufferedWriter(new FileWriter(output));
			for(String x: foo.keySet())
				write.write(x + "," + foo.get(x) + "\n");
				
			write.close();
			System.out.println("All Done!");
		} catch (IOException e) {
			System.out.println("error in printing");
		}
	}
	public static Set<String> readSecurities(){
		Set<String> result = new HashSet<String>();
		Scanner scan = tools.openScan(input);
		scan.nextLine();
		while(scan.hasNextLine()){
			String[] line = scan.nextLine().split(",");
			for(String s: line){
				if(s.length()>5)
					result.add(s);
			}
		}
		return result;
	}
}

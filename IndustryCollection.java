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
import com.bloomberglp.blpapi.Element;
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
	
	Map<String, String> pairings;
	Set<String> securities;
	
	public static void main(String[] args) {
		IndustryCollection ic = new IndustryCollection();
		System.out.println("reading");
		ic.securities = readSecurities();
		System.out.println("gathering");
		ic.gather();
		System.out.println("printing");
		ic.exportPairings();
	}
	public IndustryCollection(){
		this.pairings = new HashMap<String, String>();
	}
	public void gather(){
		System.out.println("set up");
		Session session =Api.setupSession();
		Service service = session.getService("//blp/refdata");

		System.out.println("forming request");
		//form requests
		Request req = formRequest(service, securities);

		System.out.println("sending request");
		//send requests
		try {
			session.sendRequest(req, new CorrelationID());
		} catch (IOException e) {
			System.out.println("error in sending request");
		}

		System.out.println("collecting");
		//collect requests
		collectEvents(session);

	}
	public void collectEvents(Session sess){
		boolean keepLooping = true;
		while(keepLooping){
			System.out.print(".");
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
	public void handleResponseEvent(Event e){
		MessageIterator it = e.messageIterator();
		while(it.hasNext()){
			Message message = it.next();
			Element securityDataArray = message.getElement("securityData");

			for( int i = 0; i<securityDataArray.numValues(); i++){
				Element securityData = securityDataArray.getValueAsElement(i);
				String name = securityData.getElementAsString("security");
				Element fieldData = securityData.getElement("fieldData");
				String industry = fieldData.getElementAsString("INDUSTRY_SECTOR");
				this.pairings.put(name, industry);
			}
		}
	}
	public static Request formRequest(Service service, Collection<String> foo){
		Request req = service.createRequest("ReferenceDataRequest");
		req.getElement("fields").appendValue("INDUSTRY_SECTOR");
		for(String s: foo)
			req.getElement("securities").appendValue(s);
		return req;
	}
	public void exportPairings(){
		try {
			BufferedWriter write = new BufferedWriter(new FileWriter(output));
			for(String x: this.pairings.keySet()){
				String industry = pairings.get(x).replace(',', ';');
				write.write(x + "," + industry + "\n");
			}
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
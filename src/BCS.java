import java.util.ArrayList;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

//Create BCS here, which manages everything
public class BCS {
	
	public static enum InitialDeployment {
		RND,
		NRB,
		RR;

		
	}
	
	//--------------------------------------------
	static InitialDeployment ip = InitialDeployment.RR;
	public static double rangeMin = -90;
	public static double rangeMax = 90;
	public static int subscriber_amount = 100;
	public static int broker_amount = 5;
	//--------------------------------------------
	public static Broker initialAssignSubscriber(Subscriber a, Map<String, Broker> brokerlist, int brokerid) {
		switch(ip) {
		case RND:
			Random r = new Random();
			int randombroker = 0+4*r.nextInt();
			switch(randombroker) {
			case 0:
				return brokerlist.get("b0");
			case 1:
				return brokerlist.get("b1");
			
			case 2:
				return brokerlist.get("b2");
			
			case 3:
				return brokerlist.get("b3");
			
			case 4:
				return brokerlist.get("b4");
			}
		case NRB:
			double lat1 = a.nscoord;
			double lon1 = a.wecoord;
			double dist0 = distance(lat1, brokerlist.get("b0").nscoord, lon1, brokerlist.get("b0").wecoord);
			double dist1 = distance(lat1, brokerlist.get("b1").nscoord, lon1, brokerlist.get("b1").wecoord);
			double dist2 = distance(lat1, brokerlist.get("b2").nscoord, lon1, brokerlist.get("b2").wecoord);
			double dist3 = distance(lat1, brokerlist.get("b3").nscoord, lon1, brokerlist.get("b3").wecoord);
			double dist4 = distance(lat1, brokerlist.get("b4").nscoord, lon1, brokerlist.get("b4").wecoord);
			double smallest = Math.min(dist0, Math.min(dist0, Math.min(dist1, Math.min(dist2, Math.min(dist3, dist4)))));
			if(smallest == dist0) {
				return brokerlist.get("b0");
			}else if (smallest == dist1) {
				return brokerlist.get("b1");
			}else if (smallest == dist2) {
				return brokerlist.get("b2");
			}else if (smallest == dist3) {
				return brokerlist.get("b3");
			}else if (smallest == dist4) {
				return brokerlist.get("b4");
			}
			break;
		case RR:
			if(brokerid == 0) {
				return brokerlist.get("b0");
			}else if (brokerid == 1) {
				return brokerlist.get("b1");
			}else if (brokerid == 2) {
				return brokerlist.get("b2");
			}else if (brokerid == 3) {
				return brokerlist.get("b3");
			}else if (brokerid == 4) {
				return brokerlist.get("b4");
			}
			break;
		}
		return brokerlist.get("b0");
	}
	//Credit to https://stackoverflow.com/questions/3694380/calculating-distance-between-two-points-using-latitude-longitude
	public static double distance(double lat1, double lat2, double lon1,
	        double lon2) {

	    final int R = 6371; // Radius of the earth

	    double latDistance = Math.toRadians(lat2 - lat1);
	    double lonDistance = Math.toRadians(lon2 - lon1);
	    double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
	            + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
	            * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
	    double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
	    double distance = R * c * 1000; // convert to meters


	    distance = Math.pow(distance, 2);

	    return Math.sqrt(distance);
	}
	public static void main(String[] args) {
        System.out.println("System starting...");
        double randomNSCoord;
        double randomWECoord;
        Map<String, Broker> brokerlist = new TreeMap<String,Broker>();
        Map<String, Subscriber> subscriberlist = new TreeMap<String, Subscriber>();
        
        
        /*for(int i =0; i<broker_amount;i++) {	
        	String brokername = "b"+Integer.toString(i);
        	brokersystem.put(brokername, new Broker(0,0,i));
        }*/
        brokerlist.put("b0", new Broker(0,0,0));
        brokerlist.put("b1", new Broker(45,45,1));
        brokerlist.put("b2", new Broker(45,-45,2));
        brokerlist.put("b3", new Broker(-45,45,3));
        brokerlist.put("b4", new Broker(-45,-45,4));
        //Create all Subscribers with variable locations
        int brokerid = 0;
        for(int i =0; i<subscriber_amount;i++) {
        	Random r = new Random();
        	randomNSCoord = rangeMin + (rangeMax - rangeMin) * r.nextDouble();
        	randomWECoord = rangeMin + (rangeMax - rangeMin) * r.nextDouble(); 	
        	String subname = "s"+Integer.toString(i);
        	subscriberlist.put(subname,new Subscriber (randomNSCoord,randomWECoord,i,0));
        	Subscriber a = subscriberlist.get(subname);
        	
        	Broker b  = initialAssignSubscriber(a, brokerlist, brokerid);
        	brokerid+=1;
        	if(brokerid == 5) {
        		brokerid =0;
        	}
        	b = b.AssignSubscribertoBroker(a, b);
        }
        System.out.println(brokerlist.get("b0").sub_ids.size());
        System.out.println(brokerlist.get("b1").sub_ids.size());
        System.out.println(brokerlist.get("b2").sub_ids.size());
        System.out.println(brokerlist.get("b3").sub_ids.size());
        System.out.println(brokerlist.get("b4").sub_ids.size());
        
	}
}

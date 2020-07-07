import java.util.ArrayList;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.stream.Collectors;

import javax.swing.text.html.HTMLDocument.Iterator;

public class Broker {
	double latPerKM = 0.021048134571484346;
	double latencyFactor = 2;
	double nscoord;
	double wecoord;
	int unhappiness;
	int[] unhappinessArray = new int[BCS.turnLimit];
	String brokername;
	int id;
	int load;
	int[] loadArray = new int[BCS.turnLimit];
	int mean_lat;
	int num_sub;
	Map<Integer, double[]> sub_loc;
	Map<Integer, Subscriber> sub_ids;
	Map<Integer, Double> sub_thresh;
	Map<Integer, Integer> sub_load;
	Map<Integer, Integer> sub_lat;
	
	
	public void removeAllSubscribers() {
		this.sub_ids.clear();
		this.sub_thresh.clear();
		this.sub_load.clear();
		this.sub_lat.clear();
		this.load = calculateLoad();
	}
	public void removeSubscriber(Integer id) {
		this.sub_ids.remove(id);
		this.sub_thresh.remove(id);
		this.sub_load.remove(id);
		this.sub_lat.remove(id);
		this.load = calculateLoad();
	}
	
	public int calculateLoad() {
		//courtesy of https://stackoverflow.com/questions/30089469/how-to-sum-values-in-a-map-with-a-stream
		return sub_load.values().stream().reduce(0, Integer::sum);
	}
	public void calculateLatencyDeviation() {
		
	}
	public Subscriber getHeaviestSubscriber(int count) {
		ArrayList<Integer> keysToIgnore = new ArrayList<>();
		while(count >= 0) {
			double biggestLoad =0;
			int id = 0;
			for (Integer key : sub_load.keySet()) {
				if(!keysToIgnore.contains(key)) {
					if(sub_load.get(key) > biggestLoad) {
						biggestLoad = sub_load.get(key);
						id = key;
					}
				}
			}
			if(count == 0) {
				return sub_ids.get(id);
			}else {
				keysToIgnore.add(id);
				count--;
				
			}
		}
		return sub_ids.get(id);
	}
	public Subscriber getFarthestSubscriber() {
		int biggestDistance =0;
		int id = 0;
		for (Integer key : sub_lat.keySet()) {
			if(sub_lat.get(key) > biggestDistance) {
				biggestDistance = sub_lat.get(key);
				id = key;
			}
		}
		return sub_ids.get(id);
	}
	
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
	    return Math.sqrt(distance)/1000;
	}
	
	//Constructor
	public Broker(double cor1, double cor2, int identifier) {
		this.nscoord = cor1;
		this.wecoord = cor2;
		this.id = identifier;
		this.brokername= "b"+ Integer.toString(this.id);
		this.sub_loc = new TreeMap<Integer, double[]>();
		this.sub_ids = new TreeMap<Integer, Subscriber>();
		this.sub_thresh = new TreeMap<Integer, Double>();
		this.sub_load= new TreeMap<Integer, Integer>();
		this.sub_lat = new TreeMap<Integer, Integer>();
		System.out.println("Broker with id " + this.id + " created"+", Coordinates are "+this.nscoord +", "+ this.wecoord);
	}
	//checks whether a user has reached his latency threshold to be transferred
	public void checkLatencyDMThreshold(Subscriber a) {
		calculateLatencyDeviation();
		for (Integer key : sub_thresh.keySet()) {
			
		}
		BCS.performLatencyDynamicMigration(id);
	}
	public Broker AssignSubscribertoBroker(Subscriber a, Broker b) {
		b.sub_ids.put(a.id, a);
		b.sub_thresh.put(a.id, this.latencyFactor);
		b.sub_load.put(a.id, a.load);
		b.sub_lat.put(a.id,(int)Math.round((distance(a.nscoord,b.nscoord,a.wecoord,b.wecoord)*latPerKM)));
		b.load += a.load;
		b.mean_lat = b.sub_lat.values().stream().reduce(0, Integer::sum)/b.sub_lat.values().size();
		//b.sub_loc.compute(key, remappingFunction)
		//System.out.println("Broker "+b.id+" took user with id "+a.id+", Subscriber has a latency of "+b.sub_lat.get(a.id)+"ms"+" and a unhappiness factor of "+a.unhappiness +" and load : "+ a.load);
		return b;
	}
	
	
	
}
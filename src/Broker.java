import java.util.ArrayList;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

public class Broker {
	double nscoord;
	double wecoord;
	int id;
	int load;
	int mean_lat;
	int num_sub;
	Map<double[], Subscriber> sub_loc;
	Map<Integer, Subscriber> sub_ids;
	Map<Integer, Subscriber> sub_thresh;
	Map<Integer, Subscriber> sub_load;
	Map<Integer, Subscriber> sub_lat;
	
	//Constructor
	public Broker(double cor1, double cor2, int identifier) {
		this.nscoord = cor1;
		this.wecoord = cor2;
		this.id = identifier;
		this.sub_loc = new TreeMap<double[], Subscriber>();
		this.sub_ids = new TreeMap<Integer, Subscriber>();
		this.sub_thresh = new TreeMap<Integer, Subscriber>();
		this.sub_load= new TreeMap<Integer, Subscriber>();
		this.sub_lat = new TreeMap<Integer, Subscriber>();
		System.out.println("Broker with id " + this.id + " created"+", Coordinates are "+this.nscoord +", "+ this.wecoord);
	}
	//checks whether a user has reached his latency threshold to be transferred
	public Boolean checkLatencyThreshold(Subscriber a) {
		return false;
	}
	public Broker AssignSubscribertoBroker(Subscriber a, Broker b) {
		b.sub_ids.put(a.id, a);
		b.sub_thresh.put(a.id, a);
		b.sub_load.put(a.id, a);
		b.sub_lat.put(a.id, a);
		System.out.println("Broker "+b.id+" took user with id "+a.id);
		return b;
	}
	
	
	
}
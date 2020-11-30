import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;


//Create BCS here, which manages everything
public class BCS {
	
	public static enum InitialDeployment {
		RND,
		NRB,
		RR;

		
	}
	public static enum ShuffleMethod {
		PRIO,
		SmLat,
		GreedyShuffle,
		HeavSub;

		
	}
	
	public static enum DMMethod {
		Lat,
		Load;
	}
	//-------------CONFIGURATION, DONT FORGET LOG FILE CHANGE AROUND LINE 1051 and 1054-------------------------------
	static InitialDeployment ip;
	static ShuffleMethod sm;
	static DMMethod dm;
	//min and max values for coordinates, using Germany's max coordinates and Hamburg, Berlin, Frankfurt, München and Göttingen as Broker locations.
	public static double nsrangeMin = 47;
	public static double nsrangeMax = 55;
	public static double werangeMin = 6;
	public static double werangeMax = 15;
	//load limits for DM and SH
    public static int dMloadLimit = 75;
    public static int sHloadLimit = 95;
    //load Max and Min which each subscriber can have at the start of the simulation
    public static int loadMin = 1;
    public static int loadMax = 3;
	//--------END CONFIGURATION--------DO NOT TOUCH--------LOG FILE PATH CHANGE LINE 1051 and 1054--------------------
    
    
    
  	//value provided by J.Hasenburg et. al 
  	public static double latPerKM = 0.021048134571484346;
  	
  	//random seeds used to repeat experiment results
  	public static long randomSeed;
  	public static long randomSeed2 = 0;
  	
  	public static Random rand = new Random(randomSeed);
  	public static Random r  = new Random(randomSeed2);
  	//maps used to save all brokers and subscribers
  	public static Map<String, Broker> brokerlist = new TreeMap<String,Broker>();
  	public static Map<String, Subscriber> subscriberlist = new TreeMap<String, Subscriber>();
      
    //saves load changes happening to subscribers
    public static Map<Integer, ArrayList<ArrayList<Integer>>> subscriberLoadChanges = new TreeMap<>();
      
      
      
      
      
      
      //define when the load increase/decresae can happen at the latest/earliest
      public static int loadDecreaseTurnMin = 4;
      public static int loadDecreaseTurnMax = 6;
      public static int loadIncreaseTurnMin = 1;
      public static int loadIncreaseTurnMax = 3;
      
      //defines how much the threshold should increase by if a LatDM fails
      public static int thresholdIncrease = 1;
      
      // saves total values for load and total Unhappiness
    //turn and turnlimits
    public static int turn = 0;
    public static int turnLimit = 144;
	//amount of subscribers and brokers
    public static int subscriberAmount = 100;
	public static int brokerAmount = 5;
    
	public static double[] unhappinessArray = new double[turnLimit];
	public static double[] loadArray = new double[turnLimit];
	 
	
	public static int calculateCollectiveUnhappiness(Broker b, Integer turn) {
		 int collective = 0;
		 for (Map.Entry<Integer,Subscriber> entry : b.sub_ids.entrySet()) {
				Subscriber v = entry.getValue();
				collective+= v.unhappiness;
			}
		 b.unhappiness = collective;
		 return collective;
	}
	//calculates total Latency Impact
	public static int calculateTotalUnhappiness() {
		 int collective = 0;
		 for(Map.Entry<String,Broker> entry :brokerlist.entrySet()) {
			 	for (Map.Entry<Integer,Subscriber> entry2 : entry.getValue().sub_ids.entrySet()) {
					Subscriber v = entry2.getValue();
					collective+= v.unhappiness;
				} 
		 } 
		 return collective;
	}
	
	//calculates Latency Impact, previously called Happiness.
	public static int calculateHappiness(Subscriber a, Map<String, Broker> brokerlist, Broker b) {
		double lat1 = a.nscoord;
		double lon1 = a.wecoord;
		int currentMS = distanceToLatency(distance(lat1, b.nscoord,lon1,b.wecoord));
		double dist0 = distance(lat1, brokerlist.get("b0").nscoord, lon1, brokerlist.get("b0").wecoord);
		double dist1 = distance(lat1, brokerlist.get("b1").nscoord, lon1, brokerlist.get("b1").wecoord);
		double dist2 = distance(lat1, brokerlist.get("b2").nscoord, lon1, brokerlist.get("b2").wecoord);
		double dist3 = distance(lat1, brokerlist.get("b3").nscoord, lon1, brokerlist.get("b3").wecoord);
		double dist4 = distance(lat1, brokerlist.get("b4").nscoord, lon1, brokerlist.get("b4").wecoord);
		double smallest = Math.min(dist0, Math.min(dist0, Math.min(dist1, Math.min(dist2, Math.min(dist3, dist4)))));
		int smallestLat = distanceToLatency(smallest);
		return currentMS-smallestLat;
	}
	//calculates broker with lowest load
	public static Broker calculateLowestLoad() {
		int index = 0;
		int currentLoadMin = Integer.MAX_VALUE;
		for(int i = 0; i < brokerlist.size();i++) {
			Broker b = brokerlist.get("b"+Integer.toString(i));
			if(b.load < currentLoadMin) {
				index = i; 
				currentLoadMin = b.load;
			}
		}
		return brokerlist.get("b"+Integer.toString(index));
	}
	
	//calculates total system load over all brokers
	public static int calculateTotalLoad() {
		int totalLoad = 0;
		for(int i = 0; i < brokerlist.size();i++) {
			Broker b = brokerlist.get("b"+Integer.toString(i));
			totalLoad+= b.load;
		}
		return totalLoad;
	}
	
	//calculates the nearest broker
	public static Broker calculateNearestBroker(Subscriber a, ArrayList<String> brokersToIgnore) {
		double lat1 = a.nscoord;
		double lon1 = a.wecoord;
		double[] brokerLatencies = new double[brokerlist.size()];
		Map<String, Double> brokerLats = new TreeMap<>(); 
		int count = 0;
		if(!brokersToIgnore.isEmpty()) {
			if(!brokersToIgnore.contains("b0")) {
				brokerLatencies[count] = distance(lat1, brokerlist.get("b0").nscoord, lon1, brokerlist.get("b0").wecoord);
				count++;
			}else {
				brokerLatencies[count] = Double.MAX_VALUE;
				count++;
			}
			if(!brokersToIgnore.contains("b1")) {
				brokerLatencies[count] = distance(lat1, brokerlist.get("b1").nscoord, lon1, brokerlist.get("b1").wecoord);
				count++;
			}else {
				brokerLatencies[count] = Double.MAX_VALUE;
				count++;
			}
			if(!brokersToIgnore.contains("b2")) {
				brokerLatencies[count] = distance(lat1, brokerlist.get("b2").nscoord, lon1, brokerlist.get("b2").wecoord);
				count++;
			}else {
				brokerLatencies[count] = Double.MAX_VALUE;
				count++;
			}
			if(!brokersToIgnore.contains("b3")) {
				brokerLatencies[count] = distance(lat1, brokerlist.get("b3").nscoord, lon1, brokerlist.get("b3").wecoord);
				count++;
			}else {
				brokerLatencies[count] = Double.MAX_VALUE;
				count++;
			}
			if(!brokersToIgnore.contains("b4")) {
				brokerLatencies[count] = distance(lat1, brokerlist.get("b4").nscoord, lon1, brokerlist.get("b4").wecoord);
				count++;
			}else {
				brokerLatencies[count] = Double.MAX_VALUE;
				count++;
			}
			
			
		}else {
			brokerLatencies[0] =  distance(lat1, brokerlist.get("b0").nscoord, lon1, brokerlist.get("b0").wecoord);
			brokerLatencies[1] = distance(lat1, brokerlist.get("b1").nscoord, lon1, brokerlist.get("b1").wecoord);
			brokerLatencies[2] = distance(lat1, brokerlist.get("b2").nscoord, lon1, brokerlist.get("b2").wecoord);
			brokerLatencies[3] = distance(lat1, brokerlist.get("b3").nscoord, lon1, brokerlist.get("b3").wecoord);
			brokerLatencies[4] = distance(lat1, brokerlist.get("b4").nscoord, lon1, brokerlist.get("b4").wecoord);
		}	
		double minDist = getMin(brokerLatencies);
		if(minDist == brokerLatencies[0]) {
			return brokerlist.get("b0");
		}
		if(minDist == brokerLatencies[1]) {
			return brokerlist.get("b1");
		}
		if(minDist == brokerLatencies[2]) {
			return brokerlist.get("b2");
		}
		if(minDist == brokerLatencies[3]) {
			return brokerlist.get("b3");
		}
		return brokerlist.get("b4");
	}
	//gets the smallest load broker and returns it
	public static Broker calculateSmallestLoadBroker(Subscriber currentSub, ArrayList<String> brokersToIgnore) {
		int index = 0;
		int minLoad = Integer.MAX_VALUE;
		for(int i =0;i<brokerlist.size();i++) {
			Broker broker = brokerlist.get("b"+Integer.toString(i));
			if(!brokersToIgnore.contains(broker.brokername)) {
				if(broker.load < minLoad) {
					index = i; 
					minLoad = broker.load;
				}
			}
		}
		return brokerlist.get("b"+Integer.toString(index));
	}
	//takes care of inital broker-subscriber assignment
	public static Broker initialAssignSubscriber(Subscriber a, Map<String, Broker> brokerlist, int brokerid) {
		switch(ip) {
		case RND:
			int randombroker = rand.nextInt((4 - 0) + 1) + 0;
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
			ArrayList<String> brokersToIgnore = new ArrayList<>();
			boolean brokerFound = false;
			while(!brokerFound) {
				Broker b = calculateNearestBroker(a,brokersToIgnore);
				if(b.load + a.load<dMloadLimit) {
					brokerFound = true;
					return b;
				}else {
					brokersToIgnore.add(b.brokername);
				}
				
			}
			return calculateNearestBroker(a, new ArrayList<String>());
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
	//increases data rates for the randomly chosen subscribers.
	public static void increaseDataRates() {
		int subMax = subscriberAmount;
		int count = 0;
		ArrayList<Integer> subscribersChosen = new ArrayList<>();
		while(count <= (subscriberAmount / 4)) {
			int subscriberValue = r.nextInt(subMax);
			if(!subscribersChosen.contains(subscriberValue)) {
				subscribersChosen.add(subscriberValue);
				count++;
			}
		}
		for(Integer i : subscribersChosen) {
			int turnToIncrease = turn + r.nextInt(loadIncreaseTurnMax-loadIncreaseTurnMin)+loadIncreaseTurnMin;
			int increaseBy = r.nextInt(loadMax);
			int turnToDecrease = turn + r.nextInt(loadDecreaseTurnMax-loadDecreaseTurnMin)+loadDecreaseTurnMin;
			int decreaseBy = -increaseBy;
			ArrayList<Integer> increaseTurn = new ArrayList<>();
			increaseTurn.add(i);
			increaseTurn.add(increaseBy);
			ArrayList<Integer> decreaseTurn = new ArrayList<>();
			decreaseTurn.add(i);
			decreaseTurn.add(decreaseBy);
			ArrayList<ArrayList<Integer>> currentValue = new ArrayList<>();
			if(subscriberLoadChanges.get(turnToIncrease) != null) {
				currentValue = subscriberLoadChanges.get(turnToIncrease);
				
			}
			currentValue.add(increaseTurn);
			subscriberLoadChanges.put(turnToIncrease, currentValue);
			
			currentValue = new ArrayList<>();
			if(subscriberLoadChanges.get(turnToDecrease) != null) {
				currentValue = subscriberLoadChanges.get(turnToDecrease);
				
			}
			currentValue.add(decreaseTurn);
			subscriberLoadChanges.put(turnToDecrease, currentValue);
			
			
		}
	}
	
	//Sets up subscriber assignment for main function
	public static Broker AssignSubscriber(Subscriber a, Map<String, Broker> brokerlist, int brokerid) {
		switch(ip) {
		case RND:
			int randombroker = rand.nextInt((4-0)+1)+0;
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

	//checks whether the load threshold for Dynamic Migration is reached
	public static void checkBrokerLoadDMThreshold() {
		for(int i = 0;i<brokerlist.size();i++) {
			Broker broker = brokerlist.get("b"+Integer.toString(i));
			int currentLoad = broker.calculateLoad();
			int count = 0;
			while(currentLoad > dMloadLimit) {
						if(count >= broker.sub_load.size()) {
							break;
						}
						performLoadDynamicMigration(broker, count);
						if(currentLoad == broker.calculateLoad()) {
							count++;
							performLoadDynamicMigration(broker,count);
						}
						currentLoad = broker.calculateLoad();
					
			}
		}
	}
	
	//checks whether the latency threshold for Dynamic Migration is reached
	public static void checkBrokerLatencyDMThreshold() {
		for(int i = 0;i<brokerlist.size();i++) {
			Broker broker = brokerlist.get("b"+Integer.toString(i));
			broker.calculateLatency();
			Map<Integer,Integer> sub_threshCopy = new TreeMap<>();
			sub_threshCopy.putAll(broker.sub_thresh);
			for(Map.Entry<Integer,Integer> brokerSubs: sub_threshCopy.entrySet()) {
				int subID = brokerSubs.getKey();
				int subValue = brokerSubs.getValue();
				Subscriber sub = subscriberlist.get("s"+Integer.toString(subID));
				//if Latency is y times greater than the mean latency
				if(broker.sub_lat.get(subID)> broker.mean_lat* broker.sub_thresh.get(subID)) {
					boolean status = performLatencyDynamicMigration(broker, sub);
					if(!status) {
						System.out.println("No better broker was found, increase threshold");
						broker.sub_thresh.put(subID, subValue+thresholdIncrease);
					}
				}
			}
		}
	}
	
	//checks whether the loadthreshold for shuffling is reached
	public static boolean checkBrokerLoadShuffleThreshold() {
		for(int i = 0;i<brokerlist.size();i++) {
			Broker broker = brokerlist.get("b"+Integer.toString(i));
			int currentLoad = broker.calculateLoad();
			if(currentLoad > sHloadLimit) {
				performShuffle();
				return true;
			}
		}
		return false;
	}
	//checks whether the latency threshold for shuffling is reached
	//maybe pseudoshuffle to check if shuffle creates an improvement
	public static boolean checkBrokerLatencyShuffleThreshold() {
		//checks if shuffling would create a better result than the current happiness value
		boolean shuffle = performPseudoShuffle();
		if(shuffle == true) {
			return true;
		}else {
			return false;
		}
	}
	
	
	
	
	//offload a worse latency subscriber to another broker
	public static boolean performLatencyDynamicMigration(Broker broker , Subscriber sub) {
		broker.removeSubscriber(sub.id);
		ArrayList<String> brokersToIgnore = new ArrayList<>();
		boolean assignedToBroker = false;
		while(assignedToBroker == false) {
			Broker bestBroker = calculateNearestBroker(sub, brokersToIgnore);
			if(bestBroker.load+sub.load > dMloadLimit) {
				brokersToIgnore.add(bestBroker.brokername);
				if(brokersToIgnore.size() >= brokerlist.size()) {
					Broker b = calculateLowestLoad();
					assignedToBroker = true;
					sub.unhappiness = calculateHappiness(sub, brokerlist, b);
					b.AssignSubscribertoBroker(sub, b);
					if(broker.id == b.id) {
						return false;
					}else {
						return true;
					}
				}
			}else {
				assignedToBroker = true;
				sub.unhappiness = calculateHappiness(sub, brokerlist, bestBroker);
				bestBroker.AssignSubscribertoBroker(sub, bestBroker);
				if(broker.id == bestBroker.id) {
					return false;
				}else {
					return true;
				}
			}
		}
		return true;
	}
	//performs load dynamic Migration
	public static void performLoadDynamicMigration(Broker broker, int count) {
		Subscriber sub = broker.getHeaviestSubscriber(count);
		if(sub != null) {
			broker.removeSubscriber(sub.id);
		}else {
			return;
		}
		ArrayList<String> brokersToIgnore = new ArrayList<>();
		boolean assignedToBroker = false;
		while(assignedToBroker == false) {
			Broker bestBroker;
			if(dm == DMMethod.Lat) {
				bestBroker = calculateNearestBroker(sub, brokersToIgnore);
			}
			else {
				bestBroker = calculateSmallestLoadBroker(sub,brokersToIgnore);
			}
			if((bestBroker.load+sub.load > dMloadLimit) && (bestBroker.load + sub.load >= broker.load + sub.load)) {
				brokersToIgnore.add(bestBroker.brokername);
				if(brokersToIgnore.size() >= brokerlist.size()) {
					Broker b = calculateLowestLoad();
					assignedToBroker = true;
					sub.unhappiness = calculateHappiness(sub, brokerlist, b);
					b.AssignSubscribertoBroker(sub, b);
					
				}
			}else {
				assignedToBroker = true;
				sub.unhappiness = calculateHappiness(sub, brokerlist, bestBroker);
				bestBroker.AssignSubscribertoBroker(sub, bestBroker);
				System.out.println("Subscriber load : "+sub.load);
				System.out.println("TRANSFERS HERE to "+bestBroker.brokername+" from "+ broker.brokername);
				System.out.println("New broker load: "+bestBroker.calculateLoad());
			}
		}
	}
	
	//performs shuffle according to the the current shuffle parameter
	public static void performShuffle(){
		for(int i =0;i<brokerlist.size();i++) {
			String bname = "b"+Integer.toString(i);
			brokerlist.get(bname).removeAllSubscribers();
		}
		switch(sm) {
		case HeavSub:
			Map<String, Integer> subscriberListCopy = new TreeMap<String, Integer>();
			for(int i = 0;i<subscriberlist.size();i++) {
				String subscribername = "s" + Integer.toString(i);
				subscriberListCopy.put(subscribername, subscriberlist.get(subscribername).load);
			}
			while(!subscriberListCopy.isEmpty()) {
				String csub = Collections.max(subscriberListCopy.entrySet(), Comparator.comparingInt(Map.Entry::getValue)).getKey();
				Subscriber currentSub = subscriberlist.get(csub);
				subscriberListCopy.remove(csub);
				boolean assignedToBroker = false;
				ArrayList<String> brokersToIgnore = new ArrayList<>();
				while(assignedToBroker == false) {
					Broker bestBroker = calculateNearestBroker(currentSub, brokersToIgnore);
					if(bestBroker.load+currentSub.load > dMloadLimit) {
						brokersToIgnore.add(bestBroker.brokername);
						if(brokersToIgnore.size() >= brokerlist.size()) {
							Broker b = calculateLowestLoad();
							assignedToBroker = true;
							currentSub.unhappiness = calculateHappiness(currentSub, brokerlist, b);
							b.AssignSubscribertoBroker(currentSub, b);
						}
					}else {
						assignedToBroker = true;
						currentSub.unhappiness = calculateHappiness(currentSub, brokerlist, bestBroker);
						bestBroker.AssignSubscribertoBroker(currentSub, bestBroker);
					}
				}
			}
			
			break;
			
			//PRIO: Find the subscribers where the difference between its best and second-best broker is the biggest and put these in a a list. 
		case PRIO: 
			double[] priorities = new double[subscriberlist.size()];
			for(int i = 0;i<subscriberlist.size();i++) {
				String subname = "s" +Integer.toString(i);
				Subscriber sub = subscriberlist.get(subname);
				Broker bestBroker = calculateNearestBroker(sub,new ArrayList<String>());
				ArrayList<String>bTI = new ArrayList<>();
				bTI.add(bestBroker.brokername);
				//Ignore the best broker, find the now nearest broker
				Broker secondBroker = calculateNearestBroker(sub, bTI);
				double distToBest = distance(sub.nscoord, bestBroker.nscoord, sub.wecoord, bestBroker.wecoord);
				double distToSec = distance(sub.nscoord, secondBroker.nscoord, sub.wecoord, secondBroker.wecoord);
				priorities[i] = distToSec - distToBest;
			}
			int count = 0;
			while(count < subscriberlist.size()) {
				int index = 0;
				double smallestValue = Double.MAX_VALUE;
				for(int i = 0;i<subscriberlist.size();i++) {
					if (priorities[i] < smallestValue) {
						smallestValue = priorities[i];
						index = i;
					}
				}
				//set current highest priority to max val to ignore it in next cycle
				priorities[index] = Double.MAX_VALUE;
				Subscriber currentSub = subscriberlist.get("s"+Integer.toString(index));
				boolean assignedToBroker = false;
				ArrayList<String> brokersToIgnore = new ArrayList<>();
				while(assignedToBroker == false) {
					Broker bestBroker = calculateNearestBroker(currentSub, brokersToIgnore);
					if(bestBroker.load+currentSub.load > dMloadLimit) {
						brokersToIgnore.add(bestBroker.brokername);
						if(brokersToIgnore.size() == brokerlist.size()) {
							Broker b = calculateLowestLoad();
							assignedToBroker = true;
							currentSub.unhappiness = calculateHappiness(currentSub, brokerlist, b);
							b.AssignSubscribertoBroker(currentSub, b);
						}
					}else {
						assignedToBroker = true;
						currentSub.unhappiness = calculateHappiness(currentSub, brokerlist, bestBroker);
						bestBroker.AssignSubscribertoBroker(currentSub, bestBroker);
					}
				}
				count++;
			}
			break;
		case SmLat:
			double[] distances = new double[subscriberlist.size()];
			//calculate smallest distance for each subscriber to a broker
			for(int i = 0;i<subscriberlist.size();i++) {
				Subscriber sub = subscriberlist.get("s"+ Integer.toString(i));
				Broker bestBroker = calculateNearestBroker(sub, new ArrayList<String>());
				distances[i] = distance(sub.nscoord, bestBroker.nscoord, sub.wecoord, bestBroker.wecoord);
			}
			int count2 = 0;
			while(count2 < subscriberlist.size()) {
				int index = 0;
				double smallestValue = Double.MAX_VALUE;
				for(int i = 0;i<subscriberlist.size();i++) {
					if (distances[i] < smallestValue) {
						smallestValue = distances[i];
						index = i;
					}
				}
				distances[index] = Double.MAX_VALUE;
				Subscriber currentSub = subscriberlist.get("s"+Integer.toString(index));
				boolean assignedToBroker = false;
				ArrayList<String> brokersToIgnore = new ArrayList<>();
				while(assignedToBroker == false) {
					Broker bestBroker = calculateNearestBroker(currentSub, brokersToIgnore);
					if(bestBroker.load+currentSub.load > dMloadLimit) {
						brokersToIgnore.add(bestBroker.brokername);
						if(brokersToIgnore.size() == brokerlist.size()) {
							Broker b = calculateLowestLoad();
							assignedToBroker = true;
							currentSub.unhappiness = calculateHappiness(currentSub, brokerlist, b);
							b.AssignSubscribertoBroker(currentSub, b);
						}
					}else {
						assignedToBroker = true;
						currentSub.unhappiness = calculateHappiness(currentSub, brokerlist, bestBroker);
						bestBroker.AssignSubscribertoBroker(currentSub, bestBroker);
					}
				}
				count2++;
			//now assign the subscribers with the best latencies first
			}
			break;
		case GreedyShuffle:
			System.out.println("Greedy Shuffle");
			int[] loadStates = new int[subscriberlist.size()];
			//calculate smallest distance for each subscriber to a broker
			for(int i = 0;i<subscriberlist.size();i++) {
				Subscriber sub = subscriberlist.get("s"+ Integer.toString(i));
				loadStates[i] = sub.load;
			}
			int count3 = 0;
			while(count3 < subscriberlist.size()) {
				int index = 0;
				double biggestValue = Integer.MIN_VALUE;
				for(int i = 0;i<subscriberlist.size();i++) {
					if (loadStates[i] > biggestValue) {
						biggestValue = loadStates[i];
						index = i;
					}
				}
				loadStates[index] = Integer.MIN_VALUE;
				Subscriber currentSub = subscriberlist.get("s"+Integer.toString(index));
				boolean assignedToBroker = false;
				ArrayList<String> brokersToIgnore = new ArrayList<>();
				while(assignedToBroker == false) {
					Broker bestBroker = calculateSmallestLoadBroker(currentSub, brokersToIgnore);
					if(bestBroker.load+currentSub.load > dMloadLimit) {
						brokersToIgnore.add(bestBroker.brokername);
						if(brokersToIgnore.size() == brokerlist.size()) {
							Broker b = calculateLowestLoad();
							assignedToBroker = true;
							currentSub.unhappiness = calculateHappiness(currentSub, brokerlist, b);
							b.AssignSubscribertoBroker(currentSub, b);
						}
					}else {
						assignedToBroker = true;
						currentSub.unhappiness = calculateHappiness(currentSub, brokerlist, bestBroker);
						bestBroker.AssignSubscribertoBroker(currentSub, bestBroker);
					}
				}
				count3++;
		}
		}
		System.out.println("Total Unhappiness across all brokers:: "+calculateTotalUnhappiness());
	}
	
	//Performs a theoretical shuffle to see if there's a possible improvement
	public static boolean performPseudoShuffle() {
		//implement pseudoshuffle to check if there's an improvement
		int totalUnhappiness = calculateTotalUnhappiness();
		unhappinessArray[turn] = totalUnhappiness;
		System.out.println("Current Latency Impact: "+ totalUnhappiness);
		int newUnhappiness = 0;
		Map<String, Integer> loadstates = new TreeMap<>();
		for(Map.Entry<String,Broker> entry: brokerlist.entrySet()) {
			loadstates.put(entry.getKey(), 0); 
		}
		switch(sm) {
		case HeavSub:
			Map<String, Integer> subscriberListCopy = new TreeMap<String, Integer>();
			for(int i = 0;i<subscriberlist.size();i++) {
				String subscribername = "s" + Integer.toString(i);
				subscriberListCopy.put(subscribername, subscriberlist.get(subscribername).load);
			}
			while(!subscriberListCopy.isEmpty()) {
				String csub = Collections.max(subscriberListCopy.entrySet(), Comparator.comparingInt(Map.Entry::getValue)).getKey();
				Subscriber currentSub = subscriberlist.get(csub);
				subscriberListCopy.remove(csub);
				boolean assignedToBroker = false;
				ArrayList<String> brokersToIgnore = new ArrayList<>();
				while(assignedToBroker == false) {
					Broker bestBroker = calculateNearestBroker(currentSub, brokersToIgnore);
					if(loadstates.get(bestBroker.brokername)+currentSub.load > dMloadLimit) {
						brokersToIgnore.add(bestBroker.brokername);
						if(brokersToIgnore.size() == brokerlist.size()) {
							Broker b = calculateLowestLoad();
							assignedToBroker = true;
							loadstates.put(b.brokername, loadstates.get(b.brokername)+currentSub.load);
							newUnhappiness += calculateHappiness(currentSub, brokerlist, b);
						}
					}else {
						assignedToBroker = true;
						loadstates.put(bestBroker.brokername, loadstates.get(bestBroker.brokername)+currentSub.load);
						newUnhappiness += calculateHappiness(currentSub, brokerlist, bestBroker);
						//don't actually assign the user
						//bestBroker.AssignSubscribertoBroker(currentSub, bestBroker);
					}
				}
			}
			
			break;
			
			//PRIO: Find the subscribers where the difference between its best and second-best broker is the biggest and put these in a a list. 
		case PRIO: 
			double[] priorities = new double[subscriberlist.size()];
			
			for(int i = 0;i<subscriberlist.size();i++) {
				String subname = "s" +Integer.toString(i);
				Subscriber sub = subscriberlist.get(subname);
				Broker bestBroker = calculateNearestBroker(sub,new ArrayList<String>());
				ArrayList<String>bTI = new ArrayList<>();
				bTI.add(bestBroker.brokername);
				//Ignore the best broker, find the now nearest broker
				Broker secondBroker = calculateNearestBroker(sub, bTI);
				double distToBest = distance(sub.nscoord, bestBroker.nscoord, sub.wecoord, bestBroker.wecoord);
				double distToSec = distance(sub.nscoord, secondBroker.nscoord, sub.wecoord, secondBroker.wecoord);
				priorities[i] = distToSec - distToBest;
			}
			int count = 0;
			while(count < subscriberlist.size()) {
				int index = 0;
				double smallestValue = Double.MAX_VALUE;
				for(int i = 0;i<subscriberlist.size();i++) {
					if (priorities[i] < smallestValue) {
						smallestValue = priorities[i];
						index = i;
					}
				}
				//set current highest priority to max val to ignore it in next cycle
				priorities[index] = Double.MAX_VALUE;
				Subscriber currentSub = subscriberlist.get("s"+Integer.toString(index));
				boolean assignedToBroker = false;
				ArrayList<String> brokersToIgnore = new ArrayList<>();
				while(assignedToBroker == false) {
					Broker bestBroker = calculateNearestBroker(currentSub, brokersToIgnore);
					if(loadstates.get(bestBroker.brokername)+currentSub.load > dMloadLimit) {
						brokersToIgnore.add(bestBroker.brokername);
						if(brokersToIgnore.size() == brokerlist.size()) {
							Broker b = calculateLowestLoad();
							assignedToBroker = true;
							loadstates.put(b.brokername, loadstates.get(b.brokername)+currentSub.load);
							newUnhappiness += calculateHappiness(currentSub, brokerlist, b);
						}
					}else {
						assignedToBroker = true;
						loadstates.put(bestBroker.brokername, loadstates.get(bestBroker.brokername)+currentSub.load);
						newUnhappiness += calculateHappiness(currentSub, brokerlist, bestBroker);
						//don't actually assign the user
						//bestBroker.AssignSubscribertoBroker(currentSub, bestBroker);
					}
				}
				count++;
			}
			break;
		//SmLat: Find subscribers who have lowest latency possible in the system. 
		case SmLat:
			double[] distances = new double[subscriberlist.size()];
			//calculate smallest distance for each subscriber to a broker
			for(int i = 0;i<subscriberlist.size();i++) {
				Subscriber sub = subscriberlist.get("s"+ Integer.toString(i));
				Broker bestBroker = calculateNearestBroker(sub, new ArrayList<String>());
				distances[i] = distance(sub.nscoord, bestBroker.nscoord, sub.wecoord, bestBroker.wecoord);
			}
			int count2 = 0;
			while(count2 < subscriberlist.size()) {
				int index = 0;
				double smallestValue = Double.MAX_VALUE;
				for(int i = 0;i<subscriberlist.size();i++) {
					if (distances[i] < smallestValue) {
						smallestValue = distances[i];
						index = i;
					}
				}
				distances[index] = Double.MAX_VALUE;
				Subscriber currentSub = subscriberlist.get("s"+Integer.toString(index));
				boolean assignedToBroker = false;
				ArrayList<String> brokersToIgnore = new ArrayList<>();
				while(assignedToBroker == false) {
					Broker bestBroker = calculateNearestBroker(currentSub, brokersToIgnore);
					if(loadstates.get(bestBroker.brokername)+currentSub.load > dMloadLimit) {
						brokersToIgnore.add(bestBroker.brokername);
						if(brokersToIgnore.size() == brokerlist.size()) {
							Broker b = calculateLowestLoad();
							assignedToBroker = true;
							loadstates.put(b.brokername, loadstates.get(b.brokername)+currentSub.load);
							newUnhappiness += calculateHappiness(currentSub, brokerlist, b);
						}
					}else {
						assignedToBroker = true;
						loadstates.put(bestBroker.brokername, loadstates.get(bestBroker.brokername)+currentSub.load);
						newUnhappiness += calculateHappiness(currentSub, brokerlist, bestBroker);
						//don't actually assign the user
						//bestBroker.AssignSubscribertoBroker(currentSub, bestBroker);
					}
				}
				count2++;
			}
		//Previous heuristic used by Nguyen et al. 
		case GreedyShuffle:
			int[] subLoadStates = new int[subscriberlist.size()];
			//calculate smallest distance for each subscriber to a broker
			for(int i = 0;i<subscriberlist.size();i++) {
				Subscriber sub = subscriberlist.get("s"+ Integer.toString(i));
				subLoadStates[i] = 0+sub.load;
			}
			int count3 = 0;
			while(count3 < subscriberlist.size()) {
				int index = 0;
				double biggestValue = Integer.MIN_VALUE;
				for(int i = 0;i<subscriberlist.size();i++) {
					if (subLoadStates[i] > biggestValue) {
						biggestValue = subLoadStates[i];
						index = i;
					}
				}
				subLoadStates[index] = Integer.MIN_VALUE;
				Subscriber currentSub = subscriberlist.get("s"+Integer.toString(index));
				boolean assignedToBroker = false;
				ArrayList<String> brokersToIgnore = new ArrayList<>();
				while(assignedToBroker == false) {
					Broker bestBroker = calculateSmallestLoadBroker(currentSub, brokersToIgnore);
					if(loadstates.get(bestBroker.brokername)+currentSub.load > dMloadLimit) {
						brokersToIgnore.add(bestBroker.brokername);
						if(brokersToIgnore.size() == brokerlist.size()) {
							Broker b = calculateLowestLoad();
							assignedToBroker = true;
							loadstates.put(b.brokername, loadstates.get(b.brokername)+currentSub.load);
							newUnhappiness += calculateHappiness(currentSub, brokerlist, b);
						}
					}else {
						assignedToBroker = true;
						loadstates.put(bestBroker.brokername, loadstates.get(bestBroker.brokername)+currentSub.load);
						newUnhappiness += calculateHappiness(currentSub, brokerlist, bestBroker);
						//don't actually assign the user
						//bestBroker.AssignSubscribertoBroker(currentSub, bestBroker);

					}
				}
				count3++;
		}
			default:
				break;
			
		}
		//if shuffle is better than current configuration by 20%
		System.out.println("New theoretical Latency Impact: "+ newUnhappiness);
		if(totalUnhappiness > newUnhappiness) {
			System.out.println("Shuffle will be performed");
			performShuffle();
			
			return true;
		}
		System.out.println("Shuffle will not be performed");
		return false;
	}
	//This is a Java implementation of the Haversine method to calculate the distance between two coordinates 
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
	    return Math.sqrt(distance)/1000;
	}
	public static int distanceToLatency(double dist) {
		return ((int) Math.round(dist*latPerKM));
	}
	
	public static Double getMin(double[] latencies) {
		Double minValue = Double.MAX_VALUE;
		for(Double value: latencies){
			if(value <minValue) {
				minValue = value;
			}
		}
		return minValue;
	}
	
	
	
	//performs subscriber actions(movement)
	public static void performActions() {
		for(int i = 0;i<subscriberlist.size();i++) {
			String subscribername = "s" + Integer.toString(i);
			subscriberlist.get(subscribername).performAction();
		}
	}
	//check all defined thresholds for transfer possibilities
	public static void checkThresholds() {
		boolean shuffleHappened = false;
		shuffleHappened = checkBrokerLatencyShuffleThreshold();
		if(shuffleHappened) {
			return;
		}
		shuffleHappened = checkBrokerLoadShuffleThreshold();
		if(shuffleHappened) {
			return;
		}
		checkBrokerLoadDMThreshold();
		//If Latency is used as factor in DM
		if(dm == DMMethod.Lat) {
			checkBrokerLatencyDMThreshold();
		}
	}
	
	//Prints various information about the broker state and total system load
	public static void printStats() {
		System.out.println("B0 Amount of subscribers on Broker :"+brokerlist.get("b0").sub_ids.size()+", Collective Latency Impact : "+calculateCollectiveUnhappiness(brokerlist.get("b0"), turn)+", Collective Load : "+brokerlist.get("b0").load);
		System.out.println("B1 Amount of subscribers on Broker :"+brokerlist.get("b1").sub_ids.size()+", Collective Latency Impact : "+calculateCollectiveUnhappiness(brokerlist.get("b1"), turn)+", Collective Load : "+brokerlist.get("b1").load);
		System.out.println("B2 Amount of subscribers on Broker :"+brokerlist.get("b2").sub_ids.size()+", Collective Latency Impact : "+calculateCollectiveUnhappiness(brokerlist.get("b2"), turn)+", Collective Load : "+brokerlist.get("b2").load);
		System.out.println("B3 Amount of subscribers on Broker :"+brokerlist.get("b3").sub_ids.size()+", Collective Latency Impact : "+calculateCollectiveUnhappiness(brokerlist.get("b3"), turn)+", Collective Load : "+brokerlist.get("b3").load);
		System.out.println("B4 Amount of subscribers on Broker :"+brokerlist.get("b4").sub_ids.size()+", Collective Latency Impact : "+calculateCollectiveUnhappiness(brokerlist.get("b4"), turn)+", Collective Load : "+brokerlist.get("b4").load);
		System.out.println("Total system load: " + calculateTotalLoad());
		SaveGraphData();
	}
	//Evaluate graph data with python, prob. seaborn is best
	public static void SaveGraphData() {
		for(int i = 0; i<brokerlist.size();i++) {
			String brokername = "b"+ Integer.toString(i);
			Broker broker = brokerlist.get(brokername);
			broker.unhappinessArray[turn] = broker.unhappiness;
			broker.loadArray[turn] = broker.load;
		if(i == 0) {
			loadArray[turn] = broker.load;
		}else {
			loadArray[turn]+= broker.load;
		}
		}
		
	}
	//used to update load states of subscribers
	public static void updateLoadStates() {
		if(subscriberLoadChanges.get(turn) != null) {
			for(ArrayList<Integer> updateAction : subscriberLoadChanges.get(turn)) {
				Subscriber sub = subscriberlist.get("s"+ Integer.toString(updateAction.get(0)));
				sub.load += updateAction.get(1);
				updateBroker(sub.id, sub.load, sub.nscoord, sub.wecoord);
			}
		}	
	}
	//updates broker values, so it can track subscriber activity
	public static void updateBroker(int id, int load, double nscoord, double wecoord) {
		double [] coords = new double[2];
		coords[0] = nscoord;
		coords[1] = wecoord;
		for(int i = 0; i< brokerlist.size();i++) {
			Broker broker = brokerlist.get("b"+Integer.toString(i));
			if(broker.sub_load.get(id) != null){
				broker.sub_load.put(id, load);
				broker.calculateLoad();
			}
			if(broker.sub_loc.get(id) != null) {
				broker.sub_loc.put(id, coords);
				broker.sub_lat.put(id, (int)Math.round(distance(nscoord, broker.nscoord, wecoord, broker.wecoord)));
				broker.calculateLatency();
				Subscriber s = subscriberlist.get("s"+Integer.toString(id));
				s.unhappiness = calculateHappiness(s, brokerlist, broker);
			}
			
		}
	}
	//Writes data to arrays, to be exported to csv later
	public static void writeData(String filePath, int id) 
	{ 
	  
	    // first create file object for file placed at location 
	    // specified by filepath 
	    File file = new File(filePath); 
	    Broker broker = brokerlist.get("b"+Integer.toString(id));
	    
	    try { 
	        // create FileWriter object with file as parameter 
	    	FileWriter outputfile = new FileWriter(file); 
	    	outputfile.write("Turn,Happiness,Load\n");
	    	if(id >=0) {
		        for(int i = 0;i<broker.unhappinessArray.length;i++) {
		        	outputfile.write(Integer.toString(i)+","+broker.unhappinessArray[i]+","+broker.loadArray[i]+"\n");
		        }
		        
	    	}else {
	    		for(int i = 0;i<unhappinessArray.length;i++) {
	    			outputfile.write(Integer.toString(i)+","+unhappinessArray[i]+","+loadArray[i]+"\n");
	    		}
	    	}
	    	outputfile.close();
	    } 
	    catch (IOException e) { 
	        // TODO Auto-generated catch block 
	        e.printStackTrace(); 
	    } 
	} 
	//writes data to csv files 
	public static void dataToCSV(long randomSeed) {
		

		for(int i = 0; i<brokerlist.size();i++) {
			String filePath = "C:\\Users\\Tim\\eclipse-workspace\\PubSub_Latency\\Logs\\FINALLOW"+ip+"_"+dm+"_"+sm+"Broker"+ Integer.toString(i)+"_Seed_"+Integer.toString((int)randomSeed)+".csv";
			writeData(filePath, i);
		}
		String filePath = "C:\\Users\\Tim\\eclipse-workspace\\PubSub_Latency\\Logs\\FINALLOW"+ip+"_"+dm+"_"+sm+"Brokers Total_Seed_"+Integer.toString((int)randomSeed)+".csv";
		writeData(filePath,-1);
	}
	//clears data for next simulation run
	public static void clearData() {
		unhappinessArray = new double[turnLimit];
		loadArray = new double[turnLimit];
		for(int i =0;i<5;i++) {
			Broker broker = brokerlist.get("b"+Integer.toString(i));
			broker.unhappinessArray = new int[turnLimit];
			broker.loadArray = new int[turnLimit];
			broker.removeAllSubscribers();
		}
		subscriberlist.clear();
		brokerlist.clear();
		subscriberLoadChanges.clear();
		
	}
	//main function, runs all simulations with given parameters defined before.
	public static void main(String[] args) {
        System.out.println("System starting...");
        double randomNSCoord;
        double randomWECoord;
        int randomLoad;
        
        //Create all Subscribers with variable locations
        for(InitialDeployment ip2 : InitialDeployment.values()) {
        	ip = ip2;
        	for(ShuffleMethod sh : ShuffleMethod.values()){
        		sm = sh;
        		for(DMMethod dm2: DMMethod.values()) {
        			dm = dm2;
        
			        for(int p = 1;p<101;p++) {
			        	int brokerid = 0;
			        	brokerlist.put("b0", new Broker(51.54,9.93,0));
			            brokerlist.put("b1", new Broker(52.51,13.40,1));
			            brokerlist.put("b2", new Broker(53.06,8.83,2));
			            brokerlist.put("b3", new Broker(50.11,8.71,3));
			            brokerlist.put("b4", new Broker(48.16,11.60,4));
			        	long randomSeed =p;
			        	Random rand = new Random(randomSeed);
				        for(int i =0; i<subscriberAmount;i++) {
				        	randomNSCoord = nsrangeMin + (nsrangeMax - nsrangeMin) * rand.nextDouble();
				        	randomWECoord = werangeMin + (werangeMax - werangeMin) * rand.nextDouble(); 	
				        	randomLoad = rand.nextInt(loadMax-loadMin) + loadMin;
				        	System.out.println(randomLoad);
				        	String subname = "s"+Integer.toString(i);
				        	subscriberlist.put(subname,new Subscriber (randomNSCoord,randomWECoord,i,randomLoad));
				        	Subscriber a = subscriberlist.get(subname);
				        	
				        	Broker b  = initialAssignSubscriber(a, brokerlist, brokerid);
				        	
				        	brokerid+=1;
				        	if(brokerid == 5) {
				        		brokerid =0;
				        	}
				        	a.unhappiness = calculateHappiness(a, brokerlist, b);
				        	System.out.println(a.unhappiness);
				        	b = b.AssignSubscribertoBroker(a, b);
				        }
				        printStats();
				        checkThresholds();
				        printStats();
				        
				        System.out.println("Starting Simulation...");
				        while(turn < turnLimit) {
				        	performActions();
				        	updateLoadStates();
				        	System.out.println("Turn : "+ turn);
				        	//every turn except first
				        	//increase load of 1/4 of subscribers, which are decreased to their original value 4-6 turns later.
				        	if(turn > 0 && turn < turnLimit - loadDecreaseTurnMax) {
				        		increaseDataRates();
				        	}
				        	checkThresholds();
				        	printStats();     	
				        	try {
								Thread.sleep(2);
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
				        	turn++;
				        }
				        dataToCSV(randomSeed);
				        clearData();
				        turn = 0;
			        }
        		}
        	}
        }
	}
	
	
	
}

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
		HeavSub;

		
	}
	//--------------------------------------------
	static InitialDeployment ip = InitialDeployment.RR;
	static ShuffleMethod sm = ShuffleMethod.HeavSub;
	public static double nsrangeMin = -90;
	public static double nsrangeMax = 90;
	public static double werangeMin = -180;
	public static double werangeMax = 180;
	public static int subscriberAmount = 100;
	public static int brokerAmount = 5;
	public static double latencyShuffleThreshold;
	public static double loadShuffleThreshold;
	public static double latPerKM = 0.021048134571484346;
	public static long randomSeed = 12;
	public static long randomSeed2 = 0;
	public static Random rand = new Random(randomSeed);
	public static Map<String, Broker> brokerlist = new TreeMap<String,Broker>();
    public static Map<String, Subscriber> subscriberlist = new TreeMap<String, Subscriber>();
    public static Map<Integer, ArrayList<ArrayList<Integer>>> subscriberLoadChanges = new TreeMap<>();
    public static int dMloadLimit = 75;
    public static int sHloadLimit = 95;
    public static int totalLoad = 350;
    public static int loadMin = 1;
    public static int loadMax = 5;
    public static int turn = 0;
    public static int turnLimit = 144;
    public static int loadDecreaseTurnMin = 4;
    public static int loadDecreaseTurnMax = 6;
    public static int loadIncreaseTurnMin = 1;
    public static int loadIncreaseTurnMax = 3;
    public static Random r  = new Random(randomSeed2);
	//--------------------------------------------
	
	//calculate "happiness" according to latency diff to optimal broker
    //TODO: Rename function and value, unhappiness too unprofessional, bzw. im readme-Datei
	public static int calculateCollectiveUnhappiness(Broker b, Integer turn) {
		 int collective = 0;
		 for (Map.Entry<Integer,Subscriber> entry : b.sub_ids.entrySet()) {
				Subscriber v = entry.getValue();
				collective+= v.unhappiness;
			}
		 b.unhappiness = collective;
		 return collective;
	}
	
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
	public static int calculateTotalLoad() {
		int totalLoad = 0;
		for(int i = 0; i < brokerlist.size();i++) {
			Broker b = brokerlist.get("b"+Integer.toString(i));
			totalLoad+= b.load;
		}
		return totalLoad;
	}
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
			int turnToIncrease = turn + r.nextInt(loadIncreaseTurnMax)+loadIncreaseTurnMin;
			int increaseBy = r.nextInt(loadMax);
			int turnToDecrease = turn + r.nextInt(loadDecreaseTurnMax)+(loadDecreaseTurnMax-loadDecreaseTurnMin);
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
			
			//calculate turn when it will go up
		}
	}
	//TODO
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
	//checks whether the latency threshold for shuffling is reached
	public static void checkBrokerLoadShuffleThreshold() {
		for(int i = 0;i<brokerlist.size();i++) {
			Broker broker = brokerlist.get("b"+Integer.toString(i));
			int currentLoad = broker.calculateLoad();
			if(currentLoad > sHloadLimit) {
				performShuffle();
				break;
			}
		}
	}
	//checks whether the latency threshold for shuffling is reached
	//maybe pseudoshuffle to check if shuffle creates an improvement
	public static void checkBrokerLatencyShuffleThreshold() {
		//checks if shuffling would create a better result than the current happiness value
		performPseudoShuffle();
	}
	
	
	
	
	//offload a worse latency subscriber to another broker
	public static void performLatencyDynamicMigration(Integer id) {
		Broker broker = brokerlist.get("b"+Integer.toString(id));
		Subscriber sub = broker.getFarthestSubscriber();
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
				}
			}else {
				assignedToBroker = true;
				sub.unhappiness = calculateHappiness(sub, brokerlist, bestBroker);
				bestBroker.AssignSubscribertoBroker(sub, bestBroker);
			}
		}
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
			Broker bestBroker = calculateNearestBroker(sub, brokersToIgnore);
			if(bestBroker.load+sub.load > dMloadLimit) {
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
			
		}
	}
	public static void performPseudoShuffle() {
		//implement pseudoshuffle to check if there's an improvement
		int totalUnhappiness = calculateTotalUnhappiness();
		System.out.println("Current Happiness: "+ totalUnhappiness);
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
			default:
				break;
			
		}
		//if shuffle is better than current configuration by 20%
		System.out.println("New theoretical Happiness: "+ newUnhappiness);
		if(totalUnhappiness > newUnhappiness) {
			System.out.println("Shuffle will be performed");
			performShuffle();
			
			return;
		}
		System.out.println("Shuffle will not be performed");
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
	
	
	
	
	public static void performActions() {
		for(int i = 0;i<subscriberlist.size();i++) {
			String subscribername = "s" + Integer.toString(i);
			subscriberlist.get(subscribername).performAction();
		}
	}
	public static void checkThresholds() {
		checkBrokerLatencyShuffleThreshold();
		checkBrokerLoadShuffleThreshold();
		checkBrokerLoadDMThreshold();
	}
	
	
	public static void printStats() {
		System.out.println("B0 Amount of subscribers on Broker :"+brokerlist.get("b0").sub_ids.size()+", Collective Unhappiness : "+calculateCollectiveUnhappiness(brokerlist.get("b0"), turn)+", Collective Load : "+brokerlist.get("b0").load);
		System.out.println("B1 Amount of subscribers on Broker :"+brokerlist.get("b1").sub_ids.size()+", Collective Unhappiness : "+calculateCollectiveUnhappiness(brokerlist.get("b1"), turn)+", Collective Load : "+brokerlist.get("b1").load);
		System.out.println("B2 Amount of subscribers on Broker :"+brokerlist.get("b2").sub_ids.size()+", Collective Unhappiness : "+calculateCollectiveUnhappiness(brokerlist.get("b2"), turn)+", Collective Load : "+brokerlist.get("b2").load);
		System.out.println("B3 Amount of subscribers on Broker :"+brokerlist.get("b3").sub_ids.size()+", Collective Unhappiness : "+calculateCollectiveUnhappiness(brokerlist.get("b3"), turn)+", Collective Load : "+brokerlist.get("b3").load);
		System.out.println("B4 Amount of subscribers on Broker :"+brokerlist.get("b4").sub_ids.size()+", Collective Unhappiness : "+calculateCollectiveUnhappiness(brokerlist.get("b4"), turn)+", Collective Load : "+brokerlist.get("b4").load);
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
		}
	}
	
	public static void UpdateLoadStates() {
		ArrayList<ArrayList<Integer>> updates = new ArrayList<>();
		if(subscriberLoadChanges.get(turn) != null) {
			for(ArrayList<Integer> updateAction : subscriberLoadChanges.get(turn)) {
				Subscriber sub = subscriberlist.get("s"+ Integer.toString(updateAction.get(0)));
				sub.load += updateAction.get(1);
				updateBroker(sub.id, sub.load);
			}
		}
		
	}
	public static void updateBroker(int id, int load) {
		for(int i = 0; i< brokerlist.size();i++) {
			Broker broker = brokerlist.get("b"+Integer.toString(i));
			if(broker.sub_load.get(id) != null){
				broker.sub_load.put(id, load);
				broker.calculateLoad();
			}
		}
	}
	public static void main(String[] args) {
        System.out.println("System starting...");
        double randomNSCoord;
        double randomWECoord;
        int randomLoad;
        brokerlist.put("b0", new Broker(0,0,0));
        brokerlist.put("b1", new Broker(45,45,1));
        brokerlist.put("b2", new Broker(45,-45,2));
        brokerlist.put("b3", new Broker(-45,45,3));
        brokerlist.put("b4", new Broker(-45,-45,4));
        //Create all Subscribers with variable locations
        int brokerid = 0;
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
        	UpdateLoadStates();
        	performActions();
        	System.out.println("Turn : "+ turn);
        	//every turn except first
        	//increase load of 1/4 of subscribers, which are decreased to their original value 4-6 turns later.
        	if(turn > 0) {
        		increaseDataRates();
        	}
        	checkThresholds();
        	printStats();     	
        	try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        	turn++;
        }
        for(Map.Entry<Integer, ArrayList<ArrayList<Integer>>> entry: subscriberLoadChanges.entrySet()) {
        	System.out.println(entry.getKey());
        	System.out.println(entry.getValue());
        }
        
	}
	
	
	
}

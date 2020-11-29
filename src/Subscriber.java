import java.util.ArrayList;
import java.util.Random;

public class Subscriber {
	//CONFIGURATION VALUES
	String[] actionPossibilities = {"Move", "Move", "Move", "Stand", "Stand", "Stand", "Stand", "Stand", "Stand", "Stand"};
	//END
	public static long randomSeed = 12;
	public static Random rand = new Random(randomSeed);
	
	
	int actionAmount = BCS.turnLimit;
	
	double distlimit = 0.20;
	//double threshold= 80;
	int[] multiply = {0,1};
	double nscoord;
	double wecoord;
	int id;
	int load;
	//Unhappiness refers to latency impact
	int unhappiness;
	int factor;
	ArrayList<ArrayList<Object>> actions;

	//creates the actions that the subscriber will take 
	public ArrayList<ArrayList<Object>> generateActions(){
		ArrayList<ArrayList<Object>> actionArray = new ArrayList<ArrayList<Object>>();
		for(int i = 0; i<actionAmount;i++) {
			int rnd = rand.nextInt(actionPossibilities.length);
			switch(actionPossibilities[rnd]) {
			case "Move":
				//create random north-south distance
				double randomdist = 0+distlimit*rand.nextDouble();
				randomdist *= multiply[rand.nextInt(multiply.length)];
				double distlim2 = distlimit - Math.abs(randomdist);
				double randomdist2 = 0+distlim2*rand.nextDouble();
				randomdist2 *= multiply[rand.nextInt(multiply.length)];
				ArrayList<Object> action = new ArrayList<Object>();
				action.add(actionPossibilities[rnd]);
				action.add(randomdist);
				action.add(randomdist2);
				actionArray.add(action);
				break;
			default:
				ArrayList<Object> action2 = new ArrayList<Object>();
				action2.add(actionPossibilities[rnd]);
				action2.add(0.0);
				action2.add(0.0);
				actionArray.add(action2);
			}

		}
		return actionArray;
	}
	
	//implement load change during course of execution
	public void performAction () {
		for(int i = 0; i<actionAmount;i++) {
			String action = String.valueOf(this.actions.get(i).get(0));
			switch(action) {
			case "Move":
				this.nscoord += Double.parseDouble((this.actions.get(i).get(1).toString())); 
				this.wecoord += Double.parseDouble((this.actions.get(i).get(2).toString())); 
				break;
			default:
				break;
			}
		}
	}
	
	//Constructor for Subscriber
	public Subscriber(double cor1, double cor2, int identifier, int load){
		this.nscoord = cor1;
		this.wecoord = cor2;
		this.id = identifier;
		this.load = load;
		this.actions = generateActions();
		//System.out.println("Subscriber with id " + this.id + " created"+", Coordinates are "+this.nscoord +", "+ this.wecoord+", Load is "+this.load);
		//System.out.println("Subscriber id "+this.id+ " has the following actions"+ this.actions);
	}
}

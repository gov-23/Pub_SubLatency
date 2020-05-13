import java.util.ArrayList;
import java.util.Random;

public class Subscriber {
	
	int actionAmount = 10;
	String[] actionPossibilities = {"Move", "Move", "Move", "Stand", "Stand", "Stand", "Stand", "Stand", "Stand", "Stand"};
	double distlimit = 0.20;
	
	int[] multiply = {0,1};
	double nscoord;
	double wecoord;
	int id;
	int load;
	ArrayList<ArrayList<Object>> actions;
	//creates the actions that the subscriber will take 
	
	
	public ArrayList<ArrayList<Object>> generateActions(){
		ArrayList<ArrayList<Object>> actionArray = new ArrayList<ArrayList<Object>>();
		for(int i = 0; i<actionAmount;i++) {
			int rnd = new Random().nextInt(actionPossibilities.length);
			switch(actionPossibilities[rnd]) {
			case "Move":
				Random r = new Random();
				//create random north-south distance
				double randomdist = 0+distlimit*r.nextDouble();
				randomdist *= multiply[new Random().nextInt(multiply.length)];
				double distlim2 = distlimit - Math.abs(randomdist);
				double randomdist2 = 0+distlim2*r.nextDouble();
				randomdist2 *= multiply[new Random().nextInt(multiply.length)];
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
	
	
	public Subscriber performAction (Subscriber a) {
		for(int i = 0; i<actionAmount;i++) {
			String action = String.valueOf(a.actions.get(0).get(0));
			switch(action) {
			case "Move":
				break;
			default:
				break;
			}
		}
		return a;
	}
	
	//Constructor for Subscriber
	public Subscriber(double cor1, double cor2, int identifier, int load){
		this.nscoord = cor1;
		this.wecoord = cor2;
		this.id = identifier;
		this.load = load;
		this.actions = generateActions();
		System.out.println("Subscriber with id " + this.id + " created"+", Coordinates are "+this.nscoord +", "+ this.wecoord+", Load is "+this.load);
		System.out.println("Subscriber id "+this.id+ " has the following actions"+ this.actions);
	}
}

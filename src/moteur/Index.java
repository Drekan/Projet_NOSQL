package moteur;

import java.util.ArrayList;
import java.util.HashMap;

public class Index {
	//TODO statistiques, arborescence

	private String type; //Type de l'index : spo, sop, pso, pos, osp, ops
	private HashMap<Integer, HashMap<Integer, ArrayList<Integer>>> index;

	public Index(String t) {
		this.type = t;
		this.index = new HashMap<Integer, HashMap<Integer, ArrayList<Integer>>>();
	}



	public void addRec(int i1, int i2, int i3) {
		HashMap<Integer, ArrayList<Integer>> sousTuple = new HashMap<Integer, ArrayList<Integer>>();
		ArrayList<Integer> values = new ArrayList<Integer>();

		if(this.index.containsKey(i1)) {
			if(this.index.get(i1).containsKey(i2)) {
				values.addAll(this.index.get(i1).get(i2));
			}
			values.add(i3);
			sousTuple.put(i2, values);
			sousTuple.putAll(this.index.get(i1));
			this.index.replace(i1, sousTuple);
		}
		else {
			values.add(i3);
			sousTuple.put(i2, values);
			this.index.put(i1, sousTuple);
		}
	}


	public void add(int s,int p,int o) {

		switch(this.type) {
		case "spo":
			addRec(s,p,o);
			break;

		case "sop":
			addRec(s,o,p);
			break;

		case "pso":
			addRec(p,s,o);
			break;

		case "pos":
			addRec(p,o,s);
			break;

		case "osp":
			addRec(o,s,p);
			break;

		case "ops":
			addRec(o,p,s);
			break;
		}
	}

	public HashMap<Integer, HashMap<Integer, ArrayList<Integer>>> getIndex() {
		return index;
	}

	public String getType(){return this.type;}

	public int size() {
		return this.index.size();
	}
}

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
		if(!this.index.containsKey(i1)){
			this.index.put(i1,new HashMap<>());
		}
		
		if(!this.index.get(i1).containsKey(i2)){
			this.index.get(i1).put(i2,new ArrayList<>());
		}
		
		this.index.get(i1).get(i2).add(i3);
		
	
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

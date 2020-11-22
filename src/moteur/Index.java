package moteur;

import java.util.ArrayList;
import java.util.HashMap;

public class Index {
	//TODO statistiques, arborescence

	private String type; //Type de l'index : spo, sop, pso, pos, osp, ops
	//TODO -> faire aussi sp, so, os, op, po, ps, o, p, s

	private HashMap<Integer, HashMap<Integer, ArrayList<Integer>>> index;
	//sp, so, os, op, po, ps,
	private HashMap<Integer, HashMap<Integer, Integer>> index2;
	//o, p, s
	private HashMap<Integer, Integer> index1;

	private Integer valuesNumber; //TODO: à comparer avec la taille du dictionnaire

	public Index(String t) {
		this.type = t;
		this.index = new HashMap<Integer, HashMap<Integer, ArrayList<Integer>>>();
		this.index1 = new HashMap<>();
		this.index2 = new HashMap<>();

		this.valuesNumber = 0;
	}

	public HashMap<Integer, HashMap<Integer, Integer>> getIndex2(){
		return index2;
	}

	public HashMap<Integer, Integer> getIndex1(){
		return index1;
	}

	public void addRec(int i1, int i2, int i3) {
		if(!this.index.containsKey(i1)){
			this.index.put(i1,new HashMap<>());
		}

		if(!this.index.get(i1).containsKey(i2)){
			this.index.get(i1).put(i2,new ArrayList<>());
		}

		this.index.get(i1).get(i2).add(i3);

		if(!this.index2.containsKey(i1)){
			this.index2.put(i1,new HashMap<>());
		}

		if(!this.index2.get(i1).containsKey(i2)){
			this.index2.get(i1).put(i2,0);
		}
		this.index2.get(i1).put(i2, this.index2.get(i1).get(i2)+1);

		if(!this.index1.containsKey(i1)){
			this.index1.put(i1,0);
		}
		this.index1.put(i1, this.index1.get(i1)+1);

	}


	public void add(int s,int p,int o) {
		this.valuesNumber++;
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

	public Integer getValuesNumber(){return this.valuesNumber;}

	//TODO: utile ?
	public int size() {
		return this.index.size();
	}

	public int getvaluesNumber(){
		return this.valuesNumber;
	}

	public void displayNTriples(int n) { //display the first n triples
		int countdown = n;
		for(int k1 : this.index.keySet()) {
			if(countdown == 0) break;

			for(int k2 : this.index.get(k1).keySet()) {
				if(countdown == 0) break;

				for(int k3 : this.index.get(k1).get(k2)) {
					if(countdown == 0) break;

					System.out.println("["+k1+","+k2+","+k3+"]");
					countdown--;
				}

			}
		}
	}
}

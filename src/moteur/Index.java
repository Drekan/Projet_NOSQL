package moteur;

import java.util.ArrayList;

public class Index {
	private String type; //Type de l'index : spo, sop, pso, pos, osp, ops
	private ArrayList<int[]> index;

	public Index(String t) {
		this.type = t;
		this.index = new ArrayList<>();
	}

	public void add(int s,int p,int o) {
		int[] triple = {s,p,o};

		switch(this.type) {
		case "sop":
			triple	= new int[]{s,o,p};
			break;

		case "pso":
			triple	= new int[]{p,s,o};
			break;

		case "pos":
			triple	= new int[]{p,o,s}; 
			break;

		case "osp":
			triple	= new int[]{o,s,p};
			break;

		case "ops":
			triple	= new int[]{o,p,s};
			break;
		}
		this.index.add(triple);	
	}

	public int[] getTriple(int i) {
		return this.index.get(i);
	}

	public int size() {
		return this.index.size();
	}
}

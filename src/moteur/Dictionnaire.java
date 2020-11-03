package moteur;
import java.util.ArrayList;
import java.util.HashMap;

public class Dictionnaire {
	
	private ArrayList<String[]> tuples;
	private ArrayList<String> sortedRessources;
	private HashMap<Integer,String> intToString_dico;
	private HashMap<String,Integer> stringToInt_dico;
	//private HashMap<Integer,String> dictionary;
	
	
	public Dictionnaire() {
		this.intToString_dico = new HashMap<Integer,String>();
		this.stringToInt_dico = new HashMap<String,Integer>();
		this.sortedRessources = new ArrayList<>();
		this.tuples = new ArrayList<>();
		
	}
	
	
	public void add(String s,String p,String o) {
		
		this.tuples.add(new String[] {s,p,o});
		
		
		//TODO factoriser le code ci-dessous
		if(!sortedRessources.contains(s)) {
			this.sortedRessources.add(s);	
		}
		
		if(!sortedRessources.contains(p)) {
			this.sortedRessources.add(p);	
		}
		
		if(!sortedRessources.contains(o)) {
			this.sortedRessources.add(o);	
		}
		
	}
	
	public void createDico() {
		this.sortedRessources.sort(null);
		/* 
		 *  livesIn 50
		 *  livesIn 51
		 *  Paris 52
		 *  ...
		 *  <50,livesIn>
		 *  <52,Paris>
		 *  
		 */
		for(int i=0; i<this.sortedRessources.size();i++) {
			this.intToString_dico.put(i,this.sortedRessources.get(i));
			this.stringToInt_dico.put(this.sortedRessources.get(i),i);
		}
		
		this.sortedRessources.clear();
	}
	
	public int getSize() {
		return this.intToString_dico.size();
	}
	
	public String getValue(int index) {
		return this.intToString_dico.get(index);
	}
	
	public int getValue(String value) {
		return this.stringToInt_dico.get(value);
	}
	
	public String[] getTuple(int i) {
		return this.tuples.get(i);
	}
	
	public ArrayList<String[]> getTuples(){
		return this.tuples;
	}

}

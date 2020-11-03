package moteur;
import java.util.ArrayList;
import java.util.HashMap;

public class Dictionnaire {

	private ArrayList<String[]> tuples; //Données non triées sous forme de tuples pour créer les index
	private ArrayList<String> sortedRessources; //Données triées lexicographiquement
	private HashMap<Integer,String> intToString_dico;
	private HashMap<String,Integer> stringToInt_dico;


	public Dictionnaire() {
		this.intToString_dico = new HashMap<Integer,String>();
		this.stringToInt_dico = new HashMap<String,Integer>();
		this.sortedRessources = new ArrayList<>();
		this.tuples = new ArrayList<>();
	}


	public void addDistinct(String s) {
		if(!sortedRessources.contains(s)) {
			this.sortedRessources.add(s);	
		}
	}

	//Ajoute des données dans les ArrayList tuples et sortedRessources
	public void add(String s,String p,String o) {
		this.tuples.add(new String[] {s,p,o});

		addDistinct(s);
		addDistinct(p);
		addDistinct(o);
	}

	//Remplissage des deux hashMap à partir des données triées
	//Le tri sert à conserver l'ordre lexicographique
	public void createDico() {
		this.sortedRessources.sort(null);

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

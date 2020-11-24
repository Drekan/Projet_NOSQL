package moteur;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;

import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.Rio;

import parsers.RDFRawParser.RDFListener;

public class DataStructure {
	private Dictionnaire dico;
	private HashMap<String,Index> indexes;
	private Options opt;

	public DataStructure(Options options) {
		this.opt = options;
		this.indexes = new HashMap<>();
	}

	public Dictionnaire getDico() {
		return dico;
	}

	public HashMap<String,Index> getIndexes() {
		return indexes;
	}

	public Options getOpt() {
		return opt;
	}

	public void createDico(Boolean triLexicographique, Statistics stats){
		long startTime_d = System.nanoTime();

		this.dico = new Dictionnaire(triLexicographique);

		String verbose = "";

		RDFParser rdfParser = Rio.createParser(RDFFormat.RDFXML);
		RDFListener rdfListener = new RDFListener(this.dico);
		rdfParser.setRDFHandler(rdfListener);

		try {
			verbose+="Parsing des données...\n";
			rdfParser.parse(new FileReader(opt.getDataPath()),"");
			this.dico.createDico();

			stats.setRDFTripleNum(dico.getTuples().size());

			verbose+="La taille du dictionnaire est de " + dico.getSize()+"\n";
			verbose+="Nombre total de ressources lues : " + rdfListener.ressourcesNum+"\n";

			if(opt.getVerbose()) {
				System.out.println(verbose);
			}

		} catch (Exception e) {

		}
		long timeSpent_d = System.nanoTime() - startTime_d;
		stats.setDicCreationTime((int)timeSpent_d/1000000);
	}

	public void createIndexes(Statistics stats) { //TODO: verbose
		long startTime_i = System.nanoTime();

		String verbose = "";

		this.indexes.put("spo",new Index("spo"));
		this.indexes.put("sop",new Index("sop"));
		this.indexes.put("pos",new Index("pos"));
		this.indexes.put("pso",new Index("pso"));
		this.indexes.put("osp",new Index("osp"));
		this.indexes.put("ops",new Index("ops"));

		ArrayList<String[]> tuples = this.dico.getTuples();

		verbose+="Création des index...";
		for(Index index : indexes.values()) {
			for(int i=0;i<tuples.size();i++) {
				index.add(this.dico.getValue(tuples.get(i)[0]),this.dico.getValue(tuples.get(i)[1]),this.dico.getValue(tuples.get(i)[2]));
			}
		}

		if(opt.getVerbose()) {
			System.out.println(verbose);
		}
		long timeSpent_i = System.nanoTime() - startTime_i;

		stats.setIndexesCreationTotalTime((int)timeSpent_i/1000000);

		tuples.clear();
	}

}

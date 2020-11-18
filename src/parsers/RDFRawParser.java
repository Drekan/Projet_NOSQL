package parsers;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;

import org.openrdf.model.Statement;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.Rio;
import org.openrdf.rio.helpers.RDFHandlerBase;

import moteur.Dictionnaire;
import moteur.Index;
import moteur.Solveur;
import moteur.Statistics;

public final class RDFRawParser {

	public static class RDFListener extends RDFHandlerBase {

		public int ressourcesNum = 0;
		public Dictionnaire dico;

		public RDFListener(Dictionnaire d) {
			super();
			this.dico = d;
		}

		@Override
		public void handleStatement(Statement st) {
			String subject = st.getSubject().toString();
			String predicate = st.getPredicate().toString();
			String object = st.getObject().toString();
				
			if(subject.startsWith("\"")) {
				subject = subject.substring(1,subject.length()-1);
				System.out.println(subject);
			}
			
			if(predicate.startsWith("\"")) {
				predicate = predicate.substring(1,predicate.length()-1);
				
			}
			
			if(object.startsWith("\"")) {
				object = object.substring(1,object.length()-1);
				
			}
			
			this.ressourcesNum+=3;
			this.dico.add(subject,predicate,object);
		}
	};


	public static void main(String args[]) throws FileNotFoundException {
		//TODO : normalement il faut utiliser options du solver!
		String dataPath = "datasets/100K.rdfxml";
		String queriesPath = "queries.txt";
		String outputPath= "results/";

		Reader reader = new FileReader(dataPath);
		org.openrdf.rio.RDFParser rdfParser = Rio.createParser(RDFFormat.RDFXML);
		
		System.out.println("Voulez-vous cr�er un dictionnaire tri� ou non ? (y/N)");
		//Scanner sc = new Scanner(System.in);
		//String s = sc.nextLine();
		//Boolean sort = s.equals("y");
		//TODO: à ENLEVER
		Boolean sort=false;

		Dictionnaire d = new Dictionnaire(sort);
		ArrayList<Index> indexes = new ArrayList<>();

		indexes.add(new Index("spo"));
		indexes.add(new Index("sop"));
		indexes.add(new Index("pos"));
		indexes.add(new Index("pso"));
		indexes.add(new Index("osp"));
		indexes.add(new Index("ops"));

		/*
		 * 	private String outputPath;
	private String dataPath;
	private String queriesPath;
		 */
		
		Statistics stats = new Statistics(outputPath,dataPath,queriesPath);
		
		String options="";
		Solveur solveur = new Solveur(d, indexes,stats,options);
		RDFListener rdf_l = new RDFListener(d);
		String verbose="";

		rdfParser.setRDFHandler(rdf_l);
		try {
			verbose+="Parsing des donn�es...\n";
			
			long startTime_d = System.nanoTime();
			rdfParser.parse(reader,"");
			d.createDico();
			long timeSpent_d = System.nanoTime() - startTime_d;
			
			stats.setRDFTripleNum(d.getTuples().size());
			
			verbose+="La taille du dictionnaire est de " + d.getSize()+"\n";
			verbose+="Nombre total de ressources lues : " + rdf_l.ressourcesNum+"\n";

			verbose+="Cr�ation des index...";
			ArrayList<String[]> tuples = d.getTuples();
			
			long startTime_i = System.nanoTime();
			for(Index index : indexes) {
				for(int i=0;i<tuples.size();i++) {
					index.add(d.getValue(tuples.get(i)[0]),d.getValue(tuples.get(i)[1]),d.getValue(tuples.get(i)[2]));
					//System.out.println(d.getValue(tuples.get(i)[0])+" "+d.getValue(tuples.get(i)[1])+" "+d.getValue(tuples.get(i)[2]));
				}
			}
			long timeSpent_i = System.nanoTime() - startTime_i;
			
			stats.setIndexesCreationTotalTime((int)timeSpent_i/1000000);
			
			long timeSpent_s = System.nanoTime();
			//TODO traitement des options
			if(solveur.getOptions().getWarmPct()!=0){
				solveur.warm(solveur.getOptions().getWarmPct());
			}
			solveur.traiterQueries();
			timeSpent_s = System.nanoTime() - timeSpent_s;

			String jenaTime="";
			if(solveur.getOptions().getJena()) {
				verbose += "Appel de JENA : \n";
				long timeSpent_JENA = System.nanoTime();
				solveur.jenaQueries(queriesPath, dataPath);
				timeSpent_JENA = System.nanoTime() - timeSpent_JENA;
				jenaTime="Temps total JENA : " + (timeSpent_JENA) / 1000000 + "ms\n";
				//TODO: je pense qu'il faudrait appeler comparisonJena dans le solveur
			}
			
			stats.setTotalTime(timeSpent_i+timeSpent_d+timeSpent_s);
			
			tuples.clear();		

			verbose+="Fin du programme. \n";

			verbose+="\n---Statistiques--- \n";
			verbose+="Temps de génération du dictionnaire : " + timeSpent_d / 1000000 + "ms \n";
			verbose+="Temps de génération de l'index : " + timeSpent_i / 1000000 + "ms \n";
			verbose+="Temps de traitement des requêtes : " + timeSpent_s / 1000000 + "ms \n";
			verbose+="Temps total : " + (timeSpent_i + timeSpent_d + timeSpent_s) / 1000000 + "ms \n";
			verbose+=jenaTime+"\n";

			if(solveur.getOptions().getVerbose()) {
				System.out.println(verbose);
			}


		} catch (Exception e) {

		}

		try {
			reader.close();
		} catch (IOException e) {}

	}

}
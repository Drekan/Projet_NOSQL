package parsers;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;

import moteur.*;
import org.openrdf.model.Statement;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.Rio;
import org.openrdf.rio.helpers.RDFHandlerBase;

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


	public static void main(String args[]) throws IOException, MalformedQueryException {
		/*
		//TODO : normalement il faut utiliser options du solver!
		String optionsLine="";
		Options options = new Options(optionsLine);
		//String dataPath = "datasets/100K.rdfxml";
		//String queriesPath = "queries.txt";
		//String outputPath= "results/";

		
		Solveur solveur = new Solveur(d, indexes,stats,optionsLine);


			if(solveur.getOptions().getWarmPct()!=0){
				solveur.warm(solveur.getOptions().getWarmPct());
			}

			if(solveur.getOptions().getOptim_none()){
				solveur.traiterQueries(false);
			}
			else{
				solveur.traiterQueries(true);
			}

			if(solveur.getOptions().getStar_queries()) {
				//TODO !!
			}

			timeSpent_s = System.nanoTime() - timeSpent_s;
			String verbose ="";
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


			verbose+="Temps de traitement des requêtes : " + timeSpent_s / 1000000 + "ms \n";
			verbose+="Temps total : " + (timeSpent_i + timeSpent_d + timeSpent_s) / 1000000 + "ms \n";
			verbose+=jenaTime+"\n";

			if(solveur.getOptions().getExport_query_stats()){
				//writeStatistics
			}

			if(solveur.getOptions().getVerbose()) {
				System.out.println(verbose);
			}
			System.out.println();
			System.out.println(indexes.get(0).getType());
			indexes.get(0).displayNTriples(10);

*/
	}



}
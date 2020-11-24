package main;
import java.io.IOException;

import org.openrdf.query.MalformedQueryException;

import moteur.DataStructure;
import moteur.Options;
import moteur.Solveur;
import moteur.Statistics;

public class MiniProjet {

	//TODO: gérer l'UTF-8

	/**
	 * Classe principale du projet
	 * @param args
	 * @throws IOException
	 * @throws MalformedQueryException
	 */
	public static void main(String[] args) throws IOException, MalformedQueryException {
		long startTime_i = System.nanoTime();

		//I. Définition des options
		Options options = new Options(args);

		//Options à commenter
		/*
		options.setVerbose(true);
		options.setExport_query_results(true);
		options.setExport_query_stats(true);
		options.setJena(true);
		options.setOptim_none(false);
		//options.setDiagnostic(true);
		options.setDataPath("datasets/500K.rdfxml");*/

		Statistics statistiques = new Statistics(options);

		//II. Définition des DataStructure
		DataStructure dataStructure = new DataStructure(options);

		dataStructure.createDico(false,statistiques);

		dataStructure.createIndexes(statistiques);

		//III. Partie solveur
		Solveur solveur = new Solveur(dataStructure, options, statistiques);

		//TODO: à améliorer
		long timeSpent_i = System.nanoTime() - startTime_i;
		solveur.traiterQueries((int)timeSpent_i/1000000);
	}
}
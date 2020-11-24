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
		String optionsLine = ""; // TODO: à supprimer et remplacer par args
		//I. Définition des options
		Options options = new Options(optionsLine);

		//activation du mode verbose
		//options.setVerbose(true);

		options.setJena(false);

		//à commenter
		options.setOptim_none(false);
		//options.setDiagnostic(true);

		//à commenter
		options.setDataPath("datasets/500K.rdfxml");

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
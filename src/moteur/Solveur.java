package moteur;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.util.FileManager;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.ProjectionElem;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

public class Solveur {
	private Dictionnaire dictionnaire;

	private HashMap<String, Index> indexes;

	private Options options;

	private Statistics stats;

	/**
	 * indexMap : structure qui permet de savoir quel index utiliser en fonction du pattern que l'on a.
	 *
	 * Exemple :
	 * 12 <=> p et o ont une valeur connue, on utilise donc un index pos
	 * 0 <=> s a une valeur connue, on utilise donc un index spo
	 **/
	private HashMap<String,String> indexMap;

	public Solveur(DataStructure dataStructure, Options options, Statistics stats){
		this.indexMap = new HashMap<>();
		this.options = options;
		this.dictionnaire = dataStructure.getDico();
		this.indexes = dataStructure.getIndexes();
		this.stats = stats;

		for(Index i: this.indexes.values()){
			this.indexes.put(i.getType(),i);
		}

		this.indexMap.put("","spo"); //cas où pattern = ?x ?p ?y
		this.indexMap.put("0","spo");
		this.indexMap.put("1","pso");
		this.indexMap.put("2","osp");
		this.indexMap.put("01","spo");
		this.indexMap.put("02","sop");
		this.indexMap.put("12","pos");
	}


	public Statistics getStats(){
		return  this.stats;
	}

	/**
	 * Construit et retourne un arraylist contenant toutes les requetes
	 * @return
	 */
	public ArrayList<String> buildQueriesAL() {
		String queriesPath = this.options.getQueriesPath();
		try {
			long startTime_i = System.nanoTime();
			File myObj = new File(queriesPath);
			Scanner myReader = new Scanner(myObj);
			ArrayList<String> queries = new ArrayList<>();
			while (myReader.hasNextLine()) {
				String data = myReader.nextLine();
				if(!data.equals("")) {
					queries.add(data);
				}
			}
			long timeSpent_i = System.nanoTime() - startTime_i;
			this.stats.setQueriesReadTime((int)timeSpent_i/1000000);
			return queries;

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 *
	 * @param timeSpent
	 * @throws MalformedQueryException
	 */
	public void traiterQueries(long timeSpent) throws MalformedQueryException {
		System.out.println(this.dictionnaire.getSize()+" "+this.indexes.get("pos").getValuesNumber());
		ArrayList<String> queries = buildQueriesAL();
		boolean optim_none = this.options.getOptim_none();

		if(options.getWarmPct()!=0){
			this.warm(options.getWarmPct(),queries,optim_none);
		}

		if(this.options.getShuffle()) {
			System.out.println("Shuffle");
			Collections.shuffle(queries);
		}

		long startTime_i = System.nanoTime();

		for(String query: queries) {
			ArrayList<String> starVariables = getStarVariables(query);
			if(starVariables.size()==0) {
				if (!optim_none) {
					System.out.println("OPTIM");
					solveOptim(query);
				} else {
					System.out.println("NAIVE");
					solve(query);
				}
			}
			else{
				System.out.println("STAR QUERY");
				solveStarQuery(query,starVariables);
			}
		}
		long timeSpent_i = System.nanoTime() - startTime_i;
		//TODO: on a pas pris en compte le warm ou quoi ?
		this.stats.setWorkloadEvaluationTime((int)timeSpent_i/1000000);
		this.stats.setQueriesNum(queries.size());
		this.stats.setTotalTime((int)timeSpent_i/1000000+(int)timeSpent/1000000);
		//TODO: mettre dans des variables pour que ce soit joli ? ou fonction ?

		//TODO: est-ce bien cette fonction ?
		if(options.getExport_query_stats()){
			this.stats.writeStats();
		}
	}

	public String encodePattern(StatementPattern sp) {
		String subject = (sp.getSubjectVar().hasValue()?"0":"");
		String predicate = (sp.getPredicateVar().hasValue()?"1":"");
		String object = (sp.getObjectVar().hasValue()?"2":"");

		return subject+predicate+object;
	}

	public ArrayList<String> getVariables(StatementPattern sp) throws MalformedQueryException{
		ArrayList<String> variables = new ArrayList<>();

		if(!sp.getSubjectVar().hasValue()) {
			variables.add(sp.getSubjectVar().getName());
		}

		if(!sp.getPredicateVar().hasValue()) {
			variables.add(sp.getPredicateVar().getName());
		}

		if(!sp.getObjectVar().hasValue()) {
			variables.add(sp.getObjectVar().getName());
		}

		return variables;
	}

	public ArrayList<String> getConstantes(StatementPattern sp) throws MalformedQueryException{
		ArrayList<String> constantes = new ArrayList<>();

		if(sp.getSubjectVar().hasValue()) {
			constantes.add(sp.getSubjectVar().getValue().toString().replace("\"",""));
		}

		if(sp.getPredicateVar().hasValue()) {
			constantes.add(sp.getPredicateVar().getValue().toString().replace("\"",""));
		}

		if(sp.getObjectVar().hasValue()) {
			constantes.add(sp.getObjectVar().getValue().toString().replace("\"",""));
		}


		return constantes;
	}

	public ArrayList<String> getStarVariables(String req) throws MalformedQueryException {
		//les patterns sont récupérés
		List<StatementPattern> patterns = StatementPatternCollector.process(new SPARQLParser().parseQuery(req, null).getTupleExpr());

		//on initialise starVariables avec toutes les variables du premier pattern
		ArrayList<String> starVariables = new ArrayList<>(getVariables(patterns.get(0)));

		//on fait l'intersection avec les variables de chaque pattern
		for(StatementPattern sp : patterns) {
			starVariables.retainAll(getVariables(sp));
		}
		return starVariables;
	}

	public String getCommonVariable(HashMap<String,ArrayList<Integer>> first,HashMap<String,ArrayList<Integer>> second) {
		ArrayList<String> variables = new ArrayList<>(first.keySet());
		variables.retainAll(second.keySet());

		return variables.isEmpty()?"":variables.get(0);
	}

	public ArrayList<ArrayList<String>> produitCartesien(ArrayList<ArrayList<String>> left,ArrayList<ArrayList<String>> right){
		ArrayList<ArrayList<String>> result = new ArrayList<>();
		result.add(new ArrayList<>());
		ArrayList<String> concat = left.get(0);
		concat.addAll(right.get(0));

		result.get(0).addAll(concat);

		int i = 0;
		for(ArrayList<String> leftLine : left) {
			int j = 0;
			for(ArrayList<String> rightLine : right){
				if(i!=0 && j!=0) {
					concat = new ArrayList<>(leftLine);
					concat.addAll(rightLine);
					result.add(concat);
				}

				j++;
			}
			i++;
		}

		return result;
	}

	/**
	 * Méthode M2a
	 * @param req
	 */
	public void solve(String req) throws MalformedQueryException{
		long startTime = System.nanoTime();

		List<StatementPattern> patterns = StatementPatternCollector.process(new SPARQLParser().parseQuery(req, null).getTupleExpr());

		//1. regrouper les patterns connexes
		HashMap<StatementPattern,Integer> patternConnexes = new HashMap<>();

		int idx=0;
		//au début, un sp par case, puis on va les regrouper par composante connexe
		for(StatementPattern sp : patterns) {
			patternConnexes.put(sp,idx);
			idx++;
		}

		for(StatementPattern sp1 : patterns) {
			for(StatementPattern sp2 : patterns) {
				ArrayList<String> variablesSp1 = getVariables(sp1);
				ArrayList<String> variablesSp2 = getVariables(sp2);

				variablesSp1.retainAll(variablesSp2);

				//intersection non vide <=> il y a des variables communes entre sp1 et sp2
				if(!variablesSp1.isEmpty()) {
					patternConnexes.put(sp2,patternConnexes.get(sp1));
				}
			}
		}

		//toutes les composantes existantes
		ArrayList<Integer> allComposantes = new ArrayList<>(patternConnexes.values());

		//toutes les fusions de composantes seront stoquées ici :
		ArrayList<ArrayList<ArrayList<String>>> globalResult = new ArrayList<>();
		//pour chaque composante, on veut avoir un résultat partiel
		for(int composante : allComposantes) {

			//on récupère les sp de la composante actuelle
			ArrayList<StatementPattern> currentPatterns = new ArrayList<>();

			for(StatementPattern sp : patternConnexes.keySet()) {
				if(patternConnexes.get(sp).equals(composante)) {
					currentPatterns.add(sp);
				}
			}

			//les résultats de la composante actuelle seront d'abord stoqués ici
			HashMap<StatementPattern, HashMap<String,ArrayList<Integer>>> allResults = new HashMap<>();
			ArrayList<String> allVariable = new ArrayList<>();

			//on fait la résolution classique
			for(StatementPattern sp: currentPatterns) {
				allResults.put(sp,new HashMap<>());

				//on encode le pattern pour savoir quel index utiliser
				String indexType = this.indexMap.get(this.encodePattern(sp));
				Index index = this.indexes.get(indexType);

				ArrayList<String> variables = getVariables(sp);
				ArrayList<String> constantes = getConstantes(sp);

				//on ajoute les variables dans la hashmap du pattern actuel
				for(String v: variables) {
					if(!allVariable.contains(v)) {
						allVariable.add(v);
					}
					allResults.get(sp).put(v,new ArrayList<>());
				}

				if(constantes.size() == 2) { // deux constantes dans le pattern
					int c1 = this.dictionnaire.getValue(constantes.get(0));
					int c2 = this.dictionnaire.getValue(constantes.get(1));

					allResults.get(sp).put(variables.get(0),index.getIndex().get(c1).get(c2));
				}
				else if(constantes.size() == 1) { // une constante dans le pattern
					int c1 = this.dictionnaire.getValue(constantes.get(0));

					Set<Integer> keys_c1 = index.getIndex().get(c1).keySet();
					ArrayList<Integer> resO = new ArrayList();
					for (int i : keys_c1) {
						for(int j : index.getIndex().get(c1).get(i)) {
							allResults.get(sp).get(variables.get(0)).add(i);
							allResults.get(sp).get(variables.get(1)).add(j);
						}
					}
				}
				else {
					//Cas où il y a 3 variables
					//Choix de base SPO
					//TODO: normalement n'arrive jamais alors on enlève ?
					// TODO: est-ce qu'il existe un plus optimisé qu'un autre?
					// TODO: Vérifier noms de variables

					Index spo = this.indexes.get("spo");

					for(int s : spo.getIndex().keySet()) {
						for(int p : spo.getIndex().get(s).keySet()) {
							for(int o : spo.getIndex().get(s).get(p)) {
								allResults.get(sp).get(variables.get(0)).add(s);
								allResults.get(sp).get(variables.get(1)).add(p);
								allResults.get(sp).get(variables.get(2)).add(o);
							}
						}
					}
				}
			}
			//ici, allResult pour la composante actuelle est remplie.
			//On doit maintenant faire un merge
			ArrayList<HashMap<String,ArrayList<Integer>>> toMerge = new ArrayList<>(allResults.values());

			while(toMerge.size()>1) {
				HashMap<String,ArrayList<Integer>> first = toMerge.remove(0);
				HashMap<String,ArrayList<Integer>> second = toMerge.remove(0);

				String commonVariable = getCommonVariable(first,second);

				toMerge.add(this.mergeGeneral(first, second, commonVariable));
			}

			//on reformate en matrice de String
			ArrayList<ArrayList<String>> results = new ArrayList<>();

			//taille de la première colonne de la fusion des patterns courants
			int size = toMerge.get(0).get(toMerge.get(0).keySet().iterator().next()).size();
			//initialisation de chaque ligne de la matrice résultat
			for(int i = 0; i<= size;i++) {
				results.add(new ArrayList());
			}

			for(String variable : toMerge.get(0).keySet()) {
				int currentLine = 0;
				results.get(currentLine).add(variable);

				for(int value : toMerge.get(0).get(variable)) {
					currentLine++;
					results.get(currentLine).add(this.dictionnaire.getValue(value));
				}
			}

			globalResult.add(results);



		}


		//Faire un produit cartésien de chaque table : c'est les résultats

		while(globalResult.size()>1) {
			ArrayList<ArrayList<String>> left = globalResult.remove(0);
			ArrayList<ArrayList<String>> right = globalResult.remove(0);

			globalResult.add(produitCartesien(left,right));
		}

		ArrayList<ArrayList<String>> queryResult = globalResult.get(0);


		//Cette structure nous permet d'avoir uniquement les variables à retourner (celles dans le SELECT)
		ArrayList<String> varToReturn = new ArrayList<>();
		ParsedQuery pq = new SPARQLParser().parseQuery(req, null);
		pq.getTupleExpr().visit(new QueryModelVisitorBase<RuntimeException>() {
			public void meet(Projection projection) {
				List<ProjectionElem> test = projection.getProjectionElemList().getElements();
				for(ProjectionElem p: test){
					varToReturn.add(p.getSourceName());
				}
			}
		});

		ArrayList<Integer> indicesVariablesProjetees = new ArrayList<>();
		for(int i = 0; i<queryResult.get(0).size();i++) {
			if(varToReturn.contains(queryResult.get(0).get(i))) {
				indicesVariablesProjetees.add(i);
			}
		}

		if(this.options.getVerbose()) {
			System.out.println("--------Résultats--------");
			for (ArrayList<String> ligne : queryResult) {
				for(int i = 0 ; i<ligne.size();i++) {
					if(indicesVariablesProjetees.contains(i)) {
						System.out.print(ligne.get(i)+", ");
					}
				}
				System.out.println();
			}
		}

		long timeSpent = System.nanoTime() - startTime;
		Integer tS = ((int)timeSpent/1000000);

	}

	/**
	 * Méthode M2b (optimisée)
	 * @param req
	 * @throws MalformedQueryException
	 */

	public void solveOptim(String req) throws MalformedQueryException {
		//TODO: merge join
		long startTime = System.nanoTime();
		String outputPath = this.options.getOutputPath();
		String verbose ="";
		verbose+="\nRequete: "+req+"\n";

		SPARQLParser sparqlParser = new SPARQLParser();
		ParsedQuery pq = sparqlParser.parseQuery(req, null);
		List<StatementPattern> patterns = StatementPatternCollector.process(pq.getTupleExpr());

		HashMap<StatementPattern, HashMap<String,ArrayList<Integer>>> allResults = new HashMap<>();

		verbose+="-- Lecture de chaque pattern par sélectivité croissante"+"\n";

		HashMap<StatementPattern, Double> selectivities = new HashMap<>();
		for(StatementPattern sp: patterns) {
			selectivities.put(sp,selectivity(sp));
		}
		ArrayList<StatementPattern> alreadySolved = new ArrayList<>();

		HashMap<String, ArrayList<Integer>> resultsPerVariable = new HashMap<>();


		while(alreadySolved.size()<patterns.size()) {
			//TODO: vérifier que ça marche
			StatementPattern spCurrent = minSelectivity(alreadySolved, selectivities);

			HashMap<String, ArrayList <Integer>> getResult = getResult(spCurrent, resultsPerVariable);

			System.out.println("/::::::::::::::"+ getResult.get("x"));
			//Si pas la variable
			/*
			for(String key: getResult.keySet()) {
				if (!resultsPerVariable.containsKey(key)) {
					resultsPerVariable.put(key, new ArrayList<>(getResult.get(key)));
				} else {
					//TODO: prendre les valeurs
					resultsPerVariable.get(key).retainAll(getResult.get(key));
				}
			}*/
			for(String s: resultsPerVariable.keySet()){
				System.out.println(s+" "+resultsPerVariable.get(s));
			}
		}

		//sortMergeJoin();



		//TODO: optimization time

		//On construit une chaine de caractères sous format CSV de nos résultats
		// Afin de pouvoir le comparer à celui de Jena

		String CSVResults="";
		/*
		for (String : results) {
			for(int i = 0 ; i<ligne.size();i++) {
				if(indicesVariablesProjetees.contains(i)) {
					CSVResults+=ligne.get(i)+",";
				}
			}
			CSVResults=CSVResults.substring(0,CSVResults.length()-1);
			CSVResults+="\n";
		}

		 */


		String jenaString = "NON_DISPONIBLE";
		if(this.options.getJena()) {
			Boolean jena = this.jenaComparison(req, CSVResults);
			if(jena){
				jenaString = "True";
			}
			else{
				jenaString="False";
			}
		}

		long timeSpent = System.nanoTime() - startTime;
		Integer tS = ((int)timeSpent/1000000);
		System.out.println("TEMPS= "+ tS.toString());
	}

	//Avoir la meme taille pour les AL dans results
	public HashMap<String, ArrayList<Integer>> getResult(StatementPattern sp, HashMap<String, ArrayList<Integer>> results) throws MalformedQueryException {
		String verbose = "";
		HashMap<String, ArrayList<Integer>> res = new HashMap<>();
		ArrayList<String> allVariable = new ArrayList<>();
		//on encode le pattern pour savoir quel index utiliser

		String indexType = this.indexMap.get(this.encodePattern(sp));
		Index index = this.indexes.get(indexType);

		verbose+="  Index utilisé: " + indexType+"\n";

		ArrayList<String> variables = getVariables(sp);
		ArrayList<String> constantes = getConstantes(sp);

		//on ajoute les variables dans la hashmap du pattern actuel
		for(String v: variables) {
			if(!allVariable.contains(v)) {
				allVariable.add(v);
			}
			res.put(v,new ArrayList<>());
		}

		if(constantes.size() == 2) { // deux constantes dans le pattern
			int c1 = this.dictionnaire.getValue(constantes.get(0));
			int c2 = this.dictionnaire.getValue(constantes.get(1));

			//Si resultats a la variable
			/*if(results.containsKey(variables.get(0))){
				//S'il ne l'a alors on l'a rajoute au résultat du pattern
				if(results.get(variables.get(0)).contains(index.getIndex().get(c1).get(c2))){
					res.put(variables.get(0), index.getIndex().get(c1).get(c2));
				}
			}else {
				//si résultat a pas la variable alors on l'ajoute
				res.put(variables.get(0), index.getIndex().get(c1).get(c2));
				results.put(variables.get(0), new ArrayList<>(index.getIndex().get(c1).get(c2)));
				System.out.println(results.toString());
			}*/
			
			
			res.put(variables.get(0), index.getIndex().get(c1).get(c2));
			results.put(variables.get(0), new ArrayList<>(index.getIndex().get(c1).get(c2)));
			
		}
		else if(constantes.size() == 1) { // une constante dans le pattern
			int c1 = this.dictionnaire.getValue(constantes.get(0));

			Set<Integer> keys_c1 = index.getIndex().get(c1).keySet();
			ArrayList<Integer> resO = new ArrayList();
			for (int i : keys_c1) {
				for(int j : index.getIndex().get(c1).get(i)) {
					//TODO
					// get(0) = x / get(1) = y
					// a x en var

					
					if(results.containsKey(variables.get(0))){
						//a la valeur de x
						if(results.get(variables.get(0)).contains(index.getIndex().get(c1).get(i))){
							res.get(variables.get(0)).add(i);
						}
						if(results.containsKey(variables.get(1))) {
							if (results.get(variables.get(1)).contains(index.getIndex().get(c1).get(i))) {
								res.get(variables.get(1)).add(j);
							}
						}
						else {
							res.get(variables.get(1)).add(j);
							results.get(variables.get(1)).add(j);
						}
						results.get(variables.get(0)).add(i);
					}
					else if(results.containsKey(variables.get(1))) {
						if (results.get(variables.get(1)).contains(index.getIndex().get(c1).get(i))) {
							res.get(variables.get(1)).add(j);
						}
						res.get(variables.get(0)).add(i);
						results.get(variables.get(0)).add(i);
						results.get(variables.get(1)).add(j);
					}

					else{
						res.get(variables.get(0)).add(i);
						res.get(variables.get(1)).add(j);
						results.get(variables.get(0)).add(i);
						results.get(variables.get(1)).add(j);
					}
					
				}
			}
		}
		else {
			//Cas où il y a 3 variables
			//Choix de base SPO

			Index spo = this.indexes.get("spo");

			for(int s : spo.getIndex().keySet()) {
				for(int p : spo.getIndex().get(s).keySet()) {
					for(int o : spo.getIndex().get(s).get(p)) {
						if(results.containsKey(variables.get(0))){
							if(results.get(variables.get(0)).contains(index.getIndex().get(s).get(p))){
								res.get(variables.get(0)).add(s);
								res.get(variables.get(1)).add(p);
								res.get(variables.get(2)).add(o);
							}
						}else {
							res.get(variables.get(0)).add(s);
							res.get(variables.get(1)).add(p);
							res.get(variables.get(2)).add(o);
							results.get(variables.get(0)).add(s);
							results.get(variables.get(1)).add(p);
							results.get(variables.get(2)).add(o);
						}
					}
				}
			}
		}
		return res;
	}

	public StatementPattern minSelectivity(ArrayList<StatementPattern> alreadySolved, HashMap<StatementPattern, Double> selectivities){
		StatementPattern minSp = new StatementPattern();
		double minS = 1;
		for(StatementPattern sp: selectivities.keySet()){
			if(selectivities.get(sp)<minS && !alreadySolved.contains(sp)){
				minSp = sp;
				minS = selectivities.get(sp);
			}
		}
		alreadySolved.add(minSp);
		return minSp;
	}

	/**
	 * Méthode M1 évaluant uniquement les requetes en étoiles
	 * @param req
	 * @param starVariables
	 * @throws MalformedQueryException
	 */
	public void solveStarQuery(String req, ArrayList<String> starVariables) throws MalformedQueryException {
		long startTime = System.nanoTime();
		String outputPath = this.options.getOutputPath();
		String verbose ="";
		verbose+="\nRequete: "+req+"\n";

		//Utilisation d'une instance de SPARLQLParser
		SPARQLParser sparqlParser = new SPARQLParser();
		ParsedQuery pq = sparqlParser.parseQuery(req, null);
		List<StatementPattern> patterns = StatementPatternCollector.process(pq.getTupleExpr());

		HashMap<StatementPattern, HashMap<String,ArrayList<Integer>>> allResults = new HashMap<>();
		// Cette structure permet d'obtenir tous les résultats pour toutes les variables pour tous les patterns
		//Clé = la valeur recherchée
		//Valeurs = un ensemble d'ensembles de résultats pour chaque pattern

		verbose+="-- Lecture de chaque pattern"+"\n";

		ArrayList<String> allVariable = new ArrayList<>();
		for(StatementPattern sp: patterns) {
			allResults.put(sp,new HashMap<>());

			//on encode le pattern pour savoir quel index utiliser
			String indexType = this.indexMap.get(this.encodePattern(sp));
			Index index = this.indexes.get(indexType);

			verbose+="  Index utilisé: " + indexType+"\n";

			ArrayList<String> variables = getVariables(sp);
			ArrayList<String> constantes = getConstantes(sp);

			//on ajoute les variables dans la hashmap du pattern actuel
			for(String v: variables) {
				if(!allVariable.contains(v)) {
					allVariable.add(v);
				}
				allResults.get(sp).put(v,new ArrayList<>());
			}

			if(constantes.size() == 2) { // deux constantes dans le pattern
				int c1 = this.dictionnaire.getValue(constantes.get(0));
				int c2 = this.dictionnaire.getValue(constantes.get(1));

				allResults.get(sp).put(variables.get(0),index.getIndex().get(c1).get(c2));
			}
			else if(constantes.size() == 1) { // une constante dans le pattern
				int c1 = this.dictionnaire.getValue(constantes.get(0));

				Set<Integer> keys_c1 = index.getIndex().get(c1).keySet();
				ArrayList<Integer> resO = new ArrayList();
				for (int i : keys_c1) {
					for(int j : index.getIndex().get(c1).get(i)) {
						allResults.get(sp).get(variables.get(0)).add(i);
						allResults.get(sp).get(variables.get(1)).add(j);
					}
				}
			}
			else {
				//Cas où il y a 3 variables
				//Choix de base SPO
				//TODO: normalement n'arrive jamais alors on enlève ?
				// TODO: est-ce qu'il existe un plus optimisé qu'un autre?
				// TODO: Vérifier noms de variables

				Index spo = this.indexes.get("spo");

				for(int s : spo.getIndex().keySet()) {
					for(int p : spo.getIndex().get(s).keySet()) {
						for(int o : spo.getIndex().get(s).get(p)) {
							allResults.get(sp).get(variables.get(0)).add(s);
							allResults.get(sp).get(variables.get(1)).add(p);
							allResults.get(sp).get(variables.get(2)).add(o);
						}
					}
				}
			}
		}

		//avoir la variable qui apparait dans chaque pattern


		//Dans allResults on a les résultats de chaque variable pour chaque pattern
		//Ici on fait pour chaque variable l'intersection des résultats
		//Ca nous permet d'obtenir pour chaque variable l'ensemble des résultas
		ArrayList<ArrayList<String>> results = this.extractResults(starVariables.get(0), allResults);


		//Cette structure nous permet d'avoir uniquement les variables à retourner (celles dans le SELECT)
		ArrayList<String> varToReturn = new ArrayList<>();
		verbose+="-- Résultat de la requete"+"\n";
		pq.getTupleExpr().visit(new QueryModelVisitorBase<RuntimeException>() {
			public void meet(Projection projection) {
				List<ProjectionElem> test = projection.getProjectionElemList().getElements();
				for(ProjectionElem p: test){
					varToReturn.add(p.getSourceName());
				}
			}
		});

		ArrayList<Integer> indicesVariablesProjetees = new ArrayList<>();
		for(int i = 0; i<results.get(0).size();i++) {
			if(varToReturn.contains(results.get(0).get(i))) {
				indicesVariablesProjetees.add(i);
			}
		}

		long timeSpent = System.nanoTime() - startTime;
		Integer tS = ((int)timeSpent/1000000);

		if(this.options.getVerbose()){
			System.out.println(verbose);
			System.out.println("--------Résultats--------");
			for (ArrayList<String> ligne : results) {
				for(int i = 0 ; i<ligne.size();i++) {
					if(indicesVariablesProjetees.contains(i)) {
						System.out.print(ligne.get(i)+", ");
					}
				}
				System.out.println();
			}
		}

		//On construit une chaine de caractères sous format CSV de nos résultats
		// Afin de pouvoir le comparer à celui de Jena
		String CSVResults="";
		for (ArrayList<String> ligne : results) {
			for(int i = 0 ; i<ligne.size();i++) {
				if(indicesVariablesProjetees.contains(i)) {
					CSVResults+=ligne.get(i)+",";
				}
			}
			CSVResults=CSVResults.substring(0,CSVResults.length()-1);
			CSVResults+="\n";
		}

		String jenaString = "NON_DISPONIBLE";
		if(this.options.getJena()) {
			Boolean jena = this.jenaComparison(req, CSVResults);
			if(jena){
				jenaString = "True";
			}
			else{
				jenaString="False";
			}
		}

		//TODO : QUELLE STRUCTURRRE POUR LE CSV ???
		if(this.options.getExport_query_results()) {
			try {
				FileWriter myWriter = new FileWriter(outputPath + "queryResult.csv");
				myWriter.write(CSVResults);
				myWriter.close();
			} catch (IOException e) {
				System.out.println("Erreur dans l'écriture du résultat de la requête");
				e.printStackTrace();
			}
		}

		writeQueryStat(req, tS.toString(),"","","",jenaString); //TODO
	}

	//TODO : dans le cas où toutes les variables ne sont pas à projeter, éviter de considérer les variables qui ne
	// seront pas dans le résultat ?
	public ArrayList<ArrayList<String>> extractResults(String starVariable,HashMap<StatementPattern,HashMap<String,ArrayList<Integer>>> allResults){

		/*1. On intersecte toutes les valeurs de la variable étoile pour tous les patterns */
		Set<Integer> starVariable_values = new HashSet<Integer>();

		//magie noire pour avoir la première clef de allResults
		Iterator<StatementPattern> patternIterator = allResults.keySet().iterator();
		StatementPattern first_pattern = patternIterator.next();

		for(Integer v : allResults.get(first_pattern).get(starVariable)) {
			starVariable_values.add(v);
		}

		while(patternIterator.hasNext()) {
			StatementPattern sp = patternIterator.next();
			starVariable_values.retainAll(allResults.get(sp).get(starVariable));
		}

		/*2.On prépare un tableau avec dans chaque case les tuples candidats d'un pattern donné, puis on les fusionne*/
		ArrayList<HashMap<String,ArrayList<Integer>>> toMerge = new ArrayList<>();

		for(StatementPattern sp : allResults.keySet()) {
			toMerge.add(allResults.get(sp));
		}

		while(toMerge.size()>1) {
			HashMap<String,ArrayList<Integer>> first = toMerge.remove(0);
			HashMap<String,ArrayList<Integer>> second = toMerge.remove(0);

			toMerge.add(merge(first,second,new ArrayList<Integer>(starVariable_values),starVariable));
		}

		//Il ne reste plus qu'un ensemble de tuples candidat : il s'agit du résultat
		HashMap<String,ArrayList<Integer>> mergedResults = toMerge.get(0);

		//on reformate en matrice de String
		ArrayList<ArrayList<String>> results = new ArrayList<>();
		//initialisation de chaque ligne de la matrice résultat
		for(int i = 0; i<= mergedResults.get(starVariable).size();i++) {
			results.add(new ArrayList());
		}

		for(String variable : mergedResults.keySet()) {
			int currentLine = 0;
			results.get(currentLine).add(variable);

			for(int value : mergedResults.get(variable)) {
				currentLine++;
				results.get(currentLine).add(this.dictionnaire.getValue(value));
			}
		}

		return results;
	}

	public HashMap<String,ArrayList<Integer>> merge(HashMap<String,ArrayList<Integer>> left, HashMap<String,ArrayList<Integer>> right,ArrayList<Integer> values,String starVariable){

		HashMap<String,ArrayList<Integer>> result = new HashMap<>();
		result.put(starVariable,new ArrayList<>());

		//TODO considérer uniquement les variables que l'on doit projeter
		ArrayList<String> varLeft = new ArrayList<>();
		ArrayList<String> varRight = new ArrayList<>();

		int l_size = left.get(starVariable).size();
		int r_size = right.get(starVariable).size();

		for(String k : left.keySet()) {
			if(!k.equals(starVariable)) {
				varLeft.add(k);
				result.put(k,new ArrayList<>());
			}
		}

		for(String k : right.keySet()) {
			if(!k.equals(starVariable)) {
				varRight.add(k);
				result.put(k,new ArrayList<>());
			}
		}

		for(int v : values) {
			for(int i=0 ; i<l_size ; i++) {
				for(int j=0 ; j<r_size ; j++) {
					if(left.get(starVariable).get(i).equals(v) && right.get(starVariable).get(j).equals(v)) {
						result.get(starVariable).add(v);

						for(String var : varLeft) {
							result.get(var).add(left.get(var).get(i));
						}

						for(String var : varRight) {
							result.get(var).add(right.get(var).get(j));
						}
					}
				}
			}
		}

		return result;
	}

	public HashMap<String,ArrayList<Integer>> mergeGeneral(HashMap<String,ArrayList<Integer>> left, HashMap<String,ArrayList<Integer>> right,String variableJointure) {

		HashMap<String, ArrayList<Integer>> result = new HashMap<>();
		result.put(variableJointure, new ArrayList<>());

		//TODO considérer uniquement les variables que l'on doit projeter
		ArrayList<String> varLeft = new ArrayList<>();
		ArrayList<String> varRight = new ArrayList<>();

		int l_size = left.get(variableJointure).size();
		int r_size = right.get(variableJointure).size();

		for (String k : left.keySet()) {
			if (!k.equals(variableJointure)) {
				varLeft.add(k);
				result.put(k, new ArrayList<>());
			}
		}

		for (String k : right.keySet()) {
			if (!k.equals(variableJointure)) {
				varRight.add(k);
				result.put(k, new ArrayList<>());
			}
		}


		for (int i = 0; i < l_size; i++) {
			for (int j = 0; j < r_size; j++) {
				if (left.get(variableJointure).get(i).equals(right.get(variableJointure).get(j))) {
					result.get(variableJointure).add(left.get(variableJointure).get(i));

					for (String var : varLeft) {
						result.get(var).add(left.get(var).get(i));
					}

					for (String var : varRight) {
						result.get(var).add(right.get(var).get(j));
					}
				}
			}
		}
		return result;
	}


	public void displayHashMap(HashMap<String,ArrayList<Integer>> map) {
		System.out.println("-----------------------");
		for(String clef : map.keySet()) {
			int maxValue = 10;
			System.out.println(clef);
			for(int value : map.get(clef)) {
				if(maxValue>0) {
					System.out.println("\t=>"+dictionnaire.getValue(value));
					maxValue--;
				}
			}
			System.out.println();

		}
	}


	//TODO: à supprimer ?
	public void jenaQueries(String queriesPath,String dataPath) {
		Model model = ModelFactory.createDefaultModel();
		InputStream in = FileManager.get().open(dataPath);

		model.read(in, null,"RDF/XML");

		try {
			File myObj = new File(queriesPath);
			Scanner myReader = new Scanner(myObj);

			while (myReader.hasNextLine()) {
				String data = myReader.nextLine();
				Query query = QueryFactory.create(data);
				QueryExecution qexec = QueryExecutionFactory.create(query,model);
				try {
					ResultSet rs = qexec.execSelect();
					System.out.println("Query : "+data);
					ResultSetFormatter.out(System.out, rs, query);
					System.out.println();

				} finally {
					qexec.close();
				}
			}

			//(String req,String dataPath,String queriesPath,String outputPath)
			myReader.close();
		} catch (FileNotFoundException e) {
			System.out.println("An error occurred.");
			e.printStackTrace();
		}
	}

	public String printRes(ArrayList<String> tab){
		String res = "";
		for(String i: tab){
			res+=i+" ";
		}
		return res;
	}


	//TODO: à supprimer  ?
	public void checkReq(String toCompare){
		System.out.println(this.dictionnaire.getValue(toCompare));
	}
	public Options getOptions(){
		return this.options;
	}

	/**
	 * Renvoie le CSV du résultat calculé par Jena d'une requete donnée
	 * @param req
	 * @return
	 */
	public String jenaSolve(String req){
		Model model = ModelFactory.createDefaultModel();
		InputStream in = FileManager.get().open(this.options.getDataPath());

		model.read(in, null,"RDF/XML");

		Query query = QueryFactory.create(req);
		QueryExecution qexec = QueryExecutionFactory.create(query,model);
		try {
			ResultSet rs = qexec.execSelect();
			//ResultSetFormatter.out(System.out, rs, query);

			OutputStream output = new OutputStream() {
				private StringBuilder string = new StringBuilder();

				@Override
				public void write(int b) throws IOException {
					this.string.append((char) b );
				}

				//Netbeans IDE automatically overrides this toString()
				public String toString() {
					return this.string.toString();
				}
			};
			ResultSetFormatter.outputAsCSV(output,rs);
			return output.toString();
		} finally {
			qexec.close();
		}
	}

	public boolean jenaComparison(String req, String ourResultCSV){
		//TODO ATTENTION AUX "EXCES"
		ArrayList<String> jena = new ArrayList<>(Arrays.asList(jenaSolve(req).split("\n")));
		ArrayList<String> ourResult = new ArrayList<>(Arrays.asList(ourResultCSV.split("\n")));

		for(String j: jena){
			if (!ourResult.contains(j)){
				return false;
			}
		}
		return true;
	}

	public void warm(float pct, ArrayList<String> queries, boolean optim_none) throws MalformedQueryException {
		Float queriesToExec=queries.size()*pct;
		ArrayList starVariables;
		ArrayList<Integer> generated = new ArrayList<>();
		for(int i=0; i<queriesToExec;i++){
			Random r = new Random();
			int c = r.nextInt(queries.size());
			while(generated.contains(c)){
				c = r.nextInt(queries.size());
			}
			generated.add(c);
			starVariables = getStarVariables(queries.get(c));
			if(starVariables.size()==0) {
				;
				if (!optim_none) {
					System.out.println("OPTIM");
					solveOptim(queries.get(c));
				} else {
					System.out.println("NAIVE");
					solve(queries.get(c));
				}
			}
			else{
				System.out.println("STAR QUERIES");
				solveStarQuery(queries.get(c),starVariables);
			}
		}
	}

	//TODO: factoriser?

	/**
	 * Calcule le critère de sélectivité pour chaque pattern
	 * @param sp
	 * @return
	 */
	public double selectivity(StatementPattern sp) {
		//Il faut récupérer la valeur dans l'index
		//les termes de la requete sont recuperes...
		ArrayList<Var> varList = new ArrayList<>();
		Var s = sp.getSubjectVar();
		Var p = sp.getPredicateVar();
		Var o = sp.getObjectVar();

		varList.add(s);
		varList.add(p);
		varList.add(o);

		//...puis separes en constante / variable
		ArrayList<String> variables = new ArrayList<>();
		ArrayList<String> constantes = new ArrayList<>();

		for (Var v : varList) {
			if (v.hasValue()) {
				constantes.add(v.getValue().toString().replace("\"", ""));
			} else {
				variables.add(v.getName());
			}
		}

		String indexType = this.indexMap.get(this.encodePattern(sp));

		String i1 = indexType.substring(0,1);

		String i2 = indexType.substring(1,2);

		if (constantes.size() == 2) {
			System.out.println(this.indexes.get(indexType).getIndex2().get(returnConvertCst(i1,s,p,o)).get(returnConvertCst(i2,s,p,o)));
			return this.indexes.get(indexType).getIndex2().get(returnConvertCst(i1,s,p,o)).get(returnConvertCst(i2,s,p,o)).doubleValue()/this.indexes.get("spo").getValuesNumber().doubleValue();
		}

		if (constantes.size() == 1) {
			System.out.println(this.indexes.get(indexType).getIndex1().get(returnConvertCst(i1,s,p,o)));
			return this.indexes.get(indexType).getIndex1().get(returnConvertCst(i1,s,p,o)).doubleValue()/this.indexes.get("spo").getValuesNumber().doubleValue();
		}

		for(String var:variables){
			System.out.println(var);
		}
		System.out.println("NOT GOOD "+constantes.size());
		return 0;
	}

	public int returnConvertCst(String i, Var s, Var p, Var o){
		String res = "";
		if(i.equals("p")){
			res = p.getValue().stringValue();
		}
		else if (i.equals("o")){
			res = o.getValue().stringValue();
		}
		else if (i.equals("s")){
			res = s.getValue().stringValue();
		}
		else{
			res = "";
		}
		return this.dictionnaire.getValue(res);
	}

	public void test(StatementPattern sp,HashMap<StatementPattern, HashMap<String,ArrayList<Integer>>> allResults, String verbose, ArrayList<String> allVariable, ArrayList<String> starVariable){
		allResults.put(sp,new HashMap<>());

		System.out.println("$$$ "+sp);
		//on encode le pattern pour savoir quel index utiliser
		String indexType = this.indexMap.get(this.encodePattern(sp));

		Index index = this.indexes.get(indexType);

		verbose+="  Index utilisé: " + indexType+"\n";

		//les termes de la requete sont recuperes...
		ArrayList<Var> varList = new ArrayList<>();
		varList.add(sp.getSubjectVar());
		varList.add(sp.getPredicateVar());
		varList.add(sp.getObjectVar());

		//...puis separes en constante / variable
		ArrayList<String> variables = new ArrayList<>();
		ArrayList<String> constantes = new ArrayList<>();

		for(Var v : varList) {
			if(v.hasValue()) {
				constantes.add(v.getValue().toString().replace("\"",""));
			}
			else {
				variables.add(v.getName());
			}
		}

		if(starVariable.isEmpty()) {
			starVariable = (ArrayList<String>)variables.clone();
		}
		else {
			starVariable.retainAll(variables);
		}

		//on ajoute les variables dans la hashmap du pattern actuel
		for(String v: variables) {
			if(!allVariable.contains(v)) {
				allVariable.add(v);
			}
			allResults.get(sp).put(v,new ArrayList<>());
		}

		if(constantes.size() == 2) { // deux constantes dans le pattern
			int c1 = this.dictionnaire.getValue(constantes.get(0));
			int c2 = this.dictionnaire.getValue(constantes.get(1));

			allResults.get(sp).put(variables.get(0),index.getIndex().get(c1).get(c2));
		}
		else if(constantes.size() == 1) { // une constante dans le pattern
			int c1 = this.dictionnaire.getValue(constantes.get(0));

			Set<Integer> keys_c1 = index.getIndex().get(c1).keySet();
			ArrayList<Integer> resO = new ArrayList();
			for (int i : keys_c1) {
				for(int j : index.getIndex().get(c1).get(i)) {
					allResults.get(sp).get(variables.get(0)).add(i);
					allResults.get(sp).get(variables.get(1)).add(j);
				}
			}
		}
		else {
			//Cas où il y a 3 variables
			//Choix de base SPO
			//TODO: normalement n'arrive jamais alors on enlève ?
			// TODO: est-ce qu'il existe un plus optimisé qu'un autre?
			// TODO: Vérifier noms de variables

			Index spo = this.indexes.get("spo");

			for(int s : spo.getIndex().keySet()) {
				for(int p : spo.getIndex().get(s).keySet()) {
					for(int o : spo.getIndex().get(s).get(p)) {
						allResults.get(sp).get(variables.get(0)).add(s);
						allResults.get(sp).get(variables.get(1)).add(p);
						allResults.get(sp).get(variables.get(2)).add(o);
					}
				}
			}
		}
	}

	public Map<String,ArrayList<Integer>> sortMergeJoin(Integer[][] left, Integer[][] right, String variable){
		//Liste des r�sultats (x : < 1,2...>)
		Map<String,ArrayList<Integer>> result = new HashMap<>();
		result.put(variable,new ArrayList<>());

		int iL = 0; //Parcours du tableau left
		int iR = 0; //Parcours du tableau right

		while(iL < left.length && iR < right.length) {
			if(left[iL][3] == right[iR][3]) {
				result.get(variable).add(left[iL][3]);
				iL++;
				iR++;
			}
			else if(left[iL][3] < right[iR][3]){
				iL++;
			}
			else {
				iR++;
			}
		}

		return result;
	}


	//TODO: vérifier que marche
	public void writeQueryStat(String req, String evalTime, String nbRep, String evalOrder, String selectivity, String jenaComparison){
		if(this.options.getExport_query_stats()) {
			try {
				FileWriter myWriter = new FileWriter(this.options.getOutputPath() + "queryStat.csv");
				myWriter.write(req + "," + evalTime + "," + nbRep + "," + evalOrder + "," + selectivity + "," + jenaComparison);
				myWriter.close();
			} catch (IOException e) {
				System.out.println("Erreur dans l'écriture du résultat de la requête");
				e.printStackTrace();
			}
		}
	}
}
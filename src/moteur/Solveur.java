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

import org.apache.commons.csv.CSVRecord;
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

	private int skipped = 0;

	private Model model;

	/**
	 * indexMap : structure qui permet de savoir quel index utiliser en fonction du pattern que l'on a.
	 *
	 * Exemple :
	 * 12 <=> p et o ont une valeur connue, on utilise donc un index pos
	 * 0 <=> s a une valeur connue, on utilise donc un index spo
	 **/
	private HashMap<String,String> indexMap;

	public Solveur(DataStructure dataStructure, Options options, Statistics stats){
		this.model = ModelFactory.createDefaultModel();
		InputStream in = FileManager.get().open(options.getDataPath());
		model.read(in, null,"RDF/XML");

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
		this.indexMap.put("01","spo");
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
			long startTime_i = System.currentTimeMillis();
			File myObj = new File(queriesPath);
			Scanner myReader = new Scanner(myObj);
			ArrayList<String> queries = new ArrayList<>();
			while (myReader.hasNextLine()) {
				String data = myReader.nextLine();
				if(!data.isEmpty()) {
					queries.add(data);
				}
			}
			long timeSpent_i = System.currentTimeMillis() - startTime_i;
			this.stats.setQueriesReadTime((int)timeSpent_i);
			return queries;

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return null;
	}


	public void supprimerOutput(){
		try{

			if(this.options.getExport_query_stats()){
				File stats = new File (this.options.getOutputPath()+"queryStat.csv");
				if(stats.delete()){
					this.options.diagnostic(stats.getName() + " est supprimé.");
				}else{
					this.options.diagnostic("Opération de suppression echouée");
				}
			}
			/*
			if(this.options.getExport_query_results()){
				File result = new File (this.options.getOutputPath()+"workloadStats.csv");
				if(result.delete()){
					this.options.diagnostic(result.getName() + " est supprimé.");
				}else{
					this.options.diagnostic("Opération de suppression echouée");
				}
			}
			 */
		}
		catch (Exception e){
			//?
		}
	}

	/**
	 * Permet de
	 * @param timeSpent
	 * @return
	 */
	public static int timeSec(long timeSpent){
		return (int)(timeSpent/1000000);
	}

	/**
	 *
	 * @param prec_timeSpent
	 * @throws MalformedQueryException
	 */
	public void traiterQueries(long prec_timeSpent) throws MalformedQueryException {
		supprimerOutput();
		ArrayList<String> queries = buildQueriesAL();
		boolean optim_none = this.options.getOptim_none();

		long warm_time = 0;
		if(options.getWarmPct()!=0){
			long warm_start = System.currentTimeMillis();
			this.warm(options.getWarmPct(),queries,optim_none);
			warm_time = System.currentTimeMillis() - warm_start;
		}

		long shuffle_time = 0;
		if(this.options.getShuffle()) {
			long shuffle_start = System.currentTimeMillis();
			this.options.diagnostic("Shuffle");
			Collections.shuffle(queries);
			shuffle_time = System.currentTimeMillis() - shuffle_start;
		}

		long opt_time = warm_time + shuffle_time;
		long startTime_i = System.currentTimeMillis();

		if(options.getExport_query_stats()) {
			try {
				FileWriter myWriter = new FileWriter(this.options.getOutputPath() + "queryStat.csv",true);
				myWriter.write("Requête" + "," + "Temps d'évaluation (ms)" + "," + "Nombre réponses" + "," + "Ordre évaluation" + "," + "selectivité" + "," + "Comparaison Jena"+"\n");
				myWriter.close();
			} catch (IOException e) {
				this.options.diagnostic("Erreur dans l'écriture du résultat de la requête");
				e.printStackTrace();
			}
		}

		for(String query: queries) {
			this.options.diagnostic("\n-------------------------");
			this.options.diagnostic("Q : "+query);

			if(isValid(query)) {
				ArrayList<String> starVariables = getStarVariables(query);
				if(starVariables.size()==0) {
					if (!optim_none) {
						this.options.diagnostic("- M2b");
						solveOptim(query);
					} else {
						this.options.diagnostic("- M2a");
						solve(query);
					}
				}
				else{
					if(this.options.getStar_queries()) {
						solveStarQuery(query,starVariables);
						this.options.diagnostic("- M1");
					}
					else {
						if (!optim_none) {
							this.options.diagnostic("- M2b");
							solveOptim(query);
						} else {
							this.options.diagnostic("- M2a");
							solve(query);
						}
					}
				}
			}else {
				this.options.diagnostic("\t-> non valide (contient des ressources non présentes dans le dataset)");
			}

		}

		long timeSpent = System.currentTimeMillis() - startTime_i;
		this.stats.setWorkloadEvaluationTime((int)timeSpent);
		this.stats.setQueriesNum(queries.size());
		this.stats.setTotalTime(prec_timeSpent+timeSpent+opt_time);

		/*
		if(options.getJena()) {
			System.out.println("\n---Jena---");
			for(String query: queries) {
				System.out.println(jenaSolve(query));
			}
		}
		 */
		if(options.getExport_query_stats()){
			this.stats.writeStats();
		}

		if(options.getVerbose() || options.getWorkload_time()) {
			System.out.println("\nTemps évaluation du workload : "+ this.stats.getWorkloadEvaluationTime()+"ms");
		}

	}

	//on compare deux version de solveOptim
	public void comparer2solveOptim(int nbIteration) throws MalformedQueryException {
		ArrayList<String> queries = buildQueriesAL();

		long totalTimeA = 0, totalTimeB = 0;

		for(int i = 0; i<nbIteration;i++) {
			System.out.println((i+1)+"/"+nbIteration);
			for(String query : queries) {
				Boolean order = new Random().nextBoolean();

				if(order) {
					long startA = System.nanoTime();
					solveOptim(query);
					totalTimeA += System.nanoTime()-startA;

					long startB = System.nanoTime();
					solveOptim2(query);
					totalTimeB += System.nanoTime()-startB;
				}else {
					long startB = System.nanoTime();
					solveOptim2(query);
					totalTimeB += System.nanoTime()-startB;

					long startA = System.nanoTime();
					solveOptim(query);
					totalTimeA += System.nanoTime()-startA;
				}

			}
		}


		System.out.println("Temps total solveOptim: "+(double)totalTimeA/1000000000+" s");
		System.out.println("Temps total solveOptim2: "+(double)totalTimeB/1000000000+" s");

	}

	public Boolean isValid(String query) throws MalformedQueryException {
		List<StatementPattern> patterns = StatementPatternCollector.process(new SPARQLParser().parseQuery(query, null).getTupleExpr());

		for(StatementPattern sp : patterns) {
			ArrayList<String> constantes = getConstantes(sp);
			for(String c : constantes) {
				if(!this.dictionnaire.exists(c)) {
					return false;
				}
			}
		}

		return true;
	}

	public String encodePattern(StatementPattern sp) {
		String subject = (sp.getSubjectVar().hasValue()?"0":"");
		String predicate = (sp.getPredicateVar().hasValue()?"1":"");
		String object = (sp.getObjectVar().hasValue()?"2":"");

		return subject+predicate+object;
	}

	/**
	 * Obtenir les variables d'un pattern
	 * @param sp
	 * @return
	 * @throws MalformedQueryException
	 */

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

	/**
	 * Obtenir les constantes d'un pattern
	 * @param sp
	 * @return
	 * @throws MalformedQueryException
	 */
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

	public <T> ArrayList<ArrayList<T>> produitCartesien(ArrayList<ArrayList<T>> left,ArrayList<ArrayList<T>> right){
		ArrayList<ArrayList<T>> result = new ArrayList<>();
		result.add(new ArrayList<>());
		ArrayList<T> concat = left.get(0);
		concat.addAll(right.get(0));

		result.get(0).addAll(concat);

		int i = 0;
		int nb = 0;

		for(ArrayList<T> leftLine : left) {
			int j = 0;
			for(ArrayList<T> rightLine : right){
				if(i!=0 && j!=0) {
					nb++;
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
		long startTime = System.currentTimeMillis();

		List<StatementPattern> patterns = StatementPatternCollector.process(new SPARQLParser().parseQuery(req, null).getTupleExpr());

		//1. regrouper les patterns connexes
		HashMap<StatementPattern,Integer> patternConnexes = buildComposantesConnexes(patterns);

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
					if(index.getIndex().get(c1).get(c2) != null)
						allResults.get(sp).put(variables.get(0),index.getIndex().get(c1).get(c2));
				}
				else if(constantes.size() == 1) { // une constante dans le pattern
					int c1 = this.dictionnaire.getValue(constantes.get(0));

					Set<Integer> keys_c1 = index.getIndex().get(c1).keySet();
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

				HashMap<String,ArrayList<Integer>> second = new HashMap<>();
				int idx_second = 0;
				String commonVariable = "";
				while(second.isEmpty()) {
					commonVariable = getCommonVariable(first,toMerge.get(idx_second));
					if(!commonVariable.equals("")) {
						second =toMerge.remove(idx_second);
					}
					idx_second++;
				}
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

		/*while(globalResult.size()>1) {
			ArrayList<ArrayList<String>> left = globalResult.remove(0);
			ArrayList<ArrayList<String>> right = globalResult.remove(0);

			globalResult.add(produitCartesien(left,right));
		}*/

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

		long timeSpent = System.currentTimeMillis() - startTime ;
		this.options.diagnostic("TEMPS= "+ timeSpent + "ms");

		String CSVResults = "";
		for (ArrayList<String> ligne : queryResult) {
			for(int i = 0 ; i<ligne.size();i++) {
				if(indicesVariablesProjetees.contains(i)) {
					CSVResults += (ligne.get(i) + ",");
				}
			}
			CSVResults=CSVResults.substring(0,CSVResults.length()-1);
			CSVResults+="\n";
		}
		if(timeSpent<1) {
			timeSpent=1;
		}
		//TODO: vérifier evalOrder
		this.traiterOptions(req,String.valueOf(queryResult.size()-1),"QueryOrder","NON_DISPONIBLE", CSVResults, String.valueOf(timeSpent));
	}

	public HashMap<StatementPattern,Integer> buildComposantesConnexes(List<StatementPattern> patterns) throws MalformedQueryException{
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

		return patternConnexes;
	}



	/**
	 * Méthode M2b (optimisée)
	 * @param req
	 * @throws MalformedQueryException
	 */

	public void solveOptim(String req) throws MalformedQueryException {

		//TODO: merge join
		long startTime = System.currentTimeMillis();
		String outputPath = this.options.getOutputPath();
		this.options.diagnostic("\nRequete: "+req+"\n");

		long resultatsPartielsStart = System.currentTimeMillis();

		SPARQLParser sparqlParser = new SPARQLParser();
		ParsedQuery pq = sparqlParser.parseQuery(req, null);
		List<StatementPattern> patterns = StatementPatternCollector.process(pq.getTupleExpr());

		HashMap<StatementPattern, HashMap<String,ArrayList<Integer>>> allResults = new HashMap<>();

		this.options.diagnostic("-- Lecture de chaque pattern par sélectivité croissante"+"\n");

		//1. Sélectionner les pattern par valeur de sélectivité croissante

		String selectivityTxt = "";
		HashMap<StatementPattern, Double> selectivities = new HashMap<>();
		for(StatementPattern sp: patterns) {
			double slct = selectivity(sp);
			selectivities.put(sp,slct);
			selectivityTxt+=sp.toString().replace("\n","").replace(",", "")+" => "+slct+" | ";
		}

		ArrayList<StatementPattern> alreadySolved = new ArrayList<>();

		HashMap<String, ArrayList<Integer>> resultsPerVariable = new HashMap<>();

		//2. résultat de chaque pattern
		HashMap<StatementPattern,HashMap<String,ArrayList<Integer>>> resultsPerPattern = new HashMap<>();

		while(alreadySolved.size()<patterns.size()) {
			//TODO: vérifier que ça marche
			StatementPattern spCurrent = minSelectivity(alreadySolved, selectivities);

			resultsPerPattern.put(spCurrent,getResult(spCurrent, resultsPerVariable));
		}


		if(this.options.getDiagnostic()) {
			this.options.diagnostic("[Calcul tuples candidats de chaque pattern : "+((System.currentTimeMillis()-resultatsPartielsStart))+"ms]");
		}

		long composantesConnexesStart = System.currentTimeMillis();

		HashMap<StatementPattern,Integer> patternConnexes = buildComposantesConnexes(patterns);

		if(this.options.getDiagnostic()) {
			this.options.diagnostic("[Calcul des composantes connexes : "+((System.currentTimeMillis()-composantesConnexesStart))+"ms]");
		}

		long mergeStart = System.currentTimeMillis();

		ArrayList<ArrayList<ArrayList<String>>> mergedComponents = new ArrayList<>();
		ArrayList<Integer> composantesConnues = new ArrayList<>();
		for(int composante : patternConnexes.values()) {
			if(!composantesConnues.contains(composante)) {
				composantesConnues.add(composante);
				ArrayList<HashMap<String,ArrayList<Integer>>> toMerge = new ArrayList<>();

				//on ajoute les patterns de même composante à toMerge
				for(StatementPattern sp : patternConnexes.keySet()) {
					if(patternConnexes.get(sp).equals(composante)) {
						toMerge.add(resultsPerPattern.get(sp));
					}
				}

				while(toMerge.size()>1) {
					HashMap<String,ArrayList<Integer>> first = toMerge.remove(0);

					HashMap<String,ArrayList<Integer>> second = new HashMap<>();
					int idx_second = 0;
					String commonVariable = "";
					while(second.isEmpty()) {
						commonVariable = getCommonVariable(first,toMerge.get(idx_second));
						if(!commonVariable.equals("")) {
							second =toMerge.remove(idx_second);
						}
						idx_second++;
					}

					toMerge.add(this.mergeGeneral(first, second, commonVariable));
				}

				//on reformate en matrice de String
				ArrayList<ArrayList<String>> merged = new ArrayList<>();

				//taille de la première colonne de la fusion des patterns courants
				int size = toMerge.get(0).get(toMerge.get(0).keySet().iterator().next()).size();
				//initialisation de chaque ligne de la matrice résultat
				for(int i = 0; i<= size;i++) {
					merged.add(new ArrayList());
				}

				for(String variable : toMerge.get(0).keySet()) {
					int currentLine = 0;
					merged.get(currentLine).add(variable);

					for(int value : toMerge.get(0).get(variable)) {
						currentLine++;
						merged.get(currentLine).add(this.dictionnaire.getValue(value));
					}
				}

				mergedComponents.add(merged);
			}
		}

		if(this.options.getDiagnostic()) {
			this.options.diagnostic("[Merge des résultats partiels : "+(System.currentTimeMillis()-mergeStart)+"ms]");
		}

		long produitCartesienStart = System.currentTimeMillis();
		//TOREMOVE
		//System.out.println("Nombre de composantes à merge : "+mergedComponents.size()+"\n");
		while(mergedComponents.size()>1) {
			ArrayList<ArrayList<String>> left = mergedComponents.remove(0);
			ArrayList<ArrayList<String>> right = mergedComponents.remove(0);


			ArrayList<ArrayList<String>> resultMerge = produitCartesien(left,right);

			//System.out.println("\tMERGE gauche("+left.size()+") , droite("+right.size()+"");
			//System.out.println("\t --> résultat de taille "+resultMerge.size()+"\n");
			mergedComponents.add(resultMerge);
		}

		ArrayList<ArrayList<String>> queryResult = mergedComponents.get(0);

		if(this.options.getDiagnostic()) {
			System.out.println("[Produit cartésien de chaque composante : "+((System.currentTimeMillis()-produitCartesienStart)/1000000)+"ms]");
		}

		long formattageResultatStart = System.currentTimeMillis();


		//Cette structure nous permet d'avoir uniquement les variables à retourner (celles dans le SELECT)
		ArrayList<String> varToReturn = new ArrayList<>();
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

		long timeSpent = System.currentTimeMillis() - startTime;
		int tS =((int)timeSpent);
		this.stats.setOptimizationTime(tS==0?1:tS); //TODO à changer


		if(this.options.getDiagnostic()) {
			this.options.diagnostic("[Formattage des résultats : "+((System.currentTimeMillis()-formattageResultatStart))+"ms]");
		}


		this.options.diagnostic(">"+(queryResult.size()-1)+" résultats");
		//}

		//sortMergeJoin();

		//On construit une chaine de caractères sous format CSV de nos résultats
		// Afin de pouvoir le comparer à celui de Jena
		String CSVResults="";
		if(this.options.getExport_query_results() || this.options.getJena()) {
			for (ArrayList<String> ligne : queryResult) {
				for(int i = 0 ; i<ligne.size();i++) {
					if(indicesVariablesProjetees.contains(i)) {
						CSVResults+=ligne.get(i)+",";
					}
				}
				CSVResults=CSVResults.substring(0,CSVResults.length()-1);
				CSVResults+="\n";

			}
		}
		if(tS<1) {
			tS=1;
		}
		traiterOptions(req, String.valueOf(queryResult.size()-1),"selectivity", selectivityTxt, CSVResults, String.valueOf(tS));

	}

	public void solveOptim2(String req) throws MalformedQueryException {

		//TODO: merge join
		long startTime = System.nanoTime();
		String outputPath = this.options.getOutputPath();
		this.options.diagnostic("\nRequete: "+req+"\n");

		long resultatsPartielsStart = System.nanoTime();

		SPARQLParser sparqlParser = new SPARQLParser();
		ParsedQuery pq = sparqlParser.parseQuery(req, null);
		List<StatementPattern> patterns = StatementPatternCollector.process(pq.getTupleExpr());

		HashMap<StatementPattern, HashMap<String,ArrayList<Integer>>> allResults = new HashMap<>();

		this.options.diagnostic("-- Lecture de chaque pattern par sélectivité croissante"+"\n");

		//1. Sélectionner les pattern par valeur de sélectivité croissante

		String selectivityTxt = "";
		HashMap<StatementPattern, Double> selectivities = new HashMap<>();
		for(StatementPattern sp: patterns) {
			double slct = selectivity(sp);
			selectivities.put(sp,slct);
			selectivityTxt+=sp.toString()+" "+slct+"\n";
		}

		ArrayList<StatementPattern> alreadySolved = new ArrayList<>();

		HashMap<String, ArrayList<Integer>> resultsPerVariable = new HashMap<>();

		//2. résultat de chaque pattern
		HashMap<StatementPattern,HashMap<String,ArrayList<Integer>>> resultsPerPattern = new HashMap<>();

		while(alreadySolved.size()<patterns.size()) {
			//TODO: vérifier que ça marche
			StatementPattern spCurrent = minSelectivity(alreadySolved, selectivities);

			resultsPerPattern.put(spCurrent,getResult2(spCurrent, resultsPerVariable));
		}


		if(this.options.getDiagnostic()) {
			this.options.diagnostic("[Calcul tuples candidats de chaque pattern : "+((System.nanoTime()-resultatsPartielsStart)/1000000)+"ms]");
		}

		long composantesConnexesStart = System.nanoTime();

		HashMap<StatementPattern,Integer> patternConnexes = buildComposantesConnexes(patterns);

		if(this.options.getDiagnostic()) {
			this.options.diagnostic("[Calcul des composantes connexes : "+((System.nanoTime()-composantesConnexesStart)/1000000)+"ms]");
		}

		long mergeStart = System.nanoTime();

		ArrayList<ArrayList<ArrayList<String>>> mergedComponents = new ArrayList<>();
		for(int composante : patternConnexes.values()) {
			ArrayList<HashMap<String,ArrayList<Integer>>> toMerge = new ArrayList<>();

			//on ajoute les patterns de même composante à toMerge
			for(StatementPattern sp : patternConnexes.keySet()) {
				if(patternConnexes.get(sp).equals(composante)) {
					toMerge.add(resultsPerPattern.get(sp));
				}
			}

			while(toMerge.size()>1) {
				HashMap<String,ArrayList<Integer>> first = toMerge.remove(0);

				HashMap<String,ArrayList<Integer>> second = new HashMap<>();
				int idx_second = 0;
				String commonVariable = "";
				while(second.isEmpty()) {
					commonVariable = getCommonVariable(first,toMerge.get(idx_second));
					if(!commonVariable.equals("")) {
						second =toMerge.remove(idx_second);
					}
					idx_second++;
				}

				toMerge.add(this.mergeGeneral(first, second, commonVariable));
			}

			//on reformate en matrice de String
			ArrayList<ArrayList<String>> merged = new ArrayList<>();

			//taille de la première colonne de la fusion des patterns courants
			int size = toMerge.get(0).get(toMerge.get(0).keySet().iterator().next()).size();
			//initialisation de chaque ligne de la matrice résultat
			for(int i = 0; i<= size;i++) {
				merged.add(new ArrayList());
			}

			for(String variable : toMerge.get(0).keySet()) {
				int currentLine = 0;
				merged.get(currentLine).add(variable);

				for(int value : toMerge.get(0).get(variable)) {
					currentLine++;
					merged.get(currentLine).add(this.dictionnaire.getValue(value));
				}
			}

			mergedComponents.add(merged);
		}

		if(this.options.getDiagnostic()) {
			this.options.diagnostic("[Merge des résultats partiels : "+((System.nanoTime()-mergeStart)/1000000)+"ms]");
		}

		long produitCartesienStart = System.nanoTime();

		while(mergedComponents.size()>1) {
			ArrayList<ArrayList<String>> left = mergedComponents.remove(0);
			ArrayList<ArrayList<String>> right = mergedComponents.remove(0);

			mergedComponents.add(produitCartesien(left,right));
		}

		ArrayList<ArrayList<String>> queryResult = mergedComponents.get(0);

		if(this.options.getDiagnostic()) {
			System.out.println("[Produit cartésien de chaque composante : "+((System.nanoTime()-produitCartesienStart)/1000000)+"ms]");
		}

		long formattageResultatStart = System.nanoTime();


		//Cette structure nous permet d'avoir uniquement les variables à retourner (celles dans le SELECT)
		ArrayList<String> varToReturn = new ArrayList<>();
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

		long timeSpent = System.nanoTime();
		timeSpent = (timeSpent-startTime);
		int tS =((int)timeSpent/1000000);
		this.stats.setOptimizationTime(tS==0?1:tS); //TODO à changer

		if(this.options.getDiagnostic()) {
			this.options.diagnostic("[Formattage des résultats : "+((System.nanoTime()-formattageResultatStart)/1000000)+"ms]");
		}


		this.options.diagnostic(">"+(queryResult.size()-1)+" résultats");
		//}

		//sortMergeJoin();

		//On construit une chaine de caractères sous format CSV de nos résultats
		// Afin de pouvoir le comparer à celui de Jena
		String CSVResults="";
		if(this.options.getExport_query_results() || this.options.getJena()) {
			for (ArrayList<String> ligne : queryResult) {
				for(int i = 0 ; i<ligne.size();i++) {
					if(indicesVariablesProjetees.contains(i)) {
						CSVResults+=ligne.get(i)+",";
					}
				}
				CSVResults=CSVResults.substring(0,CSVResults.length()-1);
				CSVResults+="\n";
			}
		}
		if(tS<1) {
			tS=1;
		}
		traiterOptions(req, String.valueOf(queryResult.size()),"selectivity", selectivityTxt, CSVResults, String.valueOf(tS));

	}

	//Avoir la meme taille pour les AL dans results
	public HashMap<String, ArrayList<Integer>> getResult(StatementPattern sp, HashMap<String, ArrayList<Integer>> memory) throws MalformedQueryException {
		HashMap<String, ArrayList<Integer>> res = new HashMap<>();
		ArrayList<String> allVariable = new ArrayList<>();
		//on encode le pattern pour savoir quel index utiliser

		String indexType = this.indexMap.get(this.encodePattern(sp));
		Index index = this.indexes.get(indexType);

		this.options.diagnostic("  Index utilisé: " + indexType+"\n");

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

			if(index.getIndex().get(c1).containsKey(c2)) {
				if(!memory.containsKey(variables.get(0))) {
					memory.put(variables.get(0), new ArrayList<>());
					memory.put(variables.get(0), new ArrayList<>(index.getIndex().get(c1).get(c2)));
					res.put(variables.get(0), index.getIndex().get(c1).get(c2));
				}else {
					if(memory.get(variables.get(0)).contains(index.getIndex().get(c1).get(c2))) {
						res.put(variables.get(0), index.getIndex().get(c1).get(c2));
					}
				}
			}
		}
		else if(constantes.size() == 1) { // une constante dans le pattern

			int c1 = this.dictionnaire.getValue(constantes.get(0));

			Boolean firstTime_v1 = !memory.containsKey(variables.get(0));
			Boolean firstTime_v2 = !memory.containsKey(variables.get(1));

			if(firstTime_v1) {
				memory.put(variables.get(0), new ArrayList<>());
			}

			if(firstTime_v2) {
				memory.put(variables.get(1), new ArrayList<>());
			}

			Set<Integer> keys_c1 = index.getIndex().get(c1).keySet();
			for (int i : keys_c1) {

				if(firstTime_v1) {
					memory.get(variables.get(0)).add(i);
				}

				if(memory.get(variables.get(0)).contains(i)) {
					for(int j : index.getIndex().get(c1).get(i)) {

						if(firstTime_v2) {
							memory.get(variables.get(1)).add(j);
						}

						if(memory.get(variables.get(1)).contains(j)) {
							res.get(variables.get(0)).add(i);
							res.get(variables.get(1)).add(j);
						}
					}
				}

			}
		}
		else {
			//TODO : prendre en compte memory ? (Cas où il y a 3 variables)
			Index spo = this.indexes.get("spo");

			for(int s : spo.getIndex().keySet()) {
				for(int p : spo.getIndex().get(s).keySet()) {
					for(int o : spo.getIndex().get(s).get(p)) {
						res.get(variables.get(0)).add(s);
						res.get(variables.get(1)).add(p);
						res.get(variables.get(2)).add(o);
					}
				}
			}
		}
		return res;
	}

	public HashMap<String, ArrayList<Integer>> getResult2(StatementPattern sp, HashMap<String, ArrayList<Integer>> memory) throws MalformedQueryException {
		HashMap<String, ArrayList<Integer>> res = new HashMap<>();
		ArrayList<String> allVariable = new ArrayList<>();
		//on encode le pattern pour savoir quel index utiliser

		String indexType = this.indexMap.get(this.encodePattern(sp));
		Index index = this.indexes.get(indexType);

		this.options.diagnostic("  Index utilisé: " + indexType+"\n");

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

			if(!memory.containsKey(variables.get(0))) {
				memory.put(variables.get(0), new ArrayList<>(index.getIndex().get(c1).get(c2)));
				res.put(variables.get(0), index.getIndex().get(c1).get(c2));
			}else {
				for(Integer mem : memory.get(variables.get(0))) {
					if(index.getIndex().get(c1).get(c2).contains(mem)) {
						res.get(variables.get(0)).add(mem);
					}else {
						memory.get(variables.get(0)).remove(mem);
					}
				}
			}
		}
		else if(constantes.size() == 1) { // une constante dans le pattern

			int c1 = this.dictionnaire.getValue(constantes.get(0));

			Boolean firstTime_v1 = !memory.containsKey(variables.get(0));
			Boolean firstTime_v2 = !memory.containsKey(variables.get(1));

			if(firstTime_v1) {
				memory.put(variables.get(0), new ArrayList<>());
			}

			if(firstTime_v2) {
				memory.put(variables.get(1), new ArrayList<>());
			}

			Set<Integer> keys_c1 = index.getIndex().get(c1).keySet();
			for (int i : keys_c1) {

				if(firstTime_v1) {
					memory.get(variables.get(0)).add(i);
				}

				if(memory.get(variables.get(0)).contains(i)) {
					for(int j : index.getIndex().get(c1).get(i)) {

						if(firstTime_v2) {
							memory.get(variables.get(1)).add(j);
						}

						if(memory.get(variables.get(1)).contains(j)) {
							res.get(variables.get(0)).add(i);
							res.get(variables.get(1)).add(j);
						}
					}
				}

			}
		}
		else {
			//TODO : prendre en compte memory ? (Cas où il y a 3 variables)
			Index spo = this.indexes.get("spo");

			for(int s : spo.getIndex().keySet()) {
				for(int p : spo.getIndex().get(s).keySet()) {
					for(int o : spo.getIndex().get(s).get(p)) {
						res.get(variables.get(0)).add(s);
						res.get(variables.get(1)).add(p);
						res.get(variables.get(2)).add(o);
					}
				}
			}
		}
		return res;
	}

	/**
	 * Méthode M1 évaluant uniquement les requetes en étoiles
	 * @param req
	 * @param starVariables
	 * @throws MalformedQueryException
	 */
	public void solveStarQuery(String req, ArrayList<String> starVariables) throws MalformedQueryException {
		long startTime = System.currentTimeMillis();
		String outputPath = this.options.getOutputPath();
		this.options.diagnostic("\nRequete: "+req+"\n");

		//Utilisation d'une instance de SPARLQLParser
		SPARQLParser sparqlParser = new SPARQLParser();
		ParsedQuery pq = sparqlParser.parseQuery(req, null);
		List<StatementPattern> patterns = StatementPatternCollector.process(pq.getTupleExpr());

		//pour chaque pattern, on a les valeurs possible de toutes ses variables
		HashMap<StatementPattern, HashMap<String,ArrayList<Integer>>> resultsPerPattern = new HashMap<>();

		this.options.diagnostic("-- Lecture de chaque pattern"+"\n");

		//on prend la première variable commune à tous les patterns
		String variableJointure = starVariables.get(0);
		ArrayList<Integer> valeursPossiblesVJ = new ArrayList<>();
		ArrayList<String> allVariable = new ArrayList<>();
		Boolean first_pattern = true;
		for(StatementPattern sp: patterns) {
			resultsPerPattern.put(sp,new HashMap<>());

			//on encode le pattern pour savoir quel index utiliser
			String indexType = this.indexMap.get(this.encodePattern(sp));
			Index index = this.indexes.get(indexType);

			this.options.diagnostic("  Index utilisé: " + indexType+"\n");

			ArrayList<String> variables = getVariables(sp);
			ArrayList<String> constantes = getConstantes(sp);

			//on ajoute les variables dans la hashmap du pattern actuel
			for(String v: variables) {
				if(!allVariable.contains(v)) {
					allVariable.add(v);
				}
				resultsPerPattern.get(sp).put(v,new ArrayList<>());
			}

			if(constantes.size() == 2) { // deux constantes dans le pattern
				int c1 = this.dictionnaire.getValue(constantes.get(0));
				int c2 = this.dictionnaire.getValue(constantes.get(1));

				if(index.getIndex().get(c1).containsKey(c2)) {
					ArrayList<Integer> results = new ArrayList<>();
					for(Integer r : index.getIndex().get(c1).get(c2)) {
						if(first_pattern || valeursPossiblesVJ.contains(r)) {
							results.add(r);

							if(first_pattern)
								valeursPossiblesVJ.add(r);
						}
					}
					valeursPossiblesVJ.retainAll(results);
					resultsPerPattern.get(sp).put(variables.get(0),results);
				}

			}
			else if(constantes.size() == 1) { // une constante dans le pattern
				// j'ai pso
				int c1 = this.dictionnaire.getValue(constantes.get(0));

				//la jointure se fait sur s ou o
				Boolean s = variableJointure.equals(variables.get(0));
				Boolean o = !s;

				ArrayList<Integer> results = new ArrayList<>();
				if(s) {
					for(Integer subject : index.getIndex().get(c1).keySet()) {
						if(first_pattern || valeursPossiblesVJ.contains(subject)) {
							for(Integer object : index.getIndex().get(c1).get(subject)) {
								results.add(subject);
								resultsPerPattern.get(sp).get(variables.get(0)).add(subject);
								resultsPerPattern.get(sp).get(variables.get(1)).add(object);

								if(first_pattern) {
									valeursPossiblesVJ.add(subject);
								}
							}
						}
					}
				}else { //o forcément vrai
					for(Integer subject : index.getIndex().get(c1).keySet()) {
						for(Integer object : index.getIndex().get(c1).get(subject)) {
							if(first_pattern || valeursPossiblesVJ.contains(object)) {
								results.add(object);
								resultsPerPattern.get(sp).get(variables.get(0)).add(subject);
								resultsPerPattern.get(sp).get(variables.get(1)).add(object);

								if(first_pattern) {
									valeursPossiblesVJ.add(object);
								}
							}

						}

					}
				}

				//intersection
				valeursPossiblesVJ.retainAll(results);

			}
			else {
				//Cas où il y a 3 variables
				//Choix de base SPO
				//TODO: normalement n'arrive jamais alors on enlève ?
				// TODO: est-ce qu'il existe un plus optimisé qu'un autre?

				Index spo = this.indexes.get("spo");

				for(int s : spo.getIndex().keySet()) {
					for(int p : spo.getIndex().get(s).keySet()) {
						for(int o : spo.getIndex().get(s).get(p)) {
							resultsPerPattern.get(sp).get(variables.get(0)).add(s);
							resultsPerPattern.get(sp).get(variables.get(1)).add(p);
							resultsPerPattern.get(sp).get(variables.get(2)).add(o);
						}
					}
				}
			}
			first_pattern = false;
		}

		//avoir la variable qui apparait dans chaque pattern

		//Dans allResults on a les résultats de chaque variable pour chaque pattern
		//Ici on fait pour chaque variable l'intersection des résultats
		//Ca nous permet d'obtenir pour chaque variable l'ensemble des résultas
		ArrayList<ArrayList<String>> results = this.extractResults(starVariables.get(0), resultsPerPattern);

		//Cette structure nous permet d'avoir uniquement les variables à retourner (celles dans le SELECT)
		ArrayList<String> varToReturn = new ArrayList<>();
		this.options.diagnostic("-- Résultat de la requete"+"\n");
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

		long timeSpent = System.currentTimeMillis() - startTime;
		int tS = ((int)timeSpent);

		/*
		this.options.diagnostic("--------Résultats--------");
		for (ArrayList<String> ligne : results) {
			for(int i = 0 ; i<ligne.size();i++) {
				if(indicesVariablesProjetees.contains(i)) {
					this.options.diagnostic(ligne.get(i)+", ");
				}
			}
			this.options.diagnostic("\n");
		}
		 */

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

		/*
		String jenaString = "NON_DISPONIBLE";
		if(this.options.getJena()) {
			Boolean jena = this.jenaComparison(req, CSVResults);
			if(jena){
				jenaString = "true";
			}
			else{
				jenaString="false";
			}
		}

		if(this.options.getExport_query_results()) {
			try {
				FileWriter myWriter = new FileWriter(outputPath + "queryResult.csv",true);
				myWriter.write(req+"\n"+CSVResults); //TODO: à vérifier
				myWriter.close();
			} catch (IOException e) {
				this.options.diagnostic("Erreur dans l'écriture du résultat de la requête");
				e.printStackTrace();
			}
		}

		writeQueryStat(req, tS.toString(),"","","",jenaString); //TODO
		 */
		if(tS<1) {
			tS=1;
		}
		traiterOptions(req, String.valueOf(results.size()-1), "QueryOrder","NON_DISPONIBLE",CSVResults,String.valueOf(tS));
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

	public boolean jenaComparison(String req, String ourResultCSV){

		ArrayList<String> jena = new ArrayList<>(Arrays.asList(jenaSolve(req).split("\n")));
		ArrayList<String> ourResult = new ArrayList<>(Arrays.asList(ourResultCSV.split("\n")));

		//System.out.println(ourResultCSV);
		//System.out.println(jenaSolve(req));
		//System.out.println("--------");
		//System.out.println(ourResultCSV);

		if(this.options.getCheckJena()){
			System.out.println("- JENA "+req);
			System.out.println(jena.get(0));
			System.out.println(ourResult.get(0));
		}

		Boolean reordered=false;
		HashMap<Integer, Integer> getOrder = new HashMap<>();
		int jL= jena.get(0).length()-1;
		Integer tailleMax=Integer.parseInt(jena.get(0).substring(jL-1,jL))+1;
		//S'ils n'ont pas le meme nombre
		if(!jena.get(0).substring(0,jL).equals(ourResult.get(0))) {
			reordered=true;
			//Cela permet de faire correspondre les ordres
			List<String> oR = Arrays.asList(ourResult.get(0).split(","));
			for (Integer i = 0; i < tailleMax; i++) {
				if(oR.size()>i) {
					String o = oR.get(i).substring(1, 2);
					getOrder.put(Integer.parseInt(o), i);
				}
			}
		}
		jena.remove(0);
		ourResult.remove(0);

		//s'il est complet
		if(jena.size() == ourResult.size()) {
			// on teste la corection
			for (String res : ourResult) {
				res=res.substring(0,res.length());
				if(reordered) {
					res = reorder(res,tailleMax, getOrder);
				}

				jena.replaceAll( e -> e.replaceAll("\n", ""));
				jena.replaceAll( e -> e.replaceAll("\r", ""));

				if (!jena.contains(res.replaceAll("\n", "").replace("\r", ""))) {
					if(this.options.getCheckJena()){
						System.out.println("  Pas correct");
						System.out.println(res);
					}
					return false;
					//pas correct
				}
			}
			//if(this.options.getCheckJena()){
			//	System.out.println("  Correct et complet");
			//}
			return true;
		}
		else {
			System.out.println("Jena : "+jena.size());
			System.out.println(jena.toString());
			System.out.println("Us : "+ourResult.size());
			if(this.options.getCheckJena()){
				System.out.println("  Pas complet");
			}
			return false;
		}
	}

	public String reorder(String s, int tM, HashMap<Integer,Integer>hm){
		String res="";
		String[] tab = s.split(",");
		for(Integer i=0;i<tM;i++){
			if(hm.keySet().contains(i)) {
				res += tab[hm.get(i)]+",";
			}
		}
		return res.substring(0,res.length()-1);
	}

	/**
	 * Renvoie le CSV du résultat calculé par Jena d'une requete donnée
	 * @param req
	 * @return
	 */
	public String jenaSolve(String req){
		Query query = QueryFactory.create(req);
		QueryExecution qexec = QueryExecutionFactory.create(query,this.model);
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

	public void warm(float pct, ArrayList<String> queries, boolean optim_none) throws MalformedQueryException {
		this.options.diagnostic("Warm "+pct);
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
			//TODO: à facto ?
			if(starVariables.size()==0) {
				if (!optim_none) {
					this.options.diagnostic("- M2b");
					solveOptim(queries.get(c));
				} else {
					this.options.diagnostic("- M2a");
					solve(queries.get(c));
				}
			}
			else{
				this.options.diagnostic("- M1");
				solveStarQuery(queries.get(c),starVariables);
			}
		}
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

	//TODO: factoriser?
	/**
	 * Calcule le critère de sélectivité pour chaque pattern
	 * @param sp
	 * @return
	 */
	public double selectivity(StatementPattern sp) {
		double selectivity = 0;
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

		double totalValuesNumber = this.indexes.get("spo").getValuesNumber().doubleValue();

		if (constantes.size() == 2) {
			int c1 = returnConvertCst(i1,s,p,o);
			int c2 = returnConvertCst(i2,s,p,o);

			if(this.indexes.get(indexType).getIndex2().get(c1).keySet().contains(c2))
				selectivity = this.indexes.get(indexType).getIndex2().get(c1).get(c2).doubleValue()/totalValuesNumber;
		}

		if (constantes.size() == 1) {
			//System.out.println(this.indexes.get(indexType).getIndex1().get(returnConvertCst(i1,s,p,o)));
			return this.indexes.get(indexType).getIndex1().get(returnConvertCst(i1,s,p,o)).doubleValue()/totalValuesNumber;
		}

		return selectivity;
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

	//TODO: vérifier que marche
	public void writeQueryStat(String req, String evalTime, String nbRep, String evalOrder, String selectivity, String jenaComparison){
		if(this.options.getExport_query_stats()) {
			try {
				FileWriter myWriter = new FileWriter(this.options.getOutputPath() + "queryStat.csv",true);
				myWriter.write(req + "," + evalTime + "," + nbRep + "," + evalOrder + "," + selectivity + "," + jenaComparison+"\n");
				myWriter.close();
			} catch (IOException e) {
				this.options.diagnostic("Erreur dans l'écriture du résultat de la requête");
				e.printStackTrace();
			}
		}
	}

	public void traiterOptions(String req, String nbRep, String evalOrder, String selectivity,String CSVResults, String tS) {
		String outputPath = this.options.getOutputPath();
		if(options.getVerbose()) {
			System.out.println("\n - Temps requete : "+ tS+"ms");
		}

		String jenaString = "NON_DISPONIBLE";
		if(this.options.getJena()) {
			Boolean jena = this.jenaComparison(req, CSVResults);
			if(jena){
				jenaString = "true";
			}
			else{
				jenaString="false";
			}
			if(this.options.diagnostic){
				System.out.println("JENA: "+jenaString);
			}
		}

		if(this.options.getExport_query_results()) {
			try {
				FileWriter myWriter = new FileWriter(outputPath + "queryResult.csv",true);
				myWriter.write(req+"\n"+CSVResults); //TODO: à vérifier
				myWriter.close();
			} catch (IOException e) {
				this.options.diagnostic("Erreur dans l'écriture du résultat de la requête");
				e.printStackTrace();
			}
		}

		if(this.options.getExport_query_stats()) {
			writeQueryStat(req, tS, nbRep, evalOrder,selectivity,jenaString);//TODO
		}

	}


	//TODO: à supprimer les 3 fonctions suivantes ?

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

	public void displayHashMap(HashMap<String,ArrayList<Integer>> map) {
		this.options.diagnostic("-----------------------");
		for(String clef : map.keySet()) {
			int maxValue = 10;
			this.options.diagnostic(clef);
			for(int value : map.get(clef)) {
				if(maxValue>0) {
					this.options.diagnostic("\t=>"+dictionnaire.getValue(value));
					maxValue--;
				}
			}
			this.options.diagnostic("\n");

		}
	}

	public String printRes(ArrayList<String> tab){
		String res = "";
		for(String i: tab){
			res+=i+" ";
		}
		return res;
	}

	public void checkReq(String toCompare){
		System.out.println(this.dictionnaire.getValue(toCompare));
	}

	public Options getOptions(){
		return this.options;
	}
}



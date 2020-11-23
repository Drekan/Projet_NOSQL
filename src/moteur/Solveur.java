package moteur;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;

/*
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.util.FileManager;
 */
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

    /*
     * indexMap : structure qui permet de savoir quel index utiliser en fonction du pattern que l'on a.
     *
     * Exemple :
     * 12 <=> p et o ont une valeur connue, on utilise donc un index pos
     * 0 <=> s a une valeur connue, on utilise donc un index spo
     */
    private HashMap<String,String> indexMap;


    public Solveur(DataStructure dataStructure, Options options, Statistics stats){
        this.indexMap = new HashMap<>();
        this.options = options;
        this.dictionnaire = dataStructure.getDico();
        this.indexes = dataStructure.getIndexes();
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

        this.stats = stats;
    }

    public Statistics getStats(){
        return  this.stats;
    }

    //Appelé si shuffle ou warm
    //Adapter pour plusieurs fichiers
    public ArrayList<String> buildQueriesAL() {
        String queriesPath = this.options.getQueriesPath();
        try {
            long startTime_i = System.nanoTime();
            File myObj = new File(queriesPath);
            Scanner myReader = new Scanner(myObj);
            ArrayList<String> queries = new ArrayList<>();
            while (myReader.hasNextLine()) {
                String data = myReader.nextLine();
                queries.add(data);
            }
            long timeSpent_i = System.nanoTime() - startTime_i;
            this.stats.setQueriesReadTime((int)timeSpent_i/1000000);
            return queries;

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    //TODO file not found ?
    //TODO: à facto ?
    public void traiterQueries(long timeSpent) throws MalformedQueryException {
        ArrayList<String> queries = buildQueriesAL();
        boolean optim_none = this.options.getOptim_none();

        if(options.getWarmPct()!=0){
            //TODO pbm
            //this.warm(options.getWarmPct(),queries,optim_none);
        }

        if(this.options.getShuffle()) {
            //TODO: affichage ?
            Collections.shuffle(queries);
        }

        long startTime_i = System.nanoTime();

        for(String query: queries) {
            System.out.println("CC");
            //TODO: vérifier que la req  est en étoile
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
        this.stats.setTotalTime((int)timeSpent_i/1000000+(int)timeSpent/1000000); //TODO: mettre dans des variables pour que ce soit joli ?

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


    public void solve(String req) throws MalformedQueryException{
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
    	
    	//2. Merge chaque composante (donc trouver une colonne sur laquelle on peut merge)
    	//3. Faire un produit cartésien de chaque table : c'est les résultats
    }

    //Solve optimisé
    public void solveOptim(String req) throws MalformedQueryException {
        //TODO: merge join
        String outputPath = this.options.getOutputPath();
        String verbose ="";
        verbose+="\nRequete: "+req+"\n";

        //Utilisation d'une instance de SPARLQLParser
        SPARQLParser sparqlParser = new SPARQLParser();
        ParsedQuery pq = sparqlParser.parseQuery(req, null);
        List<StatementPattern> patterns = StatementPatternCollector.process(pq.getTupleExpr());

        //TODO: utile dans solveOptim ?!
        HashMap<StatementPattern, HashMap<String,ArrayList<Integer>>> allResults = new HashMap<>();
        // Cette structure permet d'obtenir tous les résultats pour toutes les variables pour tous les patterns
        //Clé = la valeur recherchée
        //Valeurs = un ensemble d'ensembles de résultats pour chaque pattern

        verbose+="-- Lecture de chaque pattern"+"\n";

        HashMap<StatementPattern, Double> selectivities = new HashMap<>();
        for(StatementPattern sp: patterns) {
            selectivities.put(sp,selectivity(sp));
        }
        ArrayList<StatementPattern> alreadySolved = new ArrayList<>();
        ArrayList<String> allVariable = new ArrayList<>();
        while(alreadySolved.size()<patterns.size()) {
            StatementPattern spCurrent = minSelectivity(alreadySolved, selectivities);
            //test(spCurrent, allResults, verbose, allVariable, starVariable);
        }
        //TODO: optimization time
    }

    public StatementPattern minSelectivity(ArrayList<StatementPattern> alreadySolved, HashMap<StatementPattern, Double> selectivities){
        StatementPattern minSp = new StatementPattern();
        long minS = 1;
        for(StatementPattern sp: selectivities.keySet()){
            if(selectivities.get(sp)<minS && !alreadySolved.contains(sp)){
                alreadySolved.add(sp);
                return minSp;
            }
        }
        return minSp;
    }


    //Méthode principale de la classe
    // TODO : optimiser les paramètres
    public void solveStarQuery(String req, ArrayList<String> starVariables) throws MalformedQueryException {
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


        for (String s : varToReturn) {
            if(this.options.getExport_query_results()) {
                try {
                    FileWriter myWriter = new FileWriter(outputPath + "queryResult.csv");

                    //TODO : QUELLE STRUCTURRRE POUR LE CSV ???
                    myWriter.write(req + "\n");
                    myWriter.write(s + ": " + printRes(results.get(results.get(0).indexOf(s))) + "\n\n");


                    myWriter.close();
                } catch (IOException e) {
                    System.out.println("Erreur dans l'écriture du résultat de la requête");
                    e.printStackTrace();
                }
            }
            verbose+=s + ": " + printRes(results.get(results.get(0).indexOf(s)));
        }


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

        if(this.options.getJena()) {
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

            String[] jena = jenaSolve(req).split("\n");
            String[] ourResult = CSVResults.split("\n");

            /* TEST
            System.out.println("L1"+jena[0].contains(ourResult[0]));
            System.out.println(jena[0].length()+"--"+ourResult[0].length());
            System.out.println("L2"+jena[1].contains(ourResult[1]));
            System.out.println(jena[1].length()+"--"+ourResult[1].length());
            System.out.println("$$$"+CSVResults+"$$$");
            */

            //System.out.println(jenaSolve(req).equals(CSVResults)); //TODO très étrange que ça marche pas quand meme
            if (jena[0].contains(ourResult[0])&& jena[1].contains(ourResult[1])) { //TODO WARNING
                System.out.println("Jena-True");
            }
            else {
                System.out.println("Jena-False");
            }
        }

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

        for(int v : values) {//TODO peut-être qu'on peut supprimer cette boucle
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
    
    public HashMap<String,ArrayList<Integer>> mergeGeneral(HashMap<String,ArrayList<Integer>> left, HashMap<String,ArrayList<Integer>> right,String variableJointure){

        HashMap<String,ArrayList<Integer>> result = new HashMap<>();
        result.put(variableJointure,new ArrayList<>());
        
        //TODO considérer uniquement les variables que l'on doit projeter
        ArrayList<String> varLeft = new ArrayList<>();
        ArrayList<String> varRight = new ArrayList<>();

        int l_size = left.get(variableJointure).size();
        int r_size = right.get(variableJointure).size();

        for(String k : left.keySet()) {
            if(!k.equals(variableJointure)) {
                varLeft.add(k);
                result.put(k,new ArrayList<>());
            }
        }

        for(String k : right.keySet()) {
            if(!k.equals(variableJointure)) {
                varRight.add(k);
                result.put(k,new ArrayList<>());
            }
        }


        for(int i=0 ; i<l_size ; i++) {
        	for(int j=0 ; j<r_size ; j++) {
        		if(left.get(variableJointure).get(i).equals(right.get(variableJointure).get(j))) {
        			result.get(variableJointure).add(left.get(variableJointure).get(i));

        			for(String var : varLeft) {
        				result.get(var).add(left.get(var).get(i));
        			}

        			for(String var : varRight) {
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

    public void checkReq(String toCompare){
        System.out.println(this.dictionnaire.getValue(toCompare));
    }

    public Options getOptions(){
        return this.options;
    }



    //TODO: on compare ici le résultat d'une req de nous au res d'une req de jena
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

    public void warm(float pct, ArrayList<String> queries, boolean optim_none, ArrayList<String> starVariables) throws MalformedQueryException {
        Float queriesToExec=queries.size()*pct;

        ArrayList<Integer> generated = new ArrayList<>();
        for(int i=0; i<queriesToExec;i++){
            Random r = new Random();
            int c = r.nextInt(queries.size());
            while(generated.contains(c)){
                c = r.nextInt(queries.size());
            }
            generated.add(c);

            if(starVariables.size()==0) {
                if (!optim_none) {
                    System.out.println("OPTIM");
                    solveOptim(queries.get(c));
                } else {
                    System.out.println("NAIVE");
                    solve(queries.get(c));
                }
            }
            else{
                //TODO pbm
                //solveStarQuery(queries.get(c));
            }
        }
    }


    //TODO
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
        System.out.println(indexType+"->"+i1+"_"+i2);

        //System.out.println("___"+this.indexes.get("spo").getValuesNumber());
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
        //System.out.println("---"+i);
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
        System.out.println(res);
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

}





package moteur;

import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.*;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

import java.util.*;

public class Solveur {
    //TODO: gérer l'UTF-8
    Dictionnaire dico;
    Index spo, sop, pso, pos, osp, ops;

    public Solveur(Dictionnaire dico, ArrayList<Index> indexes){
        this.dico = dico;
        for(Index i: indexes){
            if(i.getType().equals("spo")){
                this.spo=i;
            }
            if(i.getType().equals("sop")){
                this.sop=i;
            }
            if(i.getType().equals("ops")){
                this.ops=i;
            }
            if(i.getType().equals("osp")){
                this.osp=i;
            }
            if(i.getType().equals("pos")){
                this.pos=i;
            }
            if(i.getType().equals("pso")){
                this.pso=i;
            }
        }
    }

    //Ajouter les variables de chaque pattern à notre structure allResults (récupère tous les résultats
    // pour chaque variable, pour chaque pattern)
    public void addKey(Var v, HashMap<String, ArrayList<ArrayList<Integer>>> allResults){
        if(!v.hasValue()){
            if(!allResults.containsKey(v.getName())) {
                allResults.put(v.getName(), new ArrayList<>());
                //System.out.println(v+" ajoutée à AllResults");
            }
        }
    }

    //Méthode principale de la classe
    public void solve(String req) throws MalformedQueryException {
        //Utilisation d'une instance de SPARLQLParser
        SPARQLParser sparqlParser = new SPARQLParser();
        ParsedQuery pq = sparqlParser.parseQuery(req, null);
        List<StatementPattern> patterns = StatementPatternCollector.process(pq.getTupleExpr());

        HashMap<String, ArrayList<ArrayList<Integer>>> allResults = new HashMap<>(); //TODO: faire l'intersection des AL pour avoir le résultat de la requete
        // Cette
        //Clé = la valeur recherchée
        //Valeurs = un ensemble d'ensembles de résultats pour chaque pattern

        System.out.println("\n-- Lecture de chaque pattern");

        for(StatementPattern sp: patterns) {
            //L'objectif de cette boucle est de déterminer l'index dans lequel on va pouvoir
            // récupérer l'information
            System.out.print("  Index utilisé: ");

            //On récupère les noms de variables
            Var s = sp.getSubjectVar();
            Var p = sp.getPredicateVar();
            Var o = sp.getObjectVar();

            //On les ajoute à notre structure allResults
            addKey(s, allResults);
            addKey(p, allResults);
            addKey(o, allResults);

            int valS, valP, valO;
            //On regarde si s, p ou o a une valeur = est une constante (pas une variable donc)
            if (s.hasValue()) { //index sxx
                valS = dico.getValue(s.getValue().stringValue());
                if (p.hasValue()) { //index spo
                    valP = dico.getValue(p.getValue().stringValue()); //TODO à vérifier
                    allResults.get(o.getName()).add(spo.getIndex().get(valS).get(valP)); //TODO
                    System.out.println("SPO");
                } else if (o.hasValue()) { //index sop
                    valO = dico.getValue(o.getValue().stringValue());
                    allResults.get(p.getName()).add(sop.getIndex().get(valS).get(valO));
                    System.out.println("SOP");
                } else { //TODO à vérifier avec les tailles (si un plus opti que l'autre)
                    //Cas où ni p ni o n'a de valeur
                    //On prend SOP par défaut
                    Set<Integer> keys = sop.getIndex().get((valS)).keySet();
                    ArrayList<Integer> resO = new ArrayList();
                    for (int i : keys) {
                        resO.add(i);
                        allResults.get(p.getName()).add(sop.getIndex().get((valS)).get(i));
                    }
                    allResults.get(o.getName()).add(resO);
                    System.out.println("S");
                }
            } else if (p.hasValue()) { //index pxx
                valP = dico.getValue(p.getValue().stringValue());
                if (s.hasValue()) { //index pso
                    valS = dico.getValue(s.getValue().stringValue());
                    allResults.get(o.getName()).add(pso.getIndex().get(valP).get(valS));
                    System.out.println("PSO");
                } else if (o.hasValue()) {//index pos
                    valO = dico.getValue(o.getValue().stringValue());
                    System.out.println("POS");
                    allResults.get(s.getName()).add(pos.getIndex().get(valP).get(valO));
                    //System.out.println(allResults.get(s.getName()));
                } else {
                    //Cas où ni s ni o n'a de valeur
                    //On prend PSO par défaut
                    Set<Integer> keys = pso.getIndex().get((valP)).keySet();
                    ArrayList<Integer> resS = new ArrayList();
                    for (int i : keys) {
                        resS.add(i);
                        allResults.get(s.getName()).add(pso.getIndex().get((valP)).get(i));
                    }
                    allResults.get(s.getName()).add(resS);
                    System.out.println("P");
                }
            } else if (o.hasValue()) { //on commence par un index oxx
                valO = dico.getValue(o.getValue().stringValue());
                if (s.hasValue()) { //index osp
                    valS = dico.getValue(s.getValue().stringValue());
                    allResults.get(p.getName()).add(osp.getIndex().get(valO).get(valS));
                    System.out.println("OSP");
                } else if (p.hasValue()) { //index ops
                    valP = dico.getValue(p.getValue().stringValue());
                    allResults.get(s.getName()).add(ops.getIndex().get(valO).get(valP));
                    System.out.println("OPS");
                } else {
                    //Cas où ni p ni s n'a de valeur
                    //On prend OSP par défaut
                    Set<Integer> keys = osp.getIndex().get((valO)).keySet();
                    ArrayList<Integer> resS = new ArrayList();
                    for (int i : keys) {
                        resS.add(i);
                        allResults.get(s.getName()).add(osp.getIndex().get((valO)).get(i));
                    }
                    allResults.get(s.getName()).add(resS);
                    System.out.println("O");
                }
            } else {
                //Cas où il y a 3 variables
                //Choix de base SPO
                // TODO: est-ce qu'il existe un plus optimisé qu'un autre?
                // TODO: Vérifier noms de variables

                Set<Integer> keysS = spo.getIndex().keySet();
                ArrayList<Integer> resS = new ArrayList();
                ArrayList<Integer> resP = new ArrayList();

                for (int kS : keysS) {
                    resS.add(kS);
                    Set<Integer> keysP = spo.getIndex().get((kS)).keySet();

                    for (int kP : keysP) {
                        resP.add(kP);
                        allResults.get(s.getName()).add(spo.getIndex().get((kS)).get(kP));
                    }
                }
                allResults.get(s.getName()).add(resS);
                allResults.get(p.getName()).add(resP);
                System.out.println("xxx");
            }
        }

        System.out.println("-- Intersections");
        //Dans allResults on a les résultats de chaque variable pour chaque pattern
        //Ici on fait pour chaque variable l'intersection des résultats
        //Ca nous permet d'obtenir pour chaque variable l'ensemble des résultas
        HashMap<String, ArrayList<Integer>> results = new HashMap<>();
        ArrayList<Integer> result = new ArrayList<>();
        for(String key: allResults.keySet()){
            result = allResults.get(key).get(0);
            //System.out.println(allResults.get(key).size()-1);
            //TODO: vérifier la taille
            for(int i = 1; i<allResults.get(key).size()-1;i++){
                result.retainAll(allResults.get(key).get(i));
            }
            results.put(key,result);
        }
        //System.out.println(result);

        //Cette structure nous permet d'avoir uniquement les variables à retourner (celles dans le SELECT)
        ArrayList<String> varToReturn = new ArrayList<>();
        System.out.println("-- Résultat de la requete");
        pq.getTupleExpr().visit(new QueryModelVisitorBase<RuntimeException>() {
            public void meet(Projection projection) {
                List<ProjectionElem> test = projection.getProjectionElemList().getElements();
                for(ProjectionElem p: test){
                    varToReturn.add(p.toString()); //TODO: vérifier le toString
                }
            }
        });

        for(String s: varToReturn){
            //System.out.println(s+" ++ "+ results.get(s)); //
            // TODO: régler le "problème" des "" vis à vis de s
        }

        for(String k: allResults.keySet()){
            System.out.println(k + allResults.get(k).toString());
            //for(Integer i: results.get) {
            //System.out.println(k + " ++ " + results.get(k).get(i));
            //}
        }
        //TODO: vérifier les résultats des requetes
    }
}





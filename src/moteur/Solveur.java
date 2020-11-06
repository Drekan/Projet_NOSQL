package moteur;

import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.*;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

import java.util.*;

public class Solveur {

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

    public void addKey(Var v, HashMap<String, ArrayList<ArrayList<Integer>>> allResults){
        if(!v.hasValue()){
            allResults.put(v.getName(),new ArrayList<>());
        }
    }
    public void solve(String req) throws MalformedQueryException {
        SPARQLParser sparqlParser = new SPARQLParser();
        ParsedQuery pq = sparqlParser.parseQuery(req, null);
        List<StatementPattern> patterns = StatementPatternCollector.process(pq.getTupleExpr());

        HashMap<String, ArrayList<ArrayList<Integer>>> allResults = new HashMap<>(); //TODO: faire l'intersection des AL pour avoir le résultat de la requete
        System.out.println("-- Lecture des patterns");
        
        for(StatementPattern sp: patterns) {
        	
            //objectif: déterminer l'index
            Var s = sp.getSubjectVar();
            Var p = sp.getPredicateVar();
            Var o = sp.getObjectVar();
            int valS, valP, valO;

            addKey(s, allResults);
            addKey(p, allResults);
            addKey(o, allResults);

            //On regarde si s, p ou o a une valeur = est une constante (pas une variable donc)
            //TODO Traiter le cas où y'a pas de valeurs
            if (s.hasValue()) { //on commence par un index sxx
                valS = dico.getValue(s.getValue().stringValue());
                if (p.hasValue()) { //index spo
                    valP = dico.getValue(p.getValue().stringValue()); //TODO à vérifier
                    allResults.get(o.getName()).add(spo.getIndex().get(valS).get(valP)); //TODO
                } else if (o.hasValue()) { //index sop
                    valO = dico.getValue(o.getValue().stringValue());
                    allResults.get(p.getName()).add(sop.getIndex().get(valS).get(valO));
                } else { //TODO à vérifier avec les tailles (si un plus opti que l'autre)
                    Set<Integer> keys = sop.getIndex().get((valS)).keySet();
                    ArrayList<Integer> resO = new ArrayList();
                    for (int i : keys) {
                        resO.add(i);
                        allResults.get(p.getName()).add(sop.getIndex().get((valS)).get(i));
                    }
                    allResults.get(o.getName()).add(resO);
                }
            } else if (p.hasValue()) { //on commence par un index sxx
            	
                valP = dico.getValue(p.getValue().stringValue());
                if (s.hasValue()) { //index pso
                    valS = dico.getValue(s.getValue().stringValue());
                    allResults.get(o.getName()).add(pso.getIndex().get(valP).get(valS));

                } else if (o.hasValue()) {//index pos
                    valO = dico.getValue(o.getValue().stringValue());
                    allResults.get(s.getName()).add(pos.getIndex().get(valP).get(valO));
                } else {
                    Set<Integer> keys = pso.getIndex().get((valP)).keySet();
                    ArrayList<Integer> resS = new ArrayList();
                    for (int i : keys) {
                        resS.add(i);
                        allResults.get(s.getName()).add(pso.getIndex().get((valP)).get(i));
                    }
                    allResults.get(s.getName()).add(resS);
                }
            } else if (o.hasValue()) { //on commence par un index oxx
                valO = dico.getValue(o.getValue().stringValue());
                if (s.hasValue()) { //index osp
                    valS = dico.getValue(s.getValue().stringValue());
                    allResults.get(p.getName()).add(osp.getIndex().get(valO).get(valS));
                } else if (p.hasValue()) { //index ops
                    valP = dico.getValue(p.getValue().stringValue());
                    allResults.get(s.getName()).add(ops.getIndex().get(valO).get(valP));
                } else {
                    Set<Integer> keys = osp.getIndex().get((valO)).keySet();
                    ArrayList<Integer> resS = new ArrayList();
                    for (int i : keys) {
                        resS.add(i);
                        allResults.get(s.getName()).add(osp.getIndex().get((valO)).get(i));
                    }
                    allResults.get(s.getName()).add(resS);
                }
            } else {//si on a 3 variables -- choisir un opti ?
                //Choix de base = spo
                //A vérifier si je ne me suis trompée dans les noms de variables
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
            }
        }

        System.out.println("-- Intersections");
            //Intersections pour avoir les résultats
            HashMap<String, ArrayList<Integer>> results = new HashMap<>();
            ArrayList<Integer> result = new ArrayList<>();
            for(String key: allResults.keySet()){
                result = allResults.get(key).get(0);
                for(int i = 1; i<allResults.get(key).size()-1;i++){
                    result.retainAll(allResults.get(key).get(i));
                }
                results.put(key,result);
            }
            //TODO: regarder ce qu'il a dans AllResults

            ArrayList<String> varToReturn = new ArrayList<>();

        System.out.println("-- Last check");
            pq.getTupleExpr().visit(new QueryModelVisitorBase<RuntimeException>() {
                public void meet(Projection projection) {
                    List<ProjectionElem> test = projection.getProjectionElemList().getElements();
                    for(ProjectionElem p: test){
                        varToReturn.add(p.toString()); //TODO: vérifier le toString
                    }
                }
            });

            for(String s: varToReturn){
                System.out.println(s+" -- "+ results.get(s));
            }
    }
}





package parsers;

import java.util.List;

import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

public class SPARQLRawParser {

	public static void main(String[] args) {

		SPARQLParser sparqlParser = new SPARQLParser();

		String query = "SELECT ?v1 ?v0 WHERE {	?v1 <http://schema.org/eligibleRegion> <http://db.uwaterloo.ca/~galuc/wsdbm/Country48> . ?v0 <http://schema.org/eligibleRegion> <http://db.uwaterloo.ca/~galuc/wsdbm/Country48>}";

		try {
			ParsedQuery pq = sparqlParser.parseQuery(query, null);

			List<StatementPattern> patterns = StatementPatternCollector.process(pq.getTupleExpr());

			System.out.println("first pattern : " + patterns.get(0));

			System.out.println("object of the first pattern : " + patterns.get(0).getObjectVar().getValue());


			System.out.println("variables to project : ");
			pq.getTupleExpr().visit(new QueryModelVisitorBase<RuntimeException>() {
				public void meet(Projection projection) {
					System.out.println(projection.getProjectionElemList().getElements());
				}
			});


		} catch (MalformedQueryException e) {
			e.printStackTrace();
		}

	}

}
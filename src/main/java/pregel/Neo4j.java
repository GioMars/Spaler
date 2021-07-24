package pregel;

import org.apache.spark.api.java.JavaRDD;
import org.neo4j.driver.*;

import java.util.List;

public class Neo4j {
    // Neo4J Graph
    public void neo(JavaRDD<Contig> c, JavaRDD<Edge> e){

        String uri = "bolt://localhost:7687";
        AuthToken token = AuthTokens.basic("neo4j", "password");

        Driver driver = GraphDatabase.driver(uri, token);

        Session s = driver.session();
        System.out.println("Connessione stabilita");

        String deleteQuery = "MATCH (n) DETACH DELETE n";
        s.run(deleteQuery);


		List<Contig> c1 = c.collect();
        String kmer = "";

		int count = 0;
		for(Contig y: c1) {
		    long id = y.getId();
            kmer = y.getContig();
			String cql = "create( : kmer {id : "+id+", kmer: '"+ kmer +"'})";
			s.run(cql);
			count++;
		}

		System.out.println("Aggiunti " + count + " vertici al grafo.");


        List<Edge> e1 = e.collect();
        count = 0;
        for(Edge r: e1) {
            long start = r.getSrc();
            long end = r.getDst();
            int freq = r.getFreq();
            String fvp = r.getFvp();
            String svp = r.getSvp();
            String cql = "match (n:kmer {id:"+start+"}),(m:kmer {id:"+end+"}) create (n)-[:Edge {"
                    + "freq:"+freq+", fvp:'"+fvp+"', svp:'"+svp+"'}]->(m)";
            s.run(cql);
            count++;
        }

        System.out.println("Aggiunti " + count + " archi al grafo.");
        s.close();
    }
}

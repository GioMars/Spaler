package pregel;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.graphframes.GraphFrame;
import scala.Tuple2;

// JAVA 1.8.0_282

public class Main {

	public static void main(String[] args) {
		// log preferenze
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		
		// Java Spark Session
		SparkSession spark = SparkSession.builder().
				master("local[8]").
				appName("Spaler").
				getOrCreate();
		
		// Java Spark Context
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		SQLContext sqlContext = new SQLContext(jsc);
		sqlContext.setConf("spark.sql.shuffle.partitions", "8");

		// Neo4J
		boolean Neo = false;

		// Reads input
		JavaRDD<String> reads = jsc.textFile("data/papaya.fasta");

		// FASTA FILES
		reads = reads.filter(x -> x.charAt(0) != '>').
				map(x -> x.replaceAll("[^ACGT]","A"));
		System.out.println("Numero di reads: " + reads.count());

		// Livello k
		int k = 20;

		// k+1 mers JavaPairRDD(sequence, frequency) 
		JavaPairRDD<String, Integer> subSeqsRdd = reads.flatMap(seq -> {
			ArrayList<String> subSeqs = new ArrayList<>();
			for (int i = 0; i < seq.length() - k; i++)
				subSeqs.add(seq.substring(i, i + k + 1));
			return subSeqs.iterator();}).
				map(x -> new Rep(x).rep()).
				mapToPair(x -> new Tuple2<>(x,1)).
				reduceByKey(Integer::sum);
		// System.out.println(subSeqsRdd.take(5));
		
		// Filter per low coverage
		int threshold = 0;
		subSeqsRdd = subSeqsRdd.filter(t -> t._2 > threshold);

		// Vertex
		JavaRDD<Contig> c = subSeqsRdd.map(Tuple2::_1).flatMap(seq -> {
			ArrayList<String> subSeqs = new ArrayList<>();
			for (int i = 0; i <= 1; i++)
				subSeqs.add(seq.substring(i, i + k));
			return subSeqs.iterator();}).
				map(x -> new Rep(x).rep()).
				distinct().
				map(x -> {
					Long id = new Coding().encode(x.getBytes(StandardCharsets.UTF_8));
					Contig y = new Contig(id, x, x.length());
					return y;
					});

		// Edges
		JavaRDD<Edge> e = subSeqsRdd.map(t -> {

				// Sequence and frequence
				String seq = t._1;
				int freq = t._2;
				
				String seq1 = seq.substring(0, k);
				String seq2 = seq.substring(1, k+1);
				
				String src = new Rep(seq1).rep();
				String dst = new Rep(seq2).rep();
				
				String fvp="";
				String svp="";
				if (src.equals(seq1)) {
					fvp += "L";
				} else {
					fvp += "H";
				}
				
				if (dst.equals(seq2)) {
					svp += "L";
				} else {
					svp += "H";
				}

				long srcID = new Coding().encode(src.getBytes(StandardCharsets.UTF_8));
				long dstID = new Coding().encode(dst.getBytes(StandardCharsets.UTF_8));
				Edge j = new Edge(srcID,dstID,freq, fvp, svp);

				j.setVal(Math.random());
				return j;
			}
		);

		Dataset<Row> edges = spark.createDataFrame(e, Edge.class);

		GraphFrame DBG = GraphFrame.fromEdges(edges);
		System.out.println("Numero di k-mers: " + DBG.vertices().count());
		// System.out.println("Numero di archi: " + DBG.edges().count());


		// Grafo Neo4j
		Scanner scan = new Scanner(System.in);
		Neo4j n = new Neo4j();
		if(Neo) {
			System.out.println("Digitare 1 per riprodurre su Neo4J, 0 altrimenti: ");
			int a = scan.nextInt();
			if (a == 1) {
				n.neo(c, e);
				}
			}

		int i=1;

		do {
			Tuple2<JavaRDD<Vertex>, JavaRDD<Edge>> pregel = graphUtil.preg(DBG);
			JavaPairRDD<Long, Vertex> verticesPregel = pregel._1.mapToPair(t -> new Tuple2<>(t.getId(), t));

			JavaPairRDD<Long, String> c1 = c.mapToPair(t -> new Tuple2<>(t.getId(), t.getContig()));
			JavaPairRDD<Long, Tuple2<String, Vertex>> c2 = c1.join(verticesPregel);

			// VERTICI
			System.out.println("Iterazione: " + i);

			c = c2.groupBy(t -> t._2._2.getGid()).mapValues(t -> {
				List<Tuple2<Long, Tuple2<String, Vertex>>> lista = new ArrayList<>();
				for(Tuple2<Long, Tuple2<String, Vertex>> tup : t){
					lista.add(tup);
				}

				lista.sort(new VertexComparator());

				if (lista.size() == 1) {
					long id = lista.get(0)._1;
					String seq = lista.get(0)._2._1;
					Contig j = new Contig(id, seq, seq.length());
					return j;
				} else {
					String seq = "";
					// HEAD
					Tuple2<String, Vertex> w = lista.get(0)._2;
					Vertex head = w._2;
					long gid = w._2.getGid();
					if (head.getPol().equals("L")) {
						seq += w._1;
					} else {
						seq += new Rep(w._1).repNotOrdered();
					}

					// rimozione del primo elemento da LIST
					lista.remove(0);

					// Generate CONTIG
					for (Tuple2<Long, Tuple2<String, Vertex>> y : lista) {
						Vertex j = y._2._2;
						String s = y._2._1;
						if (j.getPol().equals("L")) {
							seq += s.substring(k - 1);
						} else {
							seq += new Rep(s).repNotOrdered().substring(k - 1);
						}
					}

					Contig j = new Contig(gid, seq, seq.length());
					return j;
				}
			}).map(t -> t._2);

			// ARCHI
			e = pregel._2;
			edges = spark.createDataFrame(e, Edge.class);

			DBG = GraphFrame.fromEdges(edges);
			// System.out.println("Numero di nodi: " + DBG.vertices().count());
			// System.out.println("Numero di archi: " + DBG.edges().count());


			if (Neo) {
				System.out.println("Digitare 1 per riprodurre su Neo4J, 0 altrimenti: ");
				int a = scan.nextInt();
				if (a == 1) {
					n.neo(c, e);
				}
			}

			i++;

		} while(i<11);

		Dataset<Row> contigs = spark.createDataFrame(c, Contig.class);
		// contigs.show();

		System.out.println("Numero di nodi: " + DBG.vertices().count());

		// MAX LEN CONTIG
		// System.out.println("Max len contig: " + c.max(new ContigsMaxLen()).getLen());

	}
}

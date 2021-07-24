package pregel


import org.apache.spark.api.java.JavaRDD
import org.apache.spark.graphx.EdgeDirection
import org.graphframes.GraphFrame


object graphUtil {
  def preg(g: GraphFrame): (JavaRDD[Vertex], JavaRDD[Edge]) = {

    // Conversione a GraphX
    val graph = g.toGraphX

    // VertexRDD con attributo la polarità degli archi cui il vertice è connesso
    val polarities = graph.aggregateMessages[String](
      triplet => {
        triplet.sendToDst(triplet.attr.getString(4))
        triplet.sendToSrc(triplet.attr.getString(2))
      },
      (a, b) => a.union(b)
    )

    val bool = graph.aggregateMessages[Boolean](
      triplet => {
        val r = 0.1
        if(triplet.attr.getDouble(5) < r) {
          triplet.sendToDst(true)
          triplet.sendToSrc(true)
        } else{
          triplet.sendToDst(false)
          triplet.sendToSrc(false)
        }
      },
      (a, b) => a||b
    )


    // Associare ad ogni vertice gli attributi DEGREE, IN-DEGREE e POLARITY
    val round1 = graph.outerJoinVertices(graph.degrees)((_,attr, deg) => (attr,deg.getOrElse(0))).
      outerJoinVertices(graph.inDegrees)((_,attr, deg) => (attr,deg.getOrElse(0))).
      outerJoinVertices(polarities)((_,attr,pol) => (attr,pol.getOrElse("N"))).
      outerJoinVertices(bool)((_,attr,boolVar) => (attr,boolVar.getOrElse(true)))
    //round1.vertices.foreach(println)


    // Pulizia degli attributi dei vertici del grafo
    var round4 = round1.
      mapVertices((id, attr) => (
        id, // 1 GID (merging obgect - default ID)
        -1, // 2 INDEX (merging object - default -1)
        "L", // 3 POL (merging object - default "L") // 4 DEGREE

        // 4 CARATTERIZZAZIONE AMBIGUO/NON AMBIGUO -> TRUE/FALSE

        // attr._1._1._1._2         DEGREE
        // attr._1._1._2            IN-DEGREE
        // attr._1._2               POLARITY

        // DEGREE > 2 || (DEGREE == 2 &&
        // (INDEGREE == 2 && (POL == "HH" || POL == "LL") ||
        // (INDEGREE == 0 && (POL == "HH" || POL == "LL") ||
        // (INDEGREE == 1 && !(POL == "HH") || POL == "LL"))
        (attr._1._1._1._2 > 2) || (attr._1._1._1._2 == 2 && (
          (attr._1._1._2 == 2 && (attr._1._2.equals("HH") || attr._1._2.equals("LL"))) ||
            (attr._1._1._2 == 0 && (attr._1._2.equals("HH") || attr._1._2.equals("LL"))) ||
            (attr._1._1._2 == 1 && !(attr._1._2.equals("HH") || attr._1._2.equals("LL"))))),
        attr._1._1._1._2, // 5 DEGREE
        attr._2 // 6 BOOL
      )).mapVertices((_, attr) => (
      attr._1,
      attr._2,
      attr._3,
      attr._4 || attr._6,
      attr._5,
      attr._4                // CARATTERIZZAZIONE AMBIGUO/ NON AMBIGUO
    ))

    /*
    println("\n")
    round4.vertices.foreach(println)
     */

    // TIPS TRIMMING
    round4 = round4.subgraph(triplet => !(triplet.dstAttr._5== 1 && triplet.srcAttr._6) ||
      !(triplet.srcAttr._5 == 1 && triplet.dstAttr._6))


    // PREGEL
    val graphPregel = round4.pregel((0L, -1, "L", false),
      maxIterations = 10,
      activeDirection = EdgeDirection.Either)(

      // VPROG
      (_, attr, new_visited) => (
        new_visited._1,
        new_visited._2,
        new_visited._3,
        attr._4 || new_visited._4,
        attr._5,
        attr._6),

      // SEND MSG
      triplet => {
       if(triplet.srcAttr._4 && !triplet.dstAttr._4 && triplet.srcAttr._5 < 3) {
         if(triplet.srcAttr._2 == -1) {
           Iterator((triplet.dstId, (triplet.srcId, triplet.srcAttr._2 + 2, triplet.attr.getString(4), true)),
           (triplet.srcId, (triplet.srcId, 0, triplet.attr.getString(2), true)))
       } else {
          Iterator((triplet.dstId, (triplet.srcAttr._1, triplet.srcAttr._2 + 1, triplet.attr.getString(4), true)))
         }
       } else if (triplet.dstAttr._4 && !triplet.srcAttr._4 && triplet.dstAttr._5 < 3){
         var pol1 = "H"
         if(triplet.attr.getString(4) == "H"){
           pol1 = "L"
         }
         var pol2 = "H"
         if(triplet.attr.getString(2) == "H"){
           pol2 = "L"
         }
         if(triplet.srcAttr._2 == -1){
           Iterator((triplet.dstId, (triplet.dstId, 0, pol1, true)),
           (triplet.srcId, (triplet.dstId, triplet.dstAttr._2 + 2, pol2, true)))
         } else {
           Iterator((triplet.srcId, (triplet.dstAttr._1, triplet.dstAttr._2 + 1, pol2, true)))
         }
       } else{
         Iterator.empty
       }
      },

      // MERGE MSG
      (visited1, visited2) => (visited2._1, visited2._2, visited2._3, visited1._4 || visited2._4)
    )

    /*
    println("\n")
    println("Pregel")
    graphPregel.vertices.foreach(println)
     */


    // Vertices creation
    val verticesPregel = graphPregel.vertices.map(t => {
      val vid = t._1
      val attr = t._2
      if(attr._1 == 0){
        // ID GID INDEX POL
        val v = new Vertex(vid, vid, attr._2, attr._5, attr._3)
        v
      } else {
        val v = new Vertex(vid, attr._1, attr._2, attr._5, attr._3)
        v
      }
    }).toJavaRDD()


    // Edges creation
    // Rimozione degli archi che collegano nodi facenti parte di un medesimo gruppo
    val validGraph = graphPregel.subgraph(triplet => !((triplet.dstAttr._2 > -1) &&
      (triplet.srcAttr._2 > -1) && (triplet.srcAttr._1 == triplet.dstAttr._1)))

    // Creazione dei nuovi archi con le corrette polarità
    val edgesPregel = validGraph.mapTriplets(triplet => {
      // caso arco uscente da vm
      val attr = triplet.attr
      if(triplet.srcAttr._2 == -1 && triplet.dstAttr._2 == 0){
        val v = new Edge(attr.getLong(3),attr.getLong(0), attr.getInt(1), attr.getString(2), "L")
        v.setVal(Math.random)
        v
      } else if (triplet.srcAttr._2 == 0 && triplet.dstAttr._2 == -1) {
        val v = new Edge(attr.getLong(3), attr.getLong(0), attr.getInt(1), "H", attr.getString(4))
        v.setVal(Math.random)
        v
      }else if (triplet.srcAttr._2 > 0 && triplet.dstAttr._2 == -1){
        val v = new Edge(triplet.srcAttr._1,attr.getLong(0), attr.getInt(1), "L", attr.getString(4))
        v.setVal(Math.random)
        v
      } else if (triplet.srcAttr._2 == -1 && triplet.dstAttr._2 > 0){
        val v = new Edge(attr.getLong(3),triplet.dstAttr._1, attr.getInt(1), attr.getString(2), "H")
        v.setVal(Math.random)
        v
      } else if (triplet.srcAttr._2 == 0 && triplet.dstAttr._2 == 0){
        val v = new Edge(attr.getLong(3),attr.getLong(0), attr.getInt(1), "H", "L")
        v.setVal(Math.random)
        v
      } else if(triplet.srcAttr._2 == 0 && triplet.dstAttr._2 > 0){
        val v = new Edge(triplet.srcAttr._1,triplet.dstAttr._1, attr.getInt(1), "H", "H")
        v.setVal(Math.random)
        v
      } else if(triplet.srcAttr._2 > 0 && triplet.dstAttr._2 == 0){
        val v = new Edge(triplet.srcAttr._1,triplet.dstAttr._1, attr.getInt(1), "L", "L")
        v.setVal(Math.random)
        v
      } else if(triplet.srcAttr._2 == -1 && triplet.dstAttr._2 == -1){
        val v = new Edge(attr.getLong(3),attr.getLong(0), attr.getInt(1), attr.getString(2), attr.getString(4))
        v.setVal(Math.random)
        v
      } else {
        val v = new Edge(triplet.srcAttr._1,triplet.dstAttr._1, attr.getInt(1), "L", "H")
        v.setVal(Math.random)
        v
      }
    }).edges.map(t => t.attr).toJavaRDD()


    // RETURN
    (verticesPregel, edgesPregel)

  }
}
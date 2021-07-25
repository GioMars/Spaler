# De Novo Assembly basato su Spark 

L’assemblaggio de novo è il metodo di costruzione di genomi a partire da un gran numero di frammenti di DNA (reads), senza alcuna conoscenza a priori della sequenza completa o dell'ordine corretto di tali frammenti.
In questo lavoro è stato affrontato il problema di parallelizzazione mediante sistemi di memoria distribuiti dell'assemblaggio di nuovi genomi basato su grafi di De Bruijn, facendo riferimento alla procedura [Spaler](https://www.researchgate.net/publication/283425205_Spaler_Spark_and_GraphX_based_de_novo_genome_assembler).  Esiste un ampio interesse nell'uso dell'assemblaggio de novo poiché il numero di specie completamente sequenziate è piccolo rispetto a quelle sconosciute.
La procedura si basa sul framework Spark e sugli API GraphX e GraphFrame.

### Classi
| Classe        | Descrizione           |
|:---------- |:------------- |
| `Main.java` | classe main dell'applicazione |
| `Vertex.java` | classe wrapper per modellare gli oggetti di tipo vertice |
| `Edge.java` | classe wrapper per modellare gli oggetti di tipo arco |
| `Contig.java` | classe wrapper per modellare le sequenze nucleotidiche (contigs) |
| `Rep.java` | classe contenente metodi funzionali alla realizzazione del reverse complement di sequenze nucleotidiche |
| `Coding.java` | classe contenente metodi funzionali alla codifica di k-mers |
| `Neo4j.java` | classe contenente un metodo per la rappresentazione di un grafo su Neo4j |
| `VertexComparator.java` | classe strumentale alla comparazione di vertici sulla base del proprio indice di gruppo |
| `ContigsMaxLen.java` | classe strumentale alla comparazione di contigs sulla base della lunghezza della sequenza nucleotidica |
| `graphUtil.scala` | classe contenente l'implementazione in scala di un metodo funzionale alla semplificazione dei grafi |

### Costruzione del grafo di De Bruijn (Java)

Come primo punto si prevede la costruzione del grafo di de Bruijn (DBG) dal set di dati contenente le reads rilevate dal sequenziatore. L’assemblaggio basato su DBG opera tagliando le reads in sottosequenze consecutive di lunghezza k, chiamate k-mers. Ogni k-mer è rappresentato da un nodo sul grafo, i vari nodi sono legati tra loro mediante archi (impostati per connettere i vertici sovrapposti e dotati di un preciso orientamento). Il vantaggio principale dell'assemblatore basato su DBG è che la dimensione del grafo assemblante è proporzionale alla complessità della sequenza di riferimento, indipendentemente dalla dimensione del set di dati di reads brevi. Da ciò, l'attraversamento dei vertici seguendo le direzioni proposte dagli archi genera la sequenza originale. Tale approccio risolve il problema nell’ottica di un ciclo Euleriano; un ciclo Euleriano sopra un multigrafo è un cammino che tocca tutti i suoi archi una e una volta sola. 
L'assemblaggio de novo del genoma è un problema impegnativo a causa di quanto segue:
* Processare un'enorme quantità di dati rappresenta evidentemente una difficoltà per tutti gli attuali metodi di assemblaggio.
* La somiglianza delle sequenze è molto comune nel sequenziamento del genoma. Ciò causa un gran numero di vertici con elevato grado nel grafo e lo rende più complicato. Una manipolazione impropria di tale ramificazione produce un assemblaggio di bassa qualità.
* Errori derivanti dal processo di sequenziamento; variazioni o cancellazioni nelle sequenze portano alla costruzione di vertici falsi positivi nel DBG. L'effetto di tali errori si riflette sulla dimensione e la complessità del DBG.
* La distribuzione della copertura non uniforme che può verificarsi nella fase di sequenziamento a causa della struttura delle sequenze.

La molecola di acido desossiribonucleico (DNA) è formata da due sequenze di filamenti di nucleotidi, avvolti l'uno intorno all'altro. I nucleotidi possono essere di quattro tipi di basi: adenina (A), citosina (C), guanina (G) o timina (T). Entrambi i filamenti di una molecola di DNA immagazzinano le stesse informazioni genetiche ma in maniera complementare tale che il nucleotide A è connesso a T e il nucleotide G è connesso a C. Una sequenza originale e il suo complemento inverso rappresentano concretamente la stessa regione del DNA, quindi nella pratica sono rappresentate mediante un'unica entità per contenere l’utilizzo di memoria RAM durante l’esecuzione; per ogni k-mer si origina pertanto una sottosequenza di rappresentanza che da adesso in poi è richiamata con il nome di `rep`, da identificarsi scegliendo di volta in volta il più piccolo tra i complementi in base all’ordine lessicografico. A causa della rappresentazione complementare della sequenza del DNA ad ogni arco sono associati i due attributi `fvp` e `svp`, tali due attributi rappresentano la polarità del vertice di origine e del vertice di destinazione dell'arco. La polarità assume due modalità: `"L"` ed `"H"`. La modalità `"L"` indica che la sequenza è utilizzata senza alcuna modifica mentre la modalità `"H"` indica che la sequenza del vertice richiede di essere complementata per poter essere a sua volta agganciata. La definizione delle polarità associate agli archi incidenti su un dato vertice non è aspetto secondario del problema, in quanto influenza direttamente in-degree e out-degree del vertice in questione. Si definisca come *single in* (*single out*) un vertice di grado uno con incidente un arco entrante (uscente), si definisca come *single-in single-out* un vertice di grado due con incidenti un arco uscente ed uno entrante, si definisca come *nodo di ambiguità* un vertice con in-degree oppure out-degree superiore ad uno.

#### Rappresentazione delle sequenze nucleotidiche

L'algoritmo inizia con la pulizia delle reads sostituendo le non-basi {A,C,T,G} con A, quindi elabora ogni read di lunghezza l ≥ k + 1 ed estrae le sovrapposte (k+1)-mers nelle posizioni da 0 a (l-k). Le sottosequenze generate vengono convertite nelle loro sottosequenze `rep`. Viene applicata un'operazione di riduzione per generare un set di (k+1)-mers e le loro frequenze (copertura), quindi è possibile applicare un'operazione di filtro per rimuovere le (k+1)-mers con bassa copertura (che si presume si verifichi a causa di errori dovuti al sequenziamento).
```
int k = 6;

JavaPairRDD<String, Integer> subSeqsRdd = reads.flatMap(seq -> {
  ArrayList<String> subSeqs = new ArrayList<>();
  for (int i = 0; i < seq.length() - k; i++)
	  subSeqs.add(seq.substring(i, i + k + 1));
	return subSeqs.iterator();}).
	map(x -> new Rep(x).rep()).
	mapToPair(x -> new Tuple2<>(x,1)).
	reduceByKey(Integer::sum);
```

Ciascun (k+1)-mer può essere a sua volta rappresentato come un arco tra due k-mers sovrapposti. Tali k-mers vengono estratti e le loro sequenze vengono convertite in `rep`. Per ogni k-mer estratto, viene creato un iniziale contig (classe `Contig`) per cui sono previsti i seguenti attributi:
1. `id`, valore `long` inizializzato con la codifica numerica della sequenza associata
2. `contig`, sequenza nucleotidica (`String`)
3. `len`, lunghezza della sequenza nucleotidica (`int`) 

```
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
```

Ogni neo coppia di k-mers è collegata in maniera diretta da un arco orientato. Per ciascun arco viene creato un oggetto di classe `Edge` per cui sono previsti i seguenti attributi:
1. `src`, ID del vertice di provenienza (costruttore)
2. `dst`, ID del vertice di arrivo (costruttore)
3. `freq`, frequenza (costruttore)
4. `fvp`, polarità del vertice di provenienza (costruttore)
5. `svp`, polarità del vertice di arrivo (costruttore)
6. `val`, valore double assegnato casualmente (funzionale alla fase di semplificazione del grafo)

```
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
```

Successivamente, partendo dagli archi appena originati si procede alla creazione del grafo DBG. In tal modo le sequenze nucleotidiche sono custodite nell'apposita `JavaRDD<Contig>` mentre nel grafo sono riportate solamente le informazioni utili in vista della conseguente semplificazione. 

```
GraphFrame DBG = GraphFrame.fromEdges(edges);
```

### Generazione dei contigs (Java + Scala)

L'assemblaggio de novo del genoma è il processo di ricucitura (stitching) di una serie di brevi sequenze estratte per generare sequenze di DNA più lunghe, chiamate contigs. La generazione di contigs si suddivide a sua volte in due parti: 
1. Unione dei vertici,
2. Costruzione degli archi.

L'approccio “naive” della generazione di contigs consiste nel percorrere i percorsi ovvi formati dai vertici v1−1 (*single-in single-out*) e v1 (*single-in* oppure *single-out*) e concatenare le loro sequenze. Tale operazione si ferma ai *nodi di ambiguità* poiché la direzione del percorso rimane da questi sconosciuta. Tuttavia, l'elaborazione diretta di ogni percorso ovvio nella sua interezza non è efficiente. Questo perché il numero di tali percorsi è proporzionale al numero di *vertici di ambiguità*, che è piccolo rispetto alla grande dimensione del DBG; di conseguenza non risulta possibile utilizzare in modo efficiente le unità di elaborazione. A tal fine, l'algotritmo suddivide i potenziali percorsi ovvi in sottopercorsi (sub-paths) di breve lunghezza per poi unirli in iterazioni successive. Alla fine di ogni iterazione si genera un nuovo DBG di dimensioni ridotte, punto di partenza per l'iterazione successiva.

#### L'iterazione nel dettaglio
L'algoritmo inizia con il selezionare un insieme di vertici Vm, che vengono utilizzati come “Heads” per i percorsi che verranno uniti. Sia vm uno dei vertici in Vm. I candidati ad essere vertici vm sono tutti quei vertici v1 e quei vertici v1−1 che circondano i *vertici di ambiguità*. Sono selezionate casualmente coppie di vertici candidati; la selezione dei vertici a coppie impone l'estensione dei percorsi di assemblaggio in due direzioni opposte. La selezione delle coppie è ottenuta con riferimento al valore casuale (val | 0 ≤ valore < 1) associato a ciascun arco. I vertici connessi ad un dato arco sono inclusi in Vm se val < r, dove r è un fattore di selezione casuale. Poiché si tratta di un'operazione casuale, potrebbe essere selezionato un *vertice di ambiguità*, tuttavia quest'ultimo non può essere incluso in Vm.

Nella pratica il grafo di partenza DBG viene convertito in GraphX.

```{scala}
val graph = g.toGraphX
```

L'algoritmo utilizza la funzione Pregel di GraphX per unire i vertici selezionati nello stesso percorso. Ogni vertice ha un oggetto di fusione (merging); ossia una tripletta composta di un ID di gruppo `gid`, un indice `index` e la polarità `pol` del vertice. I valori iniziali di questa tripletta sono `< Vertex-ID, -1, "L">`. L'indice di merging rappresenta la posizione del vertice rispetto alla testa del gruppo. L'algoritmo prepara i vertici alla funzione Pregel assegnando i seguenti attributi:
1. Vertex ID
2. Group ID (di default pari al Vertex ID)
3. Indice di gruppo (di default `-1`)
4. Polarità (di default `"L"`)
5. Grado
6. Valore booleano

La determinazione del valore booleano (di cui al punto 8 dell'elenco subito sopra) risulta particolarmente delicata. Mediante il metodo `AggregateMessages` ogni arco invia ai propri nodi incidenti:
* le informazioni circa la polarità registrata;
* un valore booleano che segnala al nodo se l'arco incidente è stato selezionato nel processo di estrazione casuale. 

Mediante operazione di `join` ad ogni nodo vengono associate le informazioni appena computate a cui si aggiungono degree ed in-degree. Segue una fase di pulizia degli attributi di ogni vertice (operazione `mapVertices`). Il valore booleano di cui al punto 8 assume valore `true` se:
1. il nodo in questione è un *nodo di ambiguità*;
2. il nodo in questione è un nodo v1 oppure v1-1 ed ha come incidente un arco selezionato nel processo di estrazione casuale. 

La funzione Pregel, basata su uno scambio di messaggi attraverso i nodi, esegue ricorsivamente quattro operazioni (sino ad un massimo di `maxIterations` iterazioni stabilito a priori). I nodi di non ambiguità, etichettati con `true`, inviano messaggi ai vicini di tipo v1 e di tipo v1-1 (qualora etichettati con `false`), ricostruendo a piccoli passi i percorsi ovvi. Il nodo di partenza di ogni percorso riceve assegnato l'indice di gruppo zero, quindi invia il proprio Vertex-ID ai nodi che seguono la catena ovvia (il Vertex-ID del nodo di partenza è il group ID del gruppo di nodi che si sta formando); ad ogni scambio di messaggi l'indice di gruppo viene incrementato di una unità e la polarità dei nodi aggiornata congruamente alla direzione del percorso. Una volta raggiunto il numero massimo di iterazioni, oppure non presentandosi più la condizioni per lo scambio di messaggi, la funzione Pregel si interrompe e l'algoritmo restituisce una `RDD` di vertici aggiornati ed una `RDD` di nuovi archi.

##### Generazione degli archi

Partendo dalla `EdgeRDD` ricavata dal grafo di partenza DBG, si origina un sottografo in cui tutti gli archi che collegano nodi appartenenti al medesimo gruppo sono eliminati. Dal grafo ridotto vengono quindi creati nuovi archi. I vertici di partenza di ogni gruppo mantengono i loro archi incidenti sui vertici esterni al gruppo. Gli archi che collegano vertici non raggruppati vengono mantenuti con le stesse proprietà. Tutti gli archi che collegano le code a vertici esterni al gruppo ora ricevono collegamento alla testa del gruppo (poiché la testa del gruppo definisce l'ID del gruppo).
È importante che i valori di polarità dei nuovi archi siano coerenti con i cambiamenti nei vertici del grafo e la direzione della fusione del gruppo; pertanto tutte le casistiche di fusione sono controllate nel dettaglio dall'algoritmo. 
L'algoritmo conclude restituendo una `JavaRDD` di vertici, punto di partenza per la costruzione del grafo semplificato (protagonista della successiva iterazione).

##### Generazione dei vertici

I vertici aggiornati, restituiti in una `VertexRDD`, subiscono una operazione di `map` tale da produrre una `RDD` di oggetti di tipo `Vertex`. Un oggetto di tipo `Vertex` raccoglie tutte le informazioni di interesse quali:
1. `id`, valore codificato come `long`
2. `gid`, group id (`long`)
3. `index`, indice di merging (`int`)
4. `degree`, grado del vertice (`int`)
5. `pol`, polarità del vertice (`"L"` oppure `"H"`)

L'`RDD` ottenuta viene trasformata in una `JavaRDD`. Mediante operazione di `join` i vertici aggiornati condividono il proprio contenuto informativo con i contigs.
Segue una operazione di raggruppamento (`groupBy`) secondo il group ID, quindi di riordinamento (in ordine ascendente) secondo l'indice di gruppo; le sequenze nucleotidiche sono quindi assemblate (nel rispetto della polarità rilevate sui vertici). Il procedimento genera, a partire da ogni gruppo, una nuova `JavaRDD<Contig>`. Nel caso in cui un vertice non faccia parte di alcun gruppo, il contig ad esso associato viene lasciato intatto.

### Implementazione dei grafi in Neo4j

Mediante il metodo `neo` definito nella classe `Neo4j.java` è possibile implementare il grafo ottenuto ad ogni iterazioni su Neo4j; il tutto ponendo valore `true` alla variabile preposta ed inserendo le apposite credenziali nella classe `Neo4j.java`.
```
boolean Neo = false;
```

### Datasets
* Dataset `Toy.txt` (utile per il controllo del funzionamento dell'algoritmo)
* Dataset `papaya.fasta` (5661 reads)
* Dataset `grandis.fasta` (10000 reads)

Segue presentazione riassuntiva dei risultati sperimentali ottenuti.

|               | Nr.reads | k  | Nr.kmer | cores | Iterations | Nr.contigs | Time (sec)   |
|---------------|----------|----|---------|-------|:----------:|------------|--------------|
| papaya.fasta  | 5661     | 20 | 714969  | 8     | 10         | 303477     | 128          |
| grandis.fasta | 10000    | 20 | 1289109 | 2     | 10         | 546142     | 522 (8'42'') |
| grandis.fasta | 10000    | 20 | 1289109 | 8     | 10         | 546191     | 345 (5'45'') |


package be.hogent.dit.tin;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * 1. Mapper schrijven: Voor ieder woord moet een key teruggegeven worden.
 * 2. Combiner schrijven.
 */


public class InvertedIndex {
	

	/*
	 * Mapper:
	 * Je mag dezelfde typen overnemen. Je krijgt een long binnen en je produceert tekst.
	 * (word@fileName, 1).
	 * ONE maak je aan en zal je meermaals opnieuw gebruiken.
	 */
	public static class InvertedIndexMapper 
		extends Mapper<LongWritable, Text, Text, LongWritable>{
		
		private static final LongWritable ONE = new LongWritable(1);
		

		/*
		 * Map-methode: Je krijgt een longwritable, de tekst en de context binnen.
		 * De foutafhandeling gebeurt pas op het einde.
		 */
		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			
			/*
			 * Bepaal de bestandsnaam met context.getInputSplit().
			 * Met .getInputSplit krijg je een array van waarden terug. De split sla je op onder een FileSplit-object.
			 * Je kan het pad (.getPath als Path-object) en de filename (.getName als String-object) opslaan onder twee variabelen. 
			 */

			FileSplit filesplit = (FileSplit) context.getInputSplit();
			Path path = filesplit.getPath();
			String filename = path.getName();
			
			/*
			 * We willen itereren over iedere token of woord in een bestand. Hiervoor moeten we een StringTokenizer maken van de value. 
			 * De StringTokenizer zal dienen als een soort van array.
			 * Voor iedere token (met voorkeur werk je met een while-lus) ga je het 'woord@file' gaan uitprinten.
			 * Dit schrijf je uit naar de context.
			 *  
			*/
			StringTokenizer itr = new StringTokenizer(value.toString());
			
			while(itr.hasMoreTokens()) {

				String keyToWrite = itr.nextToken().toLowerCase() + "@" + filename;
				context.write(new Text(keyToWrite), ONE);
			}
		}
	}
	
	
	/*
	 * Combiner: Alle 1'n optellen om het verkeer te verminderen.
	 * (key, <1, 1, ..., 1>) --< (key, n) waar n het aantal 1'n is.
	 */
	public static class InvertedIndexCombiner
		extends Reducer<Text, LongWritable, Text, LongWritable>{
		
			/*
			 * De reduce-functie gaat itereren over het aantal waarden.
			 * Met een teller gaan we telkens + 1 doen.
			 * Het eindresultaat is n ofwel het totaal aantal waarden.
			 * Uiteindelijk schrijven we het woord + het totaal aantal 1'n uit naar de context.
			 */
		@Override
		public void reduce(Text word, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException{
			long total = 0;
			for(LongWritable value : values) {
				total += value.get();
			}
			
			context.write(word,  new LongWritable(total));
			
		}
	}
	
	
	/*
	 * Partitioner: Het voorkomt dat zelfde woorden op dezelfde reducer kan terechtkomen. 
	 * We willen enkel het woord hashen. In ons geval zijn de keys: Text & LongWritable.
	 * 
	 * numReducers: het aantal partities. Dit gaan we gebruiken voor de hashing.
	 * getPartition-methode: We zoeken in de String naar een "@". Daar splitsen we de String in een array. Daar nemen we enkel het eerste deel.
	 * word@fileName --> word --> hash(word)
	 * 
	 * s.hashcode() hashet het woord
	 * Integer.MAX_VALUE is het grootst mogelijke Integer/getal in Java. Dit gebruiken we om een hash te maken dat niet groter is dan dat.
	 * (..) % numReduceTasks is om te hashen over het aantal partities.
	 */
	public class InvertedIndexPartitioner<Text, LongWritable> extends Partitioner<Text, LongWritable>{
		public int getPartition(Text key, LongWritable value, int numReduceTasks) {
			String s = key.toString().split("@")[0];
			return (s.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
		}
	}
	
	
	/*
	 * Key: Text --> word@filename
	 * Iterable<LongWritable>: Het aantal voorkomens
	 * Context: eventuele zaken uitschrijven.
	 */
	public static class InvertedIndexReducer extends Reducer<Text, LongWritable, Text, Text>{
		private String previousWord = null;
		private StringBuilder outString = new StringBuilder();
		
		
		/*
		 * 1. 	We splitsen de key in twee delen: het woord en de filename. (Deel voor de "@" + Deel na de "@")
		 * 
		 * 2. 	Alle long waarden moeten worden opgeteld. Hier moeten we rekening houden dat alle woorden in een gesorteerde volgorde zullen voorkomen.
		 * 		Dit wilt zeggen dat we enkel pas naar de context mogen uitschrijwoordven wanneer het huidige woord niet gelijk is aan het vorige woord.
		 * 		Rekening houden met het vorige woord --> extra variabele previousWord. Op de eerste lijn zal er geen vorig woord zijn, dus de variabele start met een null-waarde.
		 * 		String opbouwen, vb.: 'woord	file1.txt:2;file2.txt:1' --> StringBuilder
		 * 
		 * 3. 	De if gaat kijken of het vorige woord anders is dan het huidige woord. Hier moeten we starten met een check of previousWord niet null is. Als we de check niet doen zal het programma crashen.
		 * 		Eerst print je het woord uit naar de context. Daarna print je de string uit dat je via de StringBuilder had opgebouwd.
		 * 		(length() - 1) --> De puntkomma willen we weg. Het laatste teken snijden we af.
		 */
		@Override
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			String word = key.toString().split("@")[0];
			String filename = key.toString().split("@")[1];
			
			long sum = 0;
			
			if(previousWord != null && !previousWord.equals(word)) { // nieuw woord gezien
				outString.setLength(outString.length() - 1);
				context.write(new Text(previousWord), new Text(outString.toString()));
				
			}
			
			/*
			 * 1.	Overloop alle voorkomens van het woord. In principe doet de combiner dit, maar je moet nog steeds rekening houden met een situatie waar je meerdere long-waarden krijgt.
			 *		Ieder voorkomen tel je op.
			 *		Vb.: values = <1, 1, 1> of <4,5,2>
			 *
			 * 2. 	De outputstring voor ieder bestand gaan we aanmaken. We voegen de bestandsnaam + het aantal voorkomens samen. Eindigen doen we met een puntkomma.
			 * 
			 * 3.	
			 * 
			 */
			for (LongWritable value:values) {
				sum += value.get();
			}
			
			outString.append(filename + ":" + sum + ";");
			
			previousWord = word;
		}
		
		/*
			Deze methode hebben we nodig omdat het allerlaatste woord nooit uitgeschreven zal worden.
			Je neemt de code van hierboven over. Enkel voeg je de cleanup van de klasse over.
		 */	
		public void cleanup(Context context) throws IOException, InterruptedException{
			outString.setLength(outString.length() - 1);
			context.write(new Text(previousWord), new Text(outString.toString()));
			super.cleanup(context);
		}
	}
	
	/*
	 * Geen gegeven bij deze oefening, maar zal wel worden gegeven op het examen (indien dat het geval is).
	 * Het merendeel mag je overnemen uit de WordCount oefening.
	 */
	public static void main(String[] args) throws Exception {
		if(args.length != 2) {
			System.out.println("Usage: Nuclear meltdown. Usage: <input_dir> <output_dir>");
			System.exit(-1);
		}
		
		//instance of job maken
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "WordCount");
		
		// moet je altijd doen als je ...
		job.setJarByClass(InvertedIndex.class);
		
		// set mapper and reducer
		job.setMapperClass(InvertedIndexMapper.class);
		job.setReducerClass(InvertedIndexReducer.class);
		
		// set combiner
		job.setCombinerClass(InvertedIndexCombiner.class);
		job.setPartitionerClass(InvertedIndexPartitioner.class);
		
		//set input and output types
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		//set input and output directory
		// waar zijn de input-files?
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		// waar zijn de outputfiles? 
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// voor het testen
		job.setNumReduceTasks(0);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);		
	}
}
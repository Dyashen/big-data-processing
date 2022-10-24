package be.hogent.dit.tin;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class WordCount {
	
	/*
	* Mapper schrijven --> binnenin de mapper --> soorten
	* < type van de sleutel, type van de tekst, type van de sleutel van de uitvoer (tekst is de uitvoer), waarde van de sleutel vd uitvoer (getal) >
	* long ==> longwritable
	* int ==> intwritable
	* String ==> Text
	*/
	public static class WordCountMapper 
	extends Mapper<LongWritable, Text,  Text, LongWritable>{
		
		// voorkomen dat een constructor telkens op lijn lijn 48 wordt aangemaakt en dan weer vernietigd
		private static LongWritable ONE = new LongWritable(1);
		
		@Override
		/*
		 * De sleutel en de tekst (in de bestanden) zijn hier de parameters die we overnemen uit de Mapperklasse.
		 * Er komt een derde parameter bij: context. Dit laat ons toe om zaken weg te schrijven naar de disk.
		 */
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			/*
			 * De sleutel gaan we niet gebruiken
			 * De Value is één lijn uit een invoerbestand. --> omzetten naar een array van Strings met toString() & alle whitespace gebruiken als delimiter
			 * Alle tokens overlopen met een enhanced foreach --> elk woord wegschrijven naar de context. --> casten of new Text(...) gebruiken.
			 * Ieder woord 'toevoegen' of wegschrijven naar de context.
			 */
			String[] words = value.toString().split(" ");
			for (String word : words) {
				context.write(new Text(word), ONE);
			}
		}
	}
	
	
	/*
	 * Reducer schrijven --> alles wat uit de mapper komt moet overeenkomen met de parameters hier. 
	 * Zelfde vier typen.
	 */
	public static class WordCountReducer
	extends Reducer<Text, LongWritable,  Text, LongWritable>{
		@Override
		/*
		 * 
		 * We geven het woord, de logs en de context mee van de mapper.
		 *  We overlopen de counts en we zorgen voor één getal: 
		 * 
		 */
		public void reduce(Text word, Iterable<LongWritable> counts, Context context) throws IOException, InterruptedException {
			long total = 0;
			
			// totaal gaan optellen met het aantal waarden
			// total += value is eerst incompatibel --> de long 'halen'
			for (LongWritable value : counts) {
				total += value.get();
			}
			
			context.write(word, new LongWritable(total));
		}
	}
	
	// Boilerplate-code
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		if(args.length != 2) {
			System.out.println("Usage: Nuclear meltdown. Usage: <input_dir> <output_dir>");
			System.exit(-1);
		}
		
		//instance of job maken
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "WordCount");
		
		// moet je altijd doen als je ...
		job.setJarByClass(WordCount.class);
		
		// set mapper and reducer
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		// set combiner
		job.setCombinerClass(WordCountReducer.class);
		
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
		
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);		
	}
}

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


public class InvertedIndex {
	
	public static class InvertedIndexMapper 
		extends Mapper<LongWritable, Text, Text, LongWritable>{
		
		private static final LongWritable ONE = new LongWritable(1);
		
		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			
			/* Bepaal de bestandsnaam */
			FileSplit filesplit = (FileSplit) context.getInputSplit();
			Path path = filesplit.getPath();
			String filename = path.getName();
			
			/*...*/
			StringTokenizer itr = new StringTokenizer(value.toString());
			
			while(itr.hasMoreTokens()) {
				String keyToWrite = itr.nextToken().toLowerCase() + "@" + filename;
				context.write(new Text(keyToWrite), ONE);
			}
		}
	}
	
	
	// Combiner:
	// Alle '1's optellen om het verkeer te verminderen.
	public static class InvertedIndexCombiner
		extends Reducer<Text, LongWritable, Text, LongWritable>{
		
		@Override
		public void reduce(Text word, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException{
			long total = 0;
			for(LongWritable value : values) {
				total += value.get();
			}
			
			context.write(word,  new LongWritable(total));
			
		}
	}
	
	// Partitioner: (woord@filename) enkel 'word' hashen
	public class InvertedIndexPartitioner<Text, LongWritable> extends Partitioner<Text, LongWritable>{
		public int getPartition(Text key, LongWritable value, int numReduceTasks) {
			String s = key.toString().split("@")[0];
			return (s.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
		}
	}
	
	
	// Reducer
	public static class InvertedIndexReducer extends Reducer<Text, LongWritable, Text, Text>{
		
		private String previousWord = null;
		private StringBuilder outString = new StringBuilder();
		
		
		// key: word@filename
		@Override
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			String word = key.toString().split("@")[0];
			String filename = key.toString().split("@")[1];
			
			long sum = 0;
			
			if(previousWord != null && !previousWord.equals(word)) { // nieuw woord gezien
				outString.setLength(outString.length() - 1);
				context.write(new Text(previousWord), new Text(outString.toString()));
				
			}
			
			//bezig met huidig woord
			for (LongWritable value:values) {
				sum += value.get();
			}
			
			// outstring aanpassen
			outString.append(filename + ":" + sum + ";");
			
			// previousword aanpassen
			previousWord = word;
		}
		
		
		public void cleanup(Context context) throws IOException, InterruptedException{
			outString.setLength(outString.length() - 1);
			context.write(new Text(previousWord), new Text(outString.toString()));
			super.cleanup(context);
		}
	}
	
	

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
package be.hogent.dit.tin;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Spliterator;
import java.util.stream.Stream;

/**
 * Take a (large) file and split it into a number of files and copy these (slowly) into 
 * a target directory.
 * 
 * @author Stijn Lievens
 */
public class CopyFileSlowly {

	public static void main(String[] args) throws FileNotFoundException, IOException, InterruptedException {
		if (args.length != 4) {
			System.out.println("Usage: CopyFileSlowly <inputFile> <outputDir> <numLinesPerFile> <sleep milliseconds>");
			System.exit(-1);
		}
		
		final String inputFile = args[0];
		final String outputDirectory = args[1];
		final int numLinesPerFile = Integer.parseInt(args[2]);
		final int sleepMilliseconds = Integer.parseInt(args[3]);
						
		int fileNumber = 0;
		try (BufferedReader reader = new BufferedReader(new FileReader(inputFile)) ) {
			
			// https://stackoverflow.com/questions/27583623/is-there-an-elegant-way-to-process-a-stream-in-chunks
			Stream<String> lines = reader.lines();
			Spliterator<String> split = lines.spliterator();
						
			while (true) {
				List<String> chunk = new ArrayList<>(numLinesPerFile);
				for (int i = 0; i < numLinesPerFile && split.tryAdvance(chunk::add); i++) {}
				if (chunk.isEmpty()) {
					break;
				}
				
				final String outputFile = outputDirectory + "/" + fileNumber + ".txt";
				try (PrintWriter out = new PrintWriter(outputFile)) {
					for (String line : chunk) {
						out.println(line);
					}
				}
								
				fileNumber++;
				Thread.sleep(sleepMilliseconds);
							
			}								
		}
	}

}

package be.hogent.dit.tin;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Random;

/**
 * Each call to <code>next</code> generates a new log message.
 * This message is a <code>String</code> that is meant to resemble
 * a log message that could be produced by a logging framework.
 * 
 * @author Stijn Lievens
 *
 */
public class LogMessageGenerator {
	
	private static final String [] LOGLEVELS = new String [] {
			"DEBUG", "INFO", "WARN", "ERROR", "FATAL"}; 
	
	private static final String [] SOURCES = new String [] {
			"web", "e-mail", "database", "firewall", "ml-model" 
	};
	
	private static double [] CUMULATIVE_PROB = new double [] {
			0.5, 0.7, 0.85, 0.95, 1.0};
	
	private final Random random = new Random();
	
	private long counter = 0;
	
	public String next() {
		double p = random.nextDouble();
		String message = LOGLEVELS[4];
		
		if (p <= CUMULATIVE_PROB[0]) {
			message = LOGLEVELS[0];
		} else if (p <= CUMULATIVE_PROB[1]) {
			message = LOGLEVELS[1];
		} else if (p <= CUMULATIVE_PROB[2]) {
			message = LOGLEVELS[2];
		} else if (p <= CUMULATIVE_PROB[3]) {
			message = LOGLEVELS[3];
		}
		
		int source = random.nextInt(SOURCES.length);
		
		message = message + " "
				+ SOURCES[source] + " "
				+ LocalDate.now() + " "
				+ LocalTime.now().toString() 
				+ " " + "This is message " + counter;
		counter++;
	
		return message;	
	}
	
	public static void main(String [] args) throws InterruptedException {
		LogMessageGenerator generator = new LogMessageGenerator();
		for (int i = 0; i < 100; i++) {
			System.out.println(generator.next());
			
			Thread.sleep(200);
		}
	}
}


package be.hogent.dit.tin;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Class that shows how to use an AtomicBoolean to stop an 
 * otherwise infinite loop.
 * 
 * @author Stijn Lievens
 *
 */
public class ShutDownHookExample2 {

	public static void main(String[] args) {
		
		AtomicBoolean stopRequested = new AtomicBoolean(false); 
				
		final Thread mainThread = Thread.currentThread();
		System.out.println("The name of the main thread is: " 
				+ mainThread.getName());
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				System.out.println("Shutdownhook is executing in thread:" 
						+ Thread.currentThread().getName());
				stopRequested.set(true);
				System.out.println("Waiting for main thread");
				try {
					mainThread.join(); // wait for the main Thread to finish
				} catch (InterruptedException e) {					
					e.printStackTrace();
				} finally {
					System.out.println("Done waiting for main thread");
				}
				
			}
		});
		
		while (!stopRequested.get()) {
			try {
				System.out.println("Going to sleep for 5 seconds");
				Thread.sleep(5000); // sleep for 5 seconds				
			} catch (InterruptedException e) {		
				e.printStackTrace();
			} 
		}
	}
}

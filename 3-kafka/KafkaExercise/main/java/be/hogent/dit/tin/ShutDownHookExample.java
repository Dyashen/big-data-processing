package be.hogent.dit.tin;

/**
 * Class to show that the shutdownhook runs in a different thread than the main 
 * thread.
 * 
 * @author Stijn Lievens
 */
public class ShutDownHookExample {

	public static void main(String[] args) {
				
		// Print the name of the thread the main-method is running in
		System.out.println("The name of the main thread is: " 
				+ Thread.currentThread().getName());
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				// Print the name of the thread the shutdownhook is running in
				System.out.println("Shutdownhook is executing in thread:" 
						+ Thread.currentThread().getName());
			}
		});
		
		while (true) {
			// do nothing
		}
	}
}
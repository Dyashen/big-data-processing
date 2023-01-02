package be.hogent.dit.tin;

public class ShutdownHookExample {

	public static void main(String[] args) {

		System.out.println("Main method is running in: " + Thread.currentThread().getName());
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
			
			@Override
			public void run() {
				System.out.println("Shutdownhook running in:" + Thread.currentThread().getName());
			}
			
			
				});
		
		while(true) {
			
		}
		
	}

}

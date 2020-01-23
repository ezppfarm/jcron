package farm.ezpp.cron;

public class CronTask {
	
	private boolean done;
	private DoneTask dt;
	
	public void task() {}
	
	public boolean isDone() {
		return done;
	}
	
	public void onDone(DoneTask dt) {
		this.dt = dt;
	}
	
	public void markDone() {
		done = true;
		dt.run();
	}

}

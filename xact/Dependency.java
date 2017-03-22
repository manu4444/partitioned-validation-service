package xact;

public class Dependency {
	private int toTID;
	private int fromTID;
	
	public Dependency(int from, int to){
		this.toTID = to;
		this.fromTID = from;
	}

	public int getToTID() {
		return toTID;
	}

	public int getFromTID() {
		return fromTID;
	}
	
	public boolean equals(Object obj){
		if(obj instanceof Dependency){
			Dependency dep = (Dependency) obj;
			return (dep.getFromTID()==this.fromTID) && (dep.getToTID()==this.toTID);
		}else
			return false;
	}
	
	public String toString(){
		StringBuffer buf = new StringBuffer();
		buf.append("[T");
		buf.append(fromTID);
		buf.append("->T");
		buf.append(toTID);
		buf.append("]");
		
		return buf.toString();
	}
}

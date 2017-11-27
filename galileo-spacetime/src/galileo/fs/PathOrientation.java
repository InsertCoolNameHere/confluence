package galileo.fs;

import java.util.Set;
import java.util.TreeSet;

public class PathOrientation implements Comparable<PathOrientation>{
	
	private String path;
	private String orientation;
	
	public PathOrientation(String path, String orientation) {
		
		this.path = path;
		this.orientation = orientation;
		
	}
	
	public int compareTo(PathOrientation p)
	{
	    //FoodItems temp = (FoodItems) o;
	    if(this.path != null && this.orientation != null && this.path.equals(p.getPath()) && this.orientation.equals(p.getOrientation()))
	        return 0;
	    else 
	        return -1;
	}
	
	@Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null) {
            return false;
        }

        if (getClass() != obj.getClass()) {
            return false;
        }

        PathOrientation p = (PathOrientation) obj;
        if(this.path != null && this.orientation != null && this.path.equals(p.getPath()) && this.orientation.equals(p.getOrientation()))
	        return true;

        return false;
    }
	
	
	public String getPath() {
		return path;
	}
	public void setPath(String path) {
		this.path = path;
	}
	public String getOrientation() {
		return orientation;
	}
	public void setOrientation(String orientation) {
		this.orientation = orientation;
	}
	
	@Override
    public int hashCode() {
        final int prime = 773;
        int result = 1;
        result = prime * result + ((path == null) ? 0 : path.hashCode());
        result = prime * result + ((orientation == null) ? 0 : orientation.hashCode());
        return result;
    }
	
	public static void main(String arg[]) {
		
		String s = "Hi";
		String s1 = "Hello";
		
		PathOrientation p1 = new PathOrientation(s, s1);
		PathOrientation p2 = new PathOrientation(s, s1);
		
		Set<PathOrientation> ps = new TreeSet<PathOrientation>();
		
		ps.add(p1);
		ps.add(p2);
		
		System.out.println(p1);
		System.out.println(p2);
		
		System.out.println(ps);
		
		System.out.println(p1.equals(p2));
		
	}
	

}

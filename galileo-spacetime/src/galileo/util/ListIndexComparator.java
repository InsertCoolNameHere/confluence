package galileo.util;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class ListIndexComparator implements Comparator<Integer>
{
    private final List<Double> myList;
    private final List<Integer> indexArray;

    public ListIndexComparator(List<Double> array, List<Integer> indexArray)
    {
        this.myList = array;
        this.indexArray = new ArrayList<Integer>(indexArray);
    }

    /*public Integer[] createIndexArray()
    {
        Integer[] indexes = new Integer[array.length];
        for (int i = 0; i < array.length; i++)
        {
            indexes[i] = i; // Autoboxing
        }
        return indexes;
    }*/

    @Override
    public int compare(Integer index1, Integer index2)
    {
    	int actIndex1 = indexArray.indexOf(index1);
    	int actIndex2 = indexArray.indexOf(index2);
         // Autounbox from Integer to int to use as array indexes
    	int ret = 0;
    	if(myList.get(actIndex1) > myList.get(actIndex2)) {
    		ret = 1;
    	} else if(myList.get(actIndex1) < myList.get(actIndex2)) {
    		ret = -1;
    	}
    	
        return ret;
        
    }
}
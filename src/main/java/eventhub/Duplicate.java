package eventhub;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Duplicate {

	 public static void main(String[] args) {
         // TODO Auto-generated method stub
      List<Integer> numList = new ArrayList<Integer>();
      numList.add(100);
      numList.add(20);
      numList.add(30);
      numList.add(60);
      numList.add(40);
     // Collections.sort(numList);
      Collections.sort(numList);
     // numList.stream().forEach(System.out::println);
      int len = numList.size();
      for(int i=0;i<2;i++) {
    	  Collections.sort(numList);
    	  
    	  numList.set(len-1, numList.get(len-1)/2);
    	//  System.out.println(numList.get(len-1));
    	//  System.out.println(numList.size());
      }
      Collections.sort(numList);
     int s = numList.stream().mapToInt(Integer::intValue).sum();
     System.out.println(s);
      
}
}
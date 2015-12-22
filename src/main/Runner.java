package main;

public class Runner {
  public static void main(String args[]){
	  if(args.length==1){
		Main.main(args);  
	  }else{
		  System.err.println("Arguments missing from command");
	  }
	  
  }
}

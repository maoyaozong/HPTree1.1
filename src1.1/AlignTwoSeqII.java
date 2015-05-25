
public class AlignTwoSeqII {
	String s1,s2;
  public AlignTwoSeqII(String s1, String s2){
	  this.s1=s1;
	  this.s2=s2;
  }
  
  public static int Get_Direction(int a, int b , int c,int current_node){
		int direction=0;
		if(current_node == a){
			direction = 1;
		}
		else if (current_node == b){
			direction = 2;
		}
		else {
			direction =3;
		}
		return direction;
	}
  
  public static int Max(int a, int b, int c){
		int max=-100000;
		if(a>max) max=a;
		if(b>max) max=b;
		if(c>max) max=c;
		return max;
	}
  
  public static String Alignment(String s1, String s2){
      int length_1 =  s1.length();
      int length_2 = s2.length();
      int [][] matrix = new int[length_1+1][length_2+1];
      
      matrix[0][0]=0;
      for(int i=1;i<length_2+1;i++)
      	matrix[0][i] = -2*i;
      for (int i=1;i<length_1+1;i++)
      	matrix[i][0] = -2*i;
      
      for(int i=1;i<length_1+1;i++){
      	for (int j=1;j<length_2+1;j++){
      		int score , gap = -2;
      		if(s1.charAt(i-1) != s2.charAt(j-1)){
      			score = -1;
      		}
      		else{
      			score = 1;
      		}
      		matrix[i][j] = Max(matrix[i-1][j-1]+score, matrix[i-1][j]+gap,matrix[i][j-1]+gap);
      	}
      }
      
     /* for (int i=0;i< length_1+1;i++){
      	for(int j =0;j<length_2+1;j++){
      		System.out.print(matrix[i][j]+" ");
      	}
        System.out.println();
      } */
      
      int i = length_1;
      int j = length_2;
      int current_node = matrix[length_1][length_2];
      StringBuffer sb_1 = new StringBuffer(s1);
      StringBuffer sb_2 = new StringBuffer(s2);
      while(i>0 && j>0){
      	//System.out.println(current_node);
      	int score;
      	if(s1.charAt(i-1)!=s2.charAt(j-1)){
      		score = -1;
      	}
      	else{
      		score = 1;
      	}
      	int direction = Get_Direction(matrix[i-1][j-1]+score,matrix[i-1][j]-2,matrix[i][j-1]-2,current_node);
      	
      	if (direction ==1){ 
      		current_node = matrix[i-1][j-1];
      		i=i-1;
      		j=j-1;
      	}
      	else if (direction == 2){
      		sb_2.insert(j, '-'); 
      		current_node = matrix[i-1][j];
      		i =i-1;
      	}
      	else if (direction == 3){
      		sb_1.insert(i, '-'); 
      		current_node = matrix[i][j-1];
      		j=j-1;
      	} 
      }
      int align_length_1=sb_1.length(); 
      int align_length_2=sb_2.length(); 
      if( align_length_1> align_length_2){
      	int abs = align_length_1 - align_length_2;
      	while(abs>0){
      		sb_2.insert(0, '-');
      		abs--;
      	}
      }
      else if (align_length_1< align_length_2){
      	int abs =align_length_2-align_length_1;
      	while(abs>0){
      		sb_1.insert(0,'-');
      		abs--;
      	}
      }
      return sb_2.toString();
    // return sb_1.toString()+"\t"+sb_2.toString();
}
  
  //the s1 is the start  
  String  run(){ 
	  return Alignment(this.s1,this.s2);
	  //String [] aligns = align.split("\t");
  }
}

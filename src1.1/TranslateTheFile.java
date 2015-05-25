import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
//import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;


public class TranslateTheFile {//这个类是在整个工程之前输入的，主要是对文件进行修正，删除非法字符和格式转换 , 最后文件被整合成一行 name ：seq
	static String seq1;//用来保存中心序列1
	String inputfile;
	String outputfile;
	private int line_number;
	public TranslateTheFile(String inputfile,String outputfile){//构造函数获取输入文件的路径
    	 this.inputfile=inputfile;//输入文件的路径
    	 this.outputfile=outputfile;//输出文件的路径
     }
        
     public void translate() throws IOException{
    	 //下面的代码计算count条序列的平均长度，便于后面删除太长或者太短的序列
    	 BufferedReader br1=new BufferedReader(new FileReader(inputfile));//读取文件 
    	 String line1="";
    	 String cur1="";
    	 int average_length=0;//用来记录平均的长度
    	 int count=0;//这里没有计算全部序列的平均长度，而是计算count条序列的平均长度
    	 line1=br1.readLine();
    	 while(br1.ready()&&count<1000){
    		 while(line1.length()==0&&br1.ready()){ //删除序列之间的空行
      			  line1=br1.readLine();
      		  }
    		 if(line1.charAt(0)=='>')//如果第一个字符是>的话，标识这是个序列名
    			 line1=br1.readLine();//line1读取序列名的下一行
    		 while(line1.length()==0&&br1.ready()){//删除空行
     			  line1=br1.readLine();
     		  }
    		 while(line1.length()!=0 && line1.charAt(0)!='>'){//当line1不为空，而且第一个字符不是>的时候，把后面的合成一个大长条  , 当读到序列名的时候退出 
    	   			  cur1=cur1+line1;//合成大长条
    	   			  line1=br1.readLine();//读下一行
    	   			  if(!br1.ready())
    	   				  break; 
    	   			  while(line1.length()==0&&br1.ready()){//删除空行
    	   				  line1=br1.readLine();
    	   			  } 
    	   			if(!br1.ready())
  	   				  break; 
    	   		  }	  
    		 average_length+=cur1.length();//获取这大长条的长度
    		 cur1=""; 
    		 count++;//累加序列条数
    	 } 
    	 average_length=average_length/count;//计算平均长度
    	 System.out.println("the average_length is "+average_length );
    	 
		 
    	 BufferedReader br=new BufferedReader(new FileReader(inputfile));//读取输入文件
    	 BufferedWriter bw=new BufferedWriter(new FileWriter(outputfile)); //输出文件存放位置
   	  String line="";
   	  String wline="";
   	  String cur="";
   	  String prefix="";
   	  line=br.readLine();
   	  
   	  while(br.ready()){ 
   		  
   		 while(line.length()==0&&br.ready()){//删除空行
   			  line=br.readLine();
   		  }
   		  if(line.charAt(0)=='>'){//当读到序列名行的时候
   			  prefix=line; //把序列名保存在prefix中
   			  line=br.readLine(); //读取下一行
   			  line_number++;
   		  } 
   		 while(line.length()==0&&br.ready()){//防止序列名和序列之间有空行，删除空行 
  			  line=br.readLine();
  		  }
   		while(line.length()!=0 && line.charAt(0)!='>'){ //下面的代码把是删除非法字符的，同时把很多行的序列整成一个大长条
   			line=line.toUpperCase();//首先把所有序列的字符弄成大写字符 
   			for(int i=0;i<line.length();i++){  //然后遍历序列的每个字符			             		 
      			if(line.charAt(i)!='A'&&line.charAt(i)!='T'&&line.charAt(i)!='C'&&line.charAt(i)!='G'){ //当不是ATCG里面的字符的时候，就删掉
      				if(i==line.length()-1){//当待删除的是子串最后一个字符时 
      					if(line.length()==1){// 当字符是最后一个字符同时也是第一个字符，即只有一个字符的时候 
      						line="";//把这个Line弄成空
      					}
      					else{//当这个字符是最后一个字符，同时这个串不只一个字符的时候
      					line=line.substring(0,i);// 获取序列的前面的子串
      					}
      				}//最后一个字符if
      				else if(i==0){//当待删除的不是最后一个，而是第一个字符的时候
      					line=line.substring(i+1);// 获取后面的子串 
      				}
      				else{//待删除的字符是中间字符的时候
      					line=line.substring(0,i)+line.substring(i+1,line.length());//获取前面和后面的子串
      					i=i-1; //注意i的下标
      				   }
      		        }
      	        }//序列的某一行就处理完了，后面要把一对小序列整合成一个大长条
   			 
   			  cur=cur+line;//整合成一个大长条
   			  line=br.readLine();//读下一行
   			  if(!br.ready())
   				  break; 
   			  while(line.length()==0&&br.ready()){//删除空行
   				  line=br.readLine();
   			  } 
   		  }		
   		//后面是删除太长或者是太短的序列 , the range is 0.75*average< or >1.25*average
   		 if (cur.length()<0.75*average_length || cur.length()>1.25*average_length){ 
   			 cur=""; //删除序列就把这个序列弄成空
   			 line=br.readLine();//读下一行
   		 }	
   		
   	  if(cur.length()!=0){//当不为空的时候
   		  wline=prefix+"\t"+cur; //把序列名和序列整合成一行 name ：seq  
   		  cur="";
   		  bw.write(wline);
   		  bw.newLine();
   		  bw.flush();
   		  }   
   	  }//end while 全部处理完了
   	  bw.close();
   	  br.close(); 
     	 
    //下面要获取中心序列
         System.out.println("Translate OK ");
     	/* br=new BufferedReader(new FileReader(outputfile));
     	 line=br.readLine();//读取第一行的中心序列
     	 int p1;
     	for(p1=0;p1<line.length();p1++){//把序列名拆开来
    		if(line.charAt(p1)!=':');
    		else break;
    	}		    
       br.close();
       
    	seq1=line.substring(p1+1);//不加1的话会连冒号一起弄进来
    	if(seq1==null)
    		System.out.println("In the TranslateTheFile the seq1 is null"); */
     }
     
     public int Get_line_number(){
    	 return line_number;
     }
}

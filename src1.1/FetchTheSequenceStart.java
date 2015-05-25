import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;


public class FetchTheSequenceStart {//这个类汇总中心序列
	   static String s1;//静态变量用来保存中心序列
	String theStartPath;//这个变量用来保存中心序列的文件路径
	public FetchTheSequenceStart(String thepath){//构造函数把中心文件的路径赋值
		this.theStartPath=thepath;
	}
	
	public void  combiner(String local_path) throws IOException{//这里进行汇总
	int[] Space=new int[TranslateTheFile.seq1.length()+1];//中心序列为length，一共有length+1个空隙
	BufferedReader br=new BufferedReader(new FileReader(theStartPath));//读取中心序列文件
	String line;
	
	while(br.ready()){
		
	    line = br.readLine();
	    String[] tempstring=line.split("\t");//中心序列文件的数据格式是， star_seq \t new_seq
	 	String seq=tempstring[2];//把new_seq赋值给seq
	 	String seqi=tempstring[1];//获取中心序列
	 
	 	FileWriter fw = new FileWriter(local_path+"/seqi", true);//把待比对序列写到这个文件里头去，设置为可以追加的格式
		   // 追加内容   
		   fw.write(seqi+"\r\n");
		   // 关闭文件输出流
		   fw.close();
	 	
		   int index=0;
	 	int Spaceevery[] = new int[TranslateTheFile.seq1.length()+1];//计算这条new_star的插入空格是咋样的
	 	for(int j=0;j<seq.length();j++){//遍历new_star的每个字符
	 		if(seq.charAt(j)=='-')//如果这个字符是空格，那就记录下空格的数目
	 			Spaceevery[index]++;
	 		else{//如果这个字符不是空格，那么相应的空格数组就向后走一位
	 			if(Spaceevery[index]>Space[index]) //同时判断是不是比最大的还要的
                    Space[index]=Spaceevery[index]; //是的话就更新最大的
                    index++;
	 		}
	 	}	 	
	}
	br.close();
	
	
	StringBuffer sb = new StringBuffer();
    for(int i=0;i<Space.length;i++){//遍历插入空格的数组
    	/*if (Space[i]==1 || Space[i]==2)
    	{
    		Random random =new Random();
    		double ran =random.nextDouble();
    		if (ran<0.7)
    			if(i!=Space.length-1)
    		         sb.append(TranslateTheFile.seq1.charAt(i));
    			continue;    		
    	}*/
        for(int j=0;j<Space[i];j++)//计算每个空隙的空格数
            sb.append('-');
        if(i!=Space.length-1)
         sb.append(TranslateTheFile.seq1.charAt(i));//把字符追加到空格上
    }
   s1 = sb.toString();
	if(s1==null)
		System.out.println("the s1 is null");
	}
	

}

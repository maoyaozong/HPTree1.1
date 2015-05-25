import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptID;
 


public class MSA_Step1_Mapper extends Mapper<Object,Text,Text,Text> {//第一个map函数是用中心序列和每个快中数据进行两两比较
	private Text firstKey=new Text();//这个变量用来保存key值
	private Text firstValue=new Text();//这个变量用来保存value值
    private String seq1 = new String();//这个变量用来保存中心序列
   
	 public void setup(Context context)//用于中心序列保存在本地文件系统，如果在map的时候要用上的画需要用上下文来进行传递
	    {
	    	Configuration conf = context.getConfiguration();
	    	seq1 = conf.get("id", "Empty");
	    	 
	    }
  
	public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
		   
	    firstKey.set("one");//设置Key值为one,这样所有的map输出就会传递到同一个reduce中去
		String value_temp=value.toString();//把value从Text变成String
		String name;
		int p1;
		for(p1=0;p1<value_temp.length();p1++){//找到：下标在哪里：之前的是序列名，：之后的是序列值
			if(value_temp.charAt(p1)!=':');
			else break;
		}		
		name=value_temp.substring(0,p1);//获取了序列的名字
		String seq2=value_temp.substring(p1+1);	 //获取了序列的值
		 
	   if(seq1==null)
		   System.out.println("TranslateTheFile.seq1 is null");	 
	   
		AlignTwoSeq ats = new AlignTwoSeq(seq1,seq2);// 把两条序列放到比对函数里面去执行
		ats.run();//执行序列比对，之后会返回两条序列，一条是new_star, 一条是new_seq
	 
		firstValue.set(name+":"+Make.alignt+"\t"+Make.aligns);//设置跟新后的中心序列为value的值，序列名+中心序列+比对序列
		 
		context.write(firstKey,firstValue);//设置key和value的值
	  }
}

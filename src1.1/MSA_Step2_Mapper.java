import java.io.IOException;

 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
 


public class MSA_Step2_Mapper extends Mapper<Object,Text,Text,Text> {//这里是执行二次map的，不同的是这里在key，value的时候不用保存中心序列
 
private Text secondKey=new Text();//保存输出的key ，序列名
private Text secondValue=new Text(); //保存输出的value ， 最终的序列
private String seq1 = new String();//new_star中心序列

public void setup(Context context)// 这个是传入的中心序列
   {
   	Configuration conf = context.getConfiguration();
   	seq1 = conf.get("id2", "Empty"); 
   }

public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
	String seqi=value.toString();//获取输入的数据value值，序列名+序列值
	String name;
	int pos;
	//System.out.println("This is Ok !!");
	for(pos=0;pos<seqi.length();pos++){//找到：的下标
		if(seqi.charAt(pos)!=':');
		else break;
	}
	name=seqi.substring(0,pos);//序列名
	String seq2=seqi.substring(pos+1);//序列值
	
	 if(seq1==null)
		   System.out.println("TranslateTheFile.seq1 is null");	 
	 
	AlignTwoSeqII at = new AlignTwoSeqII(seq1,seq2);// 比较两条序列
	if(seq1.length()==0 || seq2.length()==0)
	{
		System.out.println("the sequence is null");
		System.exit(0);
	}
	String align_str=at.run(); 
	
	//System.out.println("this part is OK ");
	secondValue.set(align_str);//设置value值为比对后的序列
	secondKey.set(name);//设置key值为序列名
	context.write(secondKey,secondValue);
	 
/*	AlignTwoSeq at = new AlignTwoSeq(seq1,seq2);// align the two sequence
		at.run();
		
		System.out.println("this part is OK ");
		secondValue.set(Make.alignt);
		secondKey.set(name);
		context.write(secondKey,secondValue);*/
}

}

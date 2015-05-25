import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;


public class TranslateTheFinalFile {//这里把多序列比对的输出文件转化成标准格式
	 
	 String inputfile;
	 String outputfile;
	 String sequence_name_file;
	 String sequecne_file;
	public TranslateTheFinalFile(String inputfile,String outputfile,String sequence_name_file,String sequecne_file){//构造函数获取输入文件的路径
    	 this.inputfile=inputfile;  //第一个参数是多序列比对的文件，格式是<key value>，用户弄到outputfile就可以了但是进化树还是可以用，看看能不能不删除
    	 this.outputfile=outputfile;// 第二个参数是把多序列比对结果文件转换后的输出文件，作为进化树的输入
    	 this.sequence_name_file=sequence_name_file;//保存的序列名文件
    	 this.sequecne_file=sequecne_file;//保存的序列文件
     }
        
     public void translate() throws IOException{
 		BufferedReader br=new BufferedReader(new FileReader(inputfile));//读取比对文件
     	BufferedWriter bw_temp=new BufferedWriter(new FileWriter(outputfile));//写入进化树输入文件,多序列比对的输出文件是叫final_output
     	//BufferedWriter bw_sequence_name =new BufferedWriter(new FileWriter(sequence_name_file));//写入序列名文件
     	//BufferedWriter bw_sequence=new BufferedWriter(new FileWriter(sequecne_file));//写入序列文件
          String line_temp=null;    
        while(br.ready()){
        	line_temp=br.readLine();//读取多序列比对的结果文件
        	 String[] line=line_temp.split("\t");//因为是key value格式的，所以用tab隔开之后前面是key , 后面是value
        	 /*bw_sequence_name.write(line[0]);//把序列名写到序列名文件中
        	 bw_sequence_name.newLine();
        	 bw_sequence_name.flush();
        	 bw_sequence.write(line[1]);//把序列写到序列文件中
        	 bw_sequence.newLine();
        	 bw_sequence.flush();*/
        	bw_temp.write(line[0]);//把序列名写到最终文件中
        	bw_temp.newLine();
        	int j=0;
        	int i=0;
        	while(i<line[1].length()){//以下的代码是把一大长段的序列弄成60个字符为一行的
        		 j=i;
        		// System.out.println(j);
        		i=i+60;
        		//System.out.println(i);
        		if(i<line[1].length()){
        		String tline=line[1].substring(j,i);
        		bw_temp.write(tline);
        		bw_temp.newLine();
        		bw_temp.flush();
        		}
        		else{
        			String tline=line[1].substring(j);
                	bw_temp.write(tline);
                	bw_temp.newLine();
                	bw_temp.flush();
        		}
        	}
        	
        }
     br.close();
     bw_temp.close();	 
	 //File filed=new File(inputfile);// delete the local output file 
	 //filed.delete();
		
         
    	 
     }
}

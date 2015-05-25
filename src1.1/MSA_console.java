import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.Properties;

public class MSA_console {
	static String local_path;
	public static void main(String[] args) throws Exception{
		
		// start the parameter part 
		if(args.length!=2){
			System.out.println("please input the parameter file");
			System.out.println(args.length+" "+args[0]);
			System.exit(0);
		}  
		Properties properties = new Properties();  
		FileInputStream input = new FileInputStream(new File(args[1]));  //get the parameters from the args[1]
		properties.load(input);
		String filename="" , local_path="" , dfs_path="" ,  reduce_number="";
		//check the data filename
		if(properties.getProperty("filename")==null){
			System.out.println("error: please set the data file name in the parameter file");
			System.exit(0);
		}
		else{
			 filename = properties.getProperty("filename");
		}
		// check the local path
		if(properties.getProperty("local_path")==null){
			System.out.println("error: please set the local_path in the parameter file");
			System.exit(0);
		}
		else{
			local_path = properties.getProperty("local_path");
		}
		// check the dfs_path
		if(properties.getProperty("dfs_path")==null){
			System.out.println("error: please set the dfs_path in the parameter file");
			System.exit(0);
		}
		else{
			dfs_path = properties.getProperty("dfs_path");
		}
		
		//check the reduce number
		if(properties.getProperty("reduce_number")==null){
			System.out.println("error: please set the reduce_number for phylogenetic in the parameter file"); 
			System.exit(0);
		}
		else{
			reduce_number = properties.getProperty("reduce_number");
		}
	    // end of the parameter part 
		
		TranslateTheFile KVfile=new TranslateTheFile(local_path+"/"+filename,local_path+"/inputKV");//对数据文件进行格式修改以及获取中心序列
		KVfile.translate();	
	    int line_number = KVfile.Get_line_number();
	    System.out.println("The totle sequence number is : "+line_number);
		/*
		    第一轮 maprduce  ----- MSA_step1
		*/
	 
		/*Configuration conf_MSA_step1=new Configuration();	 	
	 	FileSystem fs_MSA_step1 =FileSystem.get(conf_MSA_step1);
	 	Path src=new Path(local_path+"/inputKV");//本地源文件
	    Path dst=new Path(dfs_path+"/MSAinput/inputKV");//传到hadoop文件系统的目录下
	    fs_MSA_step1.copyFromLocalFile(src,dst);//把数据从本地文件系统传到hadoop文件系统下
	    conf_MSA_step1.set("id", TranslateTheFile.seq1);//把中心序列传到hdfs文件系统中
	    conf_MSA_step1.set("mapred.task.timeout","0");//把map的等待时间设置为0表示不怕等太久而被kill掉  
		Job job_MSA_step1=new Job(conf_MSA_step1,"MSA_step1");
		job_MSA_step1.setJarByClass(MSA_console.class);
		job_MSA_step1.setInputFormatClass(TextInputFormat.class);	
	 
		job_MSA_step1.setMapperClass(MSA_Step1_Mapper.class);//第一个map是用来两两比对序列的，同时存储新的中心序列和序列i
		job_MSA_step1.setMapOutputKeyClass(Text.class);//设置map的输出类型为longWritable类型，是每个分块的块号
		job_MSA_step1.setMapOutputValueClass(Text.class);//设置value的输出类型为Text类型，存储的是新的中心序列 
	 	FileInputFormat.addInputPath(job_MSA_step1,new Path(dfs_path+"/MSAinput/inputKV"));//mapreduece的输入文件路径
	 	FileOutputFormat.setOutputPath(job_MSA_step1, new Path(dfs_path+"/out"));//mapreduce的输出文件路径 
	 	job_MSA_step1.setNumReduceTasks(1);//设置reduce的个数为1
	 	job_MSA_step1.waitForCompletion(true);
	 	File delete_KV=new File(local_path+"/inputKV");
	 	delete_KV.delete();
	 	System.out.println("The MSA-step1 is over , now updata the center sequence");*/
	 	
	 	/*
	 	 *   This is the updata start sequence step
	 	 */
	 	
	 	/*Configuration conf_MSA_start=new Configuration();
	 	FileSystem fs_MSA_start=FileSystem.get(conf_MSA_start);
	 	Path local_path1=new Path(local_path+"/squencestart");//中心序列保持的位置为local_path1
	 	Path fs_path=new Path(dfs_path+"/out/part-r-00000");//第一次比对结果保存的位置
	 	fs_MSA_start.copyToLocalFile(fs_path, local_path1);//把数据从Hdfs文件系统保存到本地文件系统
	 	FetchTheSequenceStart ftss =new FetchTheSequenceStart(local_path+"/squencestart");//汇总中心序列同时，分离出第一轮比对后的序列
	 	ftss.combiner(local_path);
	 	File delete_start=new File(local_path+"/squencestart");//把本地的中心序列文件删除
	 	delete_start.delete();
	 	System.out.println("The updata start sequence is over , now start MSA-step2");*/
	 	
	 	/*
		  这里开始是第二轮mapareduce------MSA_step2
		*/	 
	 	
		/*Configuration conf_MSA_step2 = new Configuration(); 	
		FileSystem fs_MSA_step2 = FileSystem.get(conf_MSA_step2);	 
		conf_MSA_step2.set("id2",FetchTheSequenceStart.s1);//把最终的中心序列传到hdfs中去	 	
		Path seqi_src_1 = new Path(local_path+"/seqi");//本地源文件,"第一轮比对后的序列"
	    Path seqi_dst_2 = new Path(dfs_path+"/seqi/seqi");//传到hadoop文件系统的目录下
	    fs_MSA_step2.copyFromLocalFile(seqi_src_1,seqi_dst_2);	//把数据从本地文件系统传到Hdfs文件系统
	    System.out.println("the FetchTheSequenceStar is "+FetchTheSequenceStart.s1);
		
	    Job job_MSA_step2=new Job(conf_MSA_step2,"MSA_step2"); 
	    job_MSA_step2.setJarByClass(MSA_console.class);	
	 	FileInputFormat.addInputPath(job_MSA_step2, new Path(dfs_path+"/seqi/seqi"));//设置第二轮的Mapreduce的输入路径
	 	FileOutputFormat.setOutputPath(job_MSA_step2,new Path(dfs_path+"/TheMSAFinaloutput"));//设置输出文件的位置 
	 	
	 	job_MSA_step2.setInputFormatClass(TextInputFormat.class);	
	 	job_MSA_step2.setMapperClass(MSA_Step2_Mapper.class);//第二个map把两两序列再次做一个比对，并把最终的比对结果输出成一个文件
	 	job_MSA_step2.setOutputKeyClass(Text.class);//设置reduce的key的输出格式
	 	job_MSA_step2.setOutputValueClass(Text.class);//设置reduce的value的输出格式  set the reduce output value type
	 	job_MSA_step2.setNumReduceTasks(1);//设置reduce的个数
	 	job_MSA_step2.waitForCompletion(true);
		File delete_seqi=new File(local_path+"/seqi");//把本地的第一轮比对的序列删除
		delete_seqi.delete();
		System.out.println("The MSA-step2 is over , now processsing the MSA-output"); */
		
		/*
		 *   translate the MSA output to the standard format 
		 */
		
		/*System.out.println("Translate the MSA output to the standard format");//以下的部分是多序列比对算出的结果进行格式转换的程序
		Configuration conf_MSA_output=new Configuration();
		FileSystem fs_MSA_output =FileSystem.get(conf_MSA_output);
		Path MSA_final_dfs_path=new Path(dfs_path+"/TheMSAFinaloutput/part-r-00000");//第二轮比对后的输出文件
		Path MSA_final_local_path=new Path(local_path+"/MSA_OutPut");//最终多序列比对的最终文件在本地的存储位置
		fs_MSA_output.copyToLocalFile(MSA_final_dfs_path,MSA_final_local_path);//把数据从hdfs扣到本地中来 
		String sequence_name_file = local_path+"/sequence_name_file";//sequence_name_file文件用来保存序列的序列名
		String sequence_file = local_path+"/sequence_file";//sequence_file 文件用来保持序列的值
		TranslateTheFinalFile final_output = new TranslateTheFinalFile(local_path+"/MSA_OutPut",local_path+"/MSA_finalOutPut",sequence_name_file,sequence_file);
		final_output.translate();	//这里转换了最终的输出格式，并且把序列名和序列值分别保存到不同的文件中
		
		 System.out.println("The MSA process is over , now process the phylogenetic tree");
		 System.out.println("-----------------------------------------------------------");*/
         // end of the MSA 	
		// System.exit(0);
 	    /*
 	     *    Start the Cluster preprocess  [ with the single pre clustering  ] 
 	     */
		 long startTime = System.currentTimeMillis();
		 Pre_Cluster_process  cluster = new Pre_Cluster_process(local_path+"/inputKV", reduce_number);
		 cluster.Get_Cluster(local_path+"/single_cluster_output",line_number);//this ouput should be send to he HDFS 
		 int cluster_number = cluster.get_cluster_number();
		 
		
		 
		 Configuration conf_clustering =new Configuration(); 
		 conf_clustering.set("id2", dfs_path+"/single_cluster_output"); //上传文件路径
		 conf_clustering.set("mapred.task.timeout","0");		
		 Job job_clustering=new Job(conf_clustering,"clustering");
		 job_clustering.setJarByClass(MSA_console.class);
		 FileInputFormat.addInputPath(job_clustering, new Path(dfs_path+"/inputKV"));//mapreduce的输入目录位置
		 FileOutputFormat.setOutputPath(job_clustering,new Path(dfs_path+"/Cluster_OutPut/"));//设置输出文件的位置
		
		 Path local_clusteroutput = new Path(local_path+"/single_cluster_output");// get the single cluster output path
		 Path dfs_clusteroutput = new Path(dfs_path+"/single_cluster_output");
		 Path local_input= new Path(local_path+"/inputKV");// get the single cluster output path
		 Path dfs_input = new Path(dfs_path+"/inputKV");
		 FileSystem fs_clusterings=FileSystem.get(conf_clustering); 
		 fs_clusterings.copyFromLocalFile(local_clusteroutput,dfs_clusteroutput); // //把参考的初始聚类上传到HDFS文件系统当中
		 fs_clusterings.copyFromLocalFile(local_input,dfs_input);
		 
		 job_clustering.setInputFormatClass(TextInputFormat.class);
		 job_clustering.setMapperClass(Phylogenetic_Cluster_Mapper.class);
		 job_clustering.setMapOutputKeyClass(Text.class);
		 job_clustering.setMapOutputValueClass(Text.class);
		 job_clustering.setOutputKeyClass(Text.class);//设置reduce的key的输出格式
		 job_clustering.setOutputValueClass(Text.class);//设置reduce的value的输出格式 
		 job_clustering.setNumReduceTasks(1);//设置reduce的个数
		 job_clustering.waitForCompletion(true);
		 
		 Path Cluster_dfs_path=new Path(dfs_path+"/Cluster_OutPut/part-r-00000");//聚类后的结果文件
		 Path Cluster_local_path=new Path(local_path+"/Cluster_OutPut");//本地路径 Cluster_OutPut为文件名
		 fs_clusterings.copyToLocalFile(true, Cluster_dfs_path,Cluster_local_path);//把数据从hdfs扣到本地中来 
		 System.out.println("hello on balance");
		 OnBalance onbalance = new OnBalance(local_path+"/Cluster_OutPut");
		 onbalance.Balance(cluster_number,local_path);
		// System.exit(0);
		 System.out.println("The Clustering process is over , now process the construct phylogenetic tree");
		 System.out.println("-----------------------------------------------------------");
		 
		 File delete_output = new File(local_path+"/inputKV");
		 delete_output.delete();
        
		 // end of the Clustering 
		 
		 /*
	 	  *    Start the construct tree preprocess 
	 	 */
		 Configuration conf_neighbourJoining =new Configuration();
		 conf_neighbourJoining.set("mapred.task.timeout","0");
		 Job job_neighbourJoining=new Job(conf_neighbourJoining,"neighbourJoining");
		 job_neighbourJoining.setJarByClass(MSA_console.class);
		 
		 Path local_neighbourJoining = new Path(local_path+"/OnBalance_OutPut");
		 Path dfs_neighbourJoining = new Path(dfs_path+"/neighbourJoining");
		 FileSystem fs_neighbourJoining=FileSystem.get(conf_neighbourJoining); 
		 fs_neighbourJoining.copyFromLocalFile(local_neighbourJoining,dfs_neighbourJoining); // //把参考的初始聚类上传到HDFS文件系统当中
		 
		 FileInputFormat.addInputPath(job_neighbourJoining, new Path(dfs_path+"/neighbourJoining"));//mapreduce的输入目录位置
		 FileOutputFormat.setOutputPath(job_neighbourJoining ,new Path(dfs_path+"/neighbourJoining_OutPut/"));//设置输出文件的位置
		 
		 job_neighbourJoining.setInputFormatClass(TextInputFormat.class);	
		 job_neighbourJoining.setMapperClass(Phylogenetic_neighbourJoining_Mapper.class);
		 job_neighbourJoining.setMapOutputKeyClass(LongWritable.class);
		 job_neighbourJoining.setMapOutputValueClass(Text.class);
		 job_neighbourJoining.setOutputKeyClass(Text.class);
		 job_neighbourJoining.setOutputValueClass(Text.class);
		 job_neighbourJoining.setNumReduceTasks(Integer.parseInt(reduce_number));
		 job_neighbourJoining.setReducerClass(Phylogenetic_neighbourJoining_Reducer.class);
		 job_neighbourJoining.waitForCompletion(true);
		 System.out.println("The SubTree process is over , now process the construct Summary tree");
		 System.out.println("-----------------------------------------------------------");
        
		 /*
		  *   Start the Summary  process
		  */
		//从hdfs上下载文件，到本地。
		// * 如果hdfsFilePath是一个文件夹，则目标路径localFilePath也会看成一个文件夹，然后把hdfsFilePath下的所有文件，移动到
		// * localFilePath文件夹下。如果hdfsFilePath是一个文件，则目标路径localFilePath也会被当成一个文件路径，就算在路径后面加
		// * 上了显式的“/”
		 Path subTree_dfs_path = new Path(dfs_path+"/neighbourJoining_OutPut");
		 Path subTree_local_path = new Path(local_path+"/NeighbourJoining_subTree_OutPut");
		 fs_neighbourJoining.copyToLocalFile(true, subTree_dfs_path,subTree_local_path);
		 
		 NeighbourJoining_Summary nj_summary = new NeighbourJoining_Summary(
				 local_path+"/NeighbourJoining_subTree_OutPut",local_path+"/HPTree_OutPut");
		 nj_summary.Merge();//合并文件夹下的所有reduce输出结果
		 nj_summary.Summary();//子树汇总
		 long endTime = System.currentTimeMillis();
		 System.out.println("construct tree process is :"+(endTime-startTime)/1000+"s");
		 System.out.println("HPTree is over!");

		 }
}

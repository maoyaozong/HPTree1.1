
public class AlignTwoSeq {
        int g=10,h=2,M=1,misM=-1,rate = 2;   
	    String s,t;
	    static int maxk;
	    boolean flag = true;
	    public AlignTwoSeq(String s, String t) {//两条序列作为参数输入	    	
	        if(s.length()<t.length()){//s中存放的是短的序列
	            this.s = s;
	            this.t = t;
	        }
	        else{
	            this.s = t;
	            this.t = s;
	            flag = false;
	        }
	   //System.out.println("the s is :"+this.s.length());
	   //System.out.println("the t is :"+this.t.length());
	    }
	    public void run(){
	    	 int mn = Math.abs(s.length()-t.length());//计算s 和 t的长度差，s比t短，所以用abs保证mn为正的
	         int k = rate*mn+1;//k=2*(s,t之间的差值，用来计算k-band)
	         int pok,m = s.length(),n=t.length();//M为匹配积分，pok用于控制终止	   
	         
	         do{	       
	      //System.out.println("the error place 1");
	             AffineGapPenalty aff = new AffineGapPenalty(m, n, g, h, k, mn, s, t,M,misM);
	              maxk = aff.Init(); //必须先行调用此方法初始化数组
	    //System.out.println(maxk);
	    //a[m,n]>=M*(n-k-1)-2*(k+1)*(h+g)控制终止
	    //最坏情况估计至少k+1对空格，且不相邻	            
	             pok=M*(n-k-1)-2*(k+1)*(h+g);//最坏的情况
	   //System.out.println("the error place 2");
	             if(maxk<pok)//得分越高越好，如果得分比最差还差，那就把k的长度加大两倍
	                 {k=k*2;
	    //System.out.println(maxk);
	   // System.out.println(k);
	   //System.out.println(pok);
	                 }
	             else break;	   
	//System.out.println("the error place 3");
	         }while(k<=pok);
	         
	         int ch=1;//1.2.3对应a.b.c
	         if(maxk==AffineGapPenalty.a[m][n+k-m]){ch=1;}
	         else if(maxk==AffineGapPenalty.b[m][n+k-m]){ch=2;}
	         else {ch=3;}//(k==AffineGapPenalty.c[m][n+k-m])
	//System.out.println("this part is ok 3");
	         Make make=new Make(g,h,m,n,k,mn,s,t,flag);
	         make.f(ch);
	//System.out.println("this part is ok 4");
	    }
}

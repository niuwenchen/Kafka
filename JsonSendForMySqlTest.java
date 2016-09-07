package com.jackniu.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;  
import java.util.Map;
import java.util.Properties;  
  



import java.util.Random;
import java.util.UUID;

import net.sf.json.JSONObject;
import kafka.javaapi.producer.Producer;  
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;  

public class JsonSendForMySqlTest {
	 private Producer<String,String> inner;  
	 
	 private String productName;
	 
	 
	    public String getProductName() {
		return productName;
	}


	public void setProductName(String productName) {
		this.productName = productName;
	}

	 
	 
	private Integer PAGE_NUM = 100;
	private Integer MAX_MSG_NUM = 3;
	private Integer MAX_CLICK_TIME = 5;
	private Integer MAX_STAY_TIME = 10;
	private ArrayList<Integer> LIKE_OR_NOT = new ArrayList<Integer>(Arrays.asList(1, 0, -1));
		
	    public JsonSendForMySqlTest() throws Exception{  
	        Properties properties = new Properties(); 
	        //properties.load(ClassLoader.getSystemResourceAsStream("producer.properties"));  
	        //new FileInputStream(configFile)
//	        properties.load(new FileInputStream("D:\\d\\eclipseWorkSpace201502\\kafkaClient\\producer.properties"));
	        properties.load(new FileInputStream("D:\\software\\eclipsemars\\svn1\\kafkaClient\\mytest.properties"));
	        ProducerConfig config = new ProducerConfig(properties);  
	        
	        inner = new Producer<String, String>(config);  
	    }  
	  
	      
	    public void send(String topicName,String message) {  
	        if(topicName == null || message == null){  
	            return;  
	        }  
	        KeyedMessage<String, String> km = new KeyedMessage<String, String>(topicName,message);//������ж��partitions,��ʹ��new KeyedMessage(String topicName,K key,V value).  
	        System.out.println("执行send方法");
	        inner.send(km);  
	        System.out.println("send方法执行完成");
	    }  
	      
	    public void send(String topicName,Collection<String> messages) {  
	        if(topicName == null || messages == null){  
	            return;  
	        }  
	        if(messages.isEmpty()){  
	            return;  
	        }  
	        List<KeyedMessage<String, String>> kms = new ArrayList<KeyedMessage<String, String>>();  
	        for(String entry : messages){  
	        	KeyedMessage<String, String> km = new KeyedMessage<String, String>(topicName,entry);  
	            kms.add(km);  
	            //inner.send(km);
	        }  
	        inner.send(kms);  
	    }  
	      
	    public void close(){  
	        inner.close();  
	    }  
	    
	    public String monitorData()
	    {
			Random rand = new Random();
		    //Integer msgNum = rand.nextInt(MAX_MSG_NUM) + 1;
	        StringBuffer msg = new StringBuffer();
	        msg.append("page" + (rand.nextInt(PAGE_NUM) + 1));
	        msg.append("|");
	        msg.append(rand.nextInt(MAX_CLICK_TIME) + 1);
	        msg.append("|");
	        msg.append(rand.nextInt(MAX_CLICK_TIME) + rand.nextFloat());
	        msg.append("|");
	        msg.append(LIKE_OR_NOT.get(rand.nextInt(3)));
			

			return msg.toString();
		}

		private String creatObjectJson()
		{
			
			Random random = new Random();
			//日志类别
			String[] logTypeArray={"ERROR:","WARNING:","NOTICE:","INFO:","DEBUG:"};
			int logTypeIndex=random.nextInt(logTypeArray.length);
			
			//日期时间
			Date date =new Date();
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String dateStr=dateFormat.format(date);
            
            //产品线名称
            //String[] productArray={"bigdata","cloudplatform","appplatform"};
            String[] productArray={"bigdata"};
            int productIndex=random.nextInt(productArray.length);
            
            productName=productArray[productIndex];
            
           //系统名称
            String[] systemForBigDataArryay={"openplatform","tvrating","approsinfo"};
            int systemForBigDataIndex=random.nextInt(systemForBigDataArryay.length);
            String[] systemForCloudArryay={"aaa","bbb","ccc"};
            int systemForCloudIndex=random.nextInt(systemForCloudArryay.length);
            String[] systemForAppArryay={"ddd","eee","fff"};
            int systemForAppIndex=random.nextInt(systemForAppArryay.length);
            
            //文件
            String[] openplatDbArray={"bigdata_openplatform_mysqlDb1","bigdata_openplatform_mysqlDb2","bigdata_openplatform_mysqlDb3"};
            int openplatDbIndex=random.nextInt(openplatDbArray.length);
            String[] tvRatingDbArray={"bigdata_tvrating_mysqlDb1","bigdata_tvrating_mysqlDb2","bigdata_tvrating_mysqlDb3"};
            int tvRatingDbIndex=random.nextInt(tvRatingDbArray.length);
            String[] approsinfoDbArray={"bigdata_approsinfo_mysqlDb1","bigdata_approsinfo_mysqlDb2","bigdata_approsinfo_mysqlDb3"};
            int approsinfoDbIndex=random.nextInt(approsinfoDbArray.length);
            
            String[] aaaDbArray={"cloudplatform_aaa_mysqlDb1","cloudplatform_aaa_mysqlDb2","cloudplatform_aaa_mysqlDb3"};
            int aaaDbIndex=random.nextInt(aaaDbArray.length);
            String[] bbbDbArray={"cloudplatform_bbb_mysqlDb1","cloudplatform_bbb_mysqlDb2","cloudplatform_bbb_mysqlDb3"};
            int bbbDbIndex=random.nextInt(bbbDbArray.length);
            String[] cccDbArray={"cloudplatform_ccc_mysqlDb1","cloudplatform_ccc_mysqlDb2","cloudplatform_ccc_mysqlDb3"};
            int cccDbIndex=random.nextInt(cccDbArray.length);
            
            String[] dddDbArray={"appplatform_ddd_mysqlDb1","appplatform_ddd_mysqlDb2","appplatform_ddd_mysqlDb3"};
            int dddDbIndex=random.nextInt(dddDbArray.length);
            String[] eeeDbArray={"appplatform_eee_mysqlDb1","appplatform_eee_mysqlDb2","appplatform_eee_mysqlDb3"};
            int eeeDbIndex=random.nextInt(eeeDbArray.length);
            String[] fffDbArray={"appplatform_fff_mysqlDb1","appplatform_fff_mysqlDb2","appplatform_fff_mysqlDb3"};
            int fffDbIndex=random.nextInt(fffDbArray.length);
           
           // Ip
           String[] ips = { "10.9.201.190", "10.9.201.191",
					"10.9.201.192", "10.9.201.193", "10.9.201.194",
					"10.9.201.195", "10.9.201.196" };
           int ipsIndex = random.nextInt(ips.length);
           
           
           
            
			
			StringBuffer strBuffer=new StringBuffer();
			
			strBuffer.append("Product[").append(productArray[productIndex]).append("]").append(" ");
			
			if(0==productIndex){
				strBuffer.append("System[").append(systemForBigDataArryay[systemForBigDataIndex]).append("]").append(" ");
			}else if(1==productIndex){
				strBuffer.append("System[").append(systemForCloudArryay[systemForCloudIndex]).append("]").append(" ");
			}else if(2==productIndex){
				strBuffer.append("System[").append(systemForAppArryay[systemForAppIndex]).append("]").append(" ");
			}
			
			strBuffer.append("Ip[").append(ips[ipsIndex]).append("]").append(" ");
			
			if(0==productIndex){
				if(0==systemForBigDataIndex){
					strBuffer.append("DbName[").append(openplatDbArray[openplatDbIndex]).append("]").append("\n");
				}else if(1==systemForBigDataIndex){
					strBuffer.append("DbName[").append(tvRatingDbArray[tvRatingDbIndex]).append("]").append("\n");
				}else if(2==systemForBigDataIndex){
					strBuffer.append("DbName[").append(approsinfoDbArray[approsinfoDbIndex]).append("]").append("\n");
				}
			}else if(1==productIndex){
				if(0==systemForCloudIndex){
					strBuffer.append("DbName[").append(aaaDbArray[aaaDbIndex]).append("]").append("\n");
				}else if(1==systemForCloudIndex){
					strBuffer.append("DbName[").append(bbbDbArray[bbbDbIndex]).append("]").append("\n");
				}else if(2==systemForCloudIndex){
					strBuffer.append("DbName[").append(cccDbArray[cccDbIndex]).append("]").append("\n");
				}
			}else if(2==productIndex){
				if(0==systemForAppIndex){
					strBuffer.append("DbName[").append(dddDbArray[dddDbIndex]).append("]").append("\n");
				}else if(1==systemForAppIndex){
					strBuffer.append("DbName[").append(eeeDbArray[eeeDbIndex]).append("]").append("\n");
				}else if(2==systemForAppIndex){
					strBuffer.append("DbName[").append(fffDbArray[fffDbIndex]).append("]").append("\n");
				}
			}
			
			strBuffer.append("# Time: ").append(getFormatDate()).append("\n");
			strBuffer.append("# User@Host: root[root] @  [10.9.50.148]\n");
			strBuffer.append("# Query_time: ").append(getFormatDouble()).append("  ");
			strBuffer.append("Lock_time: ").append(getFormatDouble()).append(" ");
			strBuffer.append("Rows_sent: ").append(getFormatInteger()).append("  ");
			strBuffer.append("Rows_examined: ").append(getFormatInteger()).append("\n");
			strBuffer.append("SET timestamp=1430881865;\n");
			strBuffer.append("SELECT *  from  ").append(getTableName()).append(" where 1=1;");
			
//			String content="# Time: 150506 11:11:05\n"+
//				      "# User@Host: root[root] @  [10.9.50.148]\n"+
//				      "# Query_time: 2.967997  Lock_time: 0.000130 Rows_sent: 742874  Rows_examined: 742874\n"+
//				      "SET timestamp=1430881865;\n"+
//				      "SELECT *  from  brand_base_senti_t where 1=1;";
			
//			strBuffer.append(content);
					 
			
			return strBuffer.toString();
		}
		
		//获取日期
		public static String getFormatDate(){
			Date date =new Date();
	        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
	        String dateStr=dateFormat.format(date);
	        return dateStr.substring(2);
		}
		
		//查询时间、锁定时间
		public static String getFormatDouble(){
			Random random = new Random();
			Integer preFlag=random.nextInt(10);
			Long endFlag=Long.valueOf(random.nextInt(1000000));
			String retStr=preFlag+"."+endFlag;
			return retStr;
		}
		
		//查询行数、返回行数
		public static String getFormatInteger(){
			Random random = new Random();
			Long endFlag=Long.valueOf(random.nextInt(1000000));
			return endFlag.toString();
		}
		
		//获取表名
		public static String getTableName(){
			Random random = new Random();
			String[] tables={"table1","table2","table3","table4","table5","table6"};
			int tablesIndex = random.nextInt(tables.length);
			return tables[tablesIndex];
		}
		
		//读文件，返回字符串
		public static String ReadFile(String path){
		    File file = new File(path);
		    BufferedReader reader = null;
		    String laststr = "";
		    try {
		     //System.out.println("以行为单位读取文件内容，一次读一整行：");
		     InputStreamReader isr = new InputStreamReader(new FileInputStream(file), "UTF-8");
		      reader = new BufferedReader(isr);
		     String tempString = null;
		     //一次读入一行，直到读入null为文件结束
		     while ((tempString = reader.readLine()) != null) {
			      //显示行号
			      laststr = laststr+tempString;
		     }
		     reader.close();
		    } catch (IOException e) {
		     e.printStackTrace();
		    } finally {
		     if (reader != null) {
		      try {
		       reader.close();
		      } catch (IOException e1) {
		      }
		     }
		    }
		    return laststr;
		}
	    /** 
	     * @param args 
	     */  
	    public static void main(String[] args) {  
	    	JsonSendForMySqlTest producer = null;  
	    //    String fileName="D:/test.txt";
	        try{  
	            producer = new JsonSendForMySqlTest(); 
	            
	            while(true){
	            	//String json = producer.creatObjectJson();
	            	//String json="Product[bigdata] System[bigdataMongdb] Ip[172.168.234.22] 2015-10-30T11:40:05.767+0800 I QUERY    [conn112547] query tvRating.tv_prgm_cnt_by_minute_all query: { $query: { channel_code: \"cctv1\", date: \"2015-10-30\", start_time: { $lte: \"2015-10-30 11:40:24\" }, end_time: { $gte: \"2015-10-30 11:40:24\" } } } planSummary: COLLSCAN ntoreturn:0 ntoskip:0 nscanned:184 nscannedObjects:414254 keyUpdates:0 writeConflicts:0 numYields:3236 nreturned:0 reslen:20 locks:{ Global: { acquireCount: { r: 3237 } }, MMAPV1Journal: { acquireCount: { r: 3237 } }, Database: { acquireCount: { r: 3237 } }, Collection: { acquireCount: { R: 3237 } } } 700ms";
//	            	String json="Product[bigdata] System[mysql] Ip[192.168.1.30] DbName[bigdata_slowquery]\n"
//	          			  +"# Time: 151012 11:19:28\n"+
//	        		      "# User@Host: wgg[wgg] @  [172.168.234.9]  Id:  6258\n"+
//	        		      "# Query_time: 0.571976  Lock_time: 0.000225 Rows_sent: 0  Rows_examined: 123009\n"+
//	        		      "SET timestamp=1444619968;\n"+
//	        		      "select t1.channel,t1.audience_rating,t1.share_rating,t2.reach_rating,t2.loyal,c3.channel_id from (select channel, AVG(audience_rating) as audience_rating, AVG(share_rating) as share_rating from audience_area_ratio_share_tb where province = 'N' and day = '2015-10-16' and timeinterval >= '00:00:00-00:30:00' and timeinterval<= '24:00:00-24:00:00' GROUP BY channel) t1 left join (select channel, reach_rating, loyal from audience_area_reach_loyal_tb where province = 'N' and day = '2015-10-16' and timeinterval = '00:00:00-24:00:00' GROUP BY channel) t2 on t1.channel = t2.channel left join (SELECT channel_name,channel_id from channel_center) c3 on t1.channel = c3.channel_name ORDER BY t1.audience_rating DESC;";
	            	String json = "Hello World";
	            	System.out.println(json);
	                //producer.send(producer.getProductName()+"Mysql", json);
	            	producer.send("testmysql", json);
	                Thread.sleep(10000);  
	            }  
	        }catch(Exception e){  
	            e.printStackTrace();  
	        }finally{  
	            if(producer != null){  
	                producer.close();  
	            }  
	        }  
	  
	    }  
}

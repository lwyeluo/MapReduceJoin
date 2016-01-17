package com.join.main;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import com.join.bean.Table;
import com.join.impl.Broadcast_Join;
import com.join.impl.Multi_Join;
import com.join.impl.Repartition_Join;
import com.join.impl.Semi_Join;
import com.join.util.GlobalDefine;
import com.join.util.MetaDataService;

public class ParseSQL {

	private String TEST_SQL1 = "select * from a,b,c where a.id=b.id and a.name=c.name";
	private String TEST_SQL2 = "select a.id,b.name from a,b where a.id=b.id";
	private String TEST_SQL3 = "select a1.id,b.name from a1,b where a1.id=b.id";
	private String TEST_SQL4 = "select a2.id,b.name from a2,b where a2.id=b.id";
	private HashMap<String, Table> tableMap = new HashMap<String, Table>();
	
	
	private int small_file_index = GlobalDefine.FILE_SMALL_NOT_KNOW; 
	
	public ParseSQL() {
		MetaDataService metadataService = new MetaDataService();
		Table[] tableBean = metadataService.loadMetadata();
		for(Table t : tableBean) {
			tableMap.put(t.getName(), t);
			System.out.println(t.getName() + " " + t.getPath());
			Iterator it = t.getField().keySet().iterator();
			while(it.hasNext()) {
				String key = (String)it.next();
				System.out.println(key + ":" + t.getField().get(key));
			}
		}
	}
	
	public String[] getElement(String eles, String del) { 
		String[] ele; 
		int index = -1;
		if((index = eles.indexOf(del)) != -1) {
			//multi table
			ele = eles.split(del);
//			for(int i = 0; i < ele.length; i ++)
//				System.out.println(ele[i].trim());
		} else {
			ele = new String[1];
			ele[0] = eles.trim();
//			System.out.println(ele[0]);
		}
		return ele;
	}
	
	/**
	 * Is it can use broadcast join?
	 * @param args
	 * a.path, a.keyindex, b.path, b.keyindex
	 * @return
	 */
	public int checkUseBroadcast(String[] args) {
		try {
			System.out.println(GlobalDefine.DIRECTORY + args[0] + " "+GlobalDefine.DIRECTORY + args[2]);
		
			Configuration conf = new Configuration();
		    FileSystem fs = FileSystem.get(conf);
			Path path1 =new Path(GlobalDefine.DIRECTORY + args[0]);
			Path path2 =new Path(GlobalDefine.DIRECTORY + args[2]);
			FileStatus f1 =  fs.getFileStatus(path1);
			FileStatus f2=  fs.getFileStatus(path2);
		   // System.out.println("stat.getLen()"+f1.getLen());


		
		
		
			System.out.println("" + f1.getLen() + "<--->" + f2.getLen());
		
			small_file_index = f1.getLen() > f2.getLen() ? GlobalDefine.FILE_B_IS_SMALL 
					: GlobalDefine.FILE_A_IS_SMALL;
			long small_size = f1.getLen() > f2.getLen() ? f2.getLen() : f1.getLen();
			
			if((small_size >> GlobalDefine.BROADCAST_OFFSET) < GlobalDefine.BROADCAST_MAX_SIZE)
				return GlobalDefine.BROADCAST_JOIN;
			
			if((small_size >> GlobalDefine.BROADCAST_OFFSET) < GlobalDefine.SEMIJOIN_MAX_SIZE)
				return GlobalDefine.SEMI_JOIN;
		}catch(Exception e) {
			e.printStackTrace();
		}
		
		return GlobalDefine.REPATITION_JOIN;
	}
	
	public void parse(String sql) {
		System.out.println("parse: " + sql);
		int select_pos = sql.indexOf("select");
		int from_pos = sql.indexOf("from");
		int where_pos = sql.indexOf("where");
		
		String tables = sql.substring(from_pos + "from".length(), where_pos).trim();
		String projections = sql.substring(select_pos + "select".length(), from_pos).trim();
		String conditions = sql.substring(where_pos + "where".length());
		
		//parse tables
		String[] table = getElement(tables, ",");
		assert(table.length == 2 || table.length == 3);
		
		//parse project
		String[] projection = getElement(projections, ",");
		assert(projection.length > 0);
		
		//parse condition
		String[] condition = getElement(conditions, "and");
		assert(condition.length == 1 || condition.length == 2);
		
		//get join key
		String[] param;
		if(condition.length == 1)
			//a.path, a.keyindex, b.path, b.keyindex
			param = new String[4]; 
		else
			//a.path, a.keyindex, b.path, b.keyindex1, b.keyindex2 c.path, c.keyindex
			param = new String[7]; 
		int pos = 0;
		for(String con : condition) {
			int index = con.indexOf("=");
			assert(index != -1);
			String left = con.substring(0, index).trim();
			String right = con.substring(index + "=".length(), con.length()).trim();
			System.out.println(left + ":" + right);
			//get left param
			assert(left.indexOf(".") != -1);
			String tmp1 = left.substring(0, left.indexOf("."));
			String tmp2 = left.substring(left.indexOf(".") + 1, left.length());
			if(tmp1.equals(param[0])) {
				//swap param: 0-2 1-3
				for(int i = 0; i < 2; i ++) {
					String tmp3 = param[i];
					param[i] = param[i + 2];
					param[i + 2] = tmp3;
				}
				param[pos ++] = "" + tableMap.get(tmp1).getField().get(tmp2);
			} else if(tmp1.equals(param[2])) {
				param[pos ++] = "" + tableMap.get(tmp1).getField().get(tmp2);
			} else {
				param[pos ++] = tableMap.get(tmp1).getName();
				param[pos ++] = "" + tableMap.get(tmp1).getField().get(tmp2);
			}
			
			assert(right.indexOf(".") != -1);
			tmp1 = right.substring(0, right.indexOf("."));
			tmp2 = right.substring(right.indexOf(".") + 1, right.length());
			param[pos ++] = tableMap.get(tmp1).getName();
			param[pos ++] = "" + tableMap.get(tmp1).getField().get(tmp2);
		}
		
		//sightly modify
		for(int i = 0; i < 3; i ++) {
			if((i & 0x1) == 0)
				param[i] = tableMap.get(param[i]).getPath();
		}
		if(param.length == 7)
			param[5] = tableMap.get(param[5]).getPath();
		
		long startTime = System.currentTimeMillis();
		int res=0;
		//check size
		if(param.length == 4) {
			int ret = checkUseBroadcast(param);
			if(small_file_index == GlobalDefine.FILE_A_IS_SMALL) {
				System.out.println("\ta is small");
			}
			else if(small_file_index == GlobalDefine.FILE_B_IS_SMALL) {
				System.out.println("\tb is small...swap...");
				for(int i = 0; i < 2; i ++) {
					String tmp3 = param[i];
					param[i] = param[i + 2];
					param[i + 2] = tmp3;
				}
			}
			
			if(ret == GlobalDefine.REPATITION_JOIN) {
				System.out.println("\tREPATITION_JOIN");
				try {
					res = ToolRunner.run(new Configuration(), new Repartition_Join(), param);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}  
			}
			else if(ret == GlobalDefine.BROADCAST_JOIN) {
				System.out.println("\tBROADCAST_JOIN");
				try {
					res = ToolRunner.run(new Configuration(), new Broadcast_Join(), param);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
			}
			else if(ret == GlobalDefine.SEMI_JOIN) {
				System.out.println("\tSEMI_JOIN");
				try {
					res = ToolRunner.run(new Configuration(), new Semi_Join(), param);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
			}
			
		}
		else {
					System.out.println("MLUTI");
					try {
						res = ToolRunner.run(new Configuration(), new Multi_Join(), param);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} 
		}
		System.out.println("用时为"+(System.currentTimeMillis()-startTime));
		//print param
		System.out.print("INFO:param\n\t");
		for(int i = 0; i < param.length; i ++)
			System.out.print(param[i] + " ");
		System.out.println();
		
	}
	
	public void readInput() {
		BufferedReader bReader = new BufferedReader(new InputStreamReader(System.in));
		while(true) {
			System.out.print(">> ");
			String sql;
			try {
				sql = bReader.readLine();
				if(sql == "") {
					System.out.println("exit");
					break;
				} else if(sql.equals("t1")) {
					parse(TEST_SQL1);
				} else if(sql.equals("t2")) {
					parse(TEST_SQL2);
				}else if(sql.equals("t3")) {
					parse(TEST_SQL3);
				} else if(sql.equals("t4")) {
					parse(TEST_SQL4);
				}  
				else {
					parse(sql);
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ParseSQL parseSQL = new ParseSQL();
		parseSQL.readInput();
	}

}

package com.join.util;

public class GlobalDefine {
	public static int BROADCAST_JOIN = 1;
	public static int SEMI_JOIN = 2;
	public static int REPATITION_JOIN = 3;
	
	public static int FILE_A_IS_SMALL = 1;
	public static int FILE_B_IS_SMALL = 2;
	public static int FILE_SMALL_NOT_KNOW = 0;
	
	public static int BROADCAST_OFFSET = 20; //1<<20 1M
	public static int BROADCAST_MAX_SIZE = 25; //25M
	public static int SEMIJOIN_MAX_SIZE = 50; 
	public static String DIRECTORY = "src/"; //default directory
}

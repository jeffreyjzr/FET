package com.graph.pre;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

public class PreprocessWikiID {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		outputDuplicate();
	}
	
	public static void removeEmpty() throws IOException{
		String filepath = "/Users/Jeff/Documents/Nutstore/file/FET_file";
		BufferedReader bin = new BufferedReader(new InputStreamReader(
				new FileInputStream(filepath+File.separator+"wiki_ids.dat")));
		PrintWriter out = new PrintWriter(new FileWriter(filepath+File.separator+"wiki_ids_new.dat"));
		String readline ="";
		while ((readline = bin.readLine()) != null) {
			if(!readline.trim().equals("")){
				out.println(readline);
			}
			
		}
		bin.close();
		out.close();
	}
	
	public static void outputDuplicate() throws IOException{
		String filepath = "/Users/Jeff/Documents/Nutstore/file/FET_file";
		BufferedReader bin = new BufferedReader(new InputStreamReader(
				new FileInputStream(filepath+File.separator+"wiki_ids_re.dat")));
		Map<String,String> dMap = new HashMap<String,String>();
		PrintWriter out = new PrintWriter(new FileWriter(filepath+File.separator+"wiki_ids_du_1.dat"));
		String readline ="";
		while ((readline = bin.readLine()) != null) {
			String s[] = readline.split("_\\|_");
			if(dMap.get(s[0])!=null){
				//out.println(s[0]+"_|_"+dMap.get(s[0]));
				out.println(readline);
			}else{
				dMap.put(s[0],s[1]);
			}
			
		}
		bin.close();
		out.close();
	}
	
	public static void replaceDuplicate() throws IOException{
		String filepath = "/Users/Jeff/Documents/Nutstore/file/FET_file";
		BufferedReader bin = new BufferedReader(new InputStreamReader(
				new FileInputStream(filepath+File.separator+"title_dumpid_timestamp.dat")));
		Map<String,String> WikiMap = new HashMap<String,String>();
		String readline ="";
		while ((readline = bin.readLine()) != null) {
			String s[] = readline.split("\\t");
			WikiMap.put(s[0].toLowerCase(), s[1]);
		}
		bin.close();
		PrintWriter out = new PrintWriter(new FileWriter(filepath+File.separator+"wiki_ids_replace.dat"));
		bin = new BufferedReader(new InputStreamReader(
				new FileInputStream(filepath+File.separator+"wiki_ids_du.dat")));
		while ((readline = bin.readLine()) != null) {
			String s[] = readline.split("_\\|_");
			if(WikiMap.get(s[1])!=null){
				if(!isNumeric(WikiMap.get(s[1]))){
					out.println(readline);
					System.out.println(WikiMap.get(s[1])+"_|_"+s[1]);
				}else{
					out.println(WikiMap.get(s[1])+"_|_"+s[1]);
				}
			}else{
				//out.println(readline);
			}
		}
		bin.close();
		
		out.close();
	}
	
	
	public static void compare() throws IOException{
		String filepath = "/Users/Jeff/Documents/Nutstore/file/FET_file";
		BufferedReader bin = new BufferedReader(new InputStreamReader(
				new FileInputStream(filepath+File.separator+"wiki_ids.dat")));
		Map<String,String> WikiMap = new HashMap<String,String>();
		String readline ="";
		while ((readline = bin.readLine()) != null) {
			String s[] = readline.split("_\\|_");
			WikiMap.put(s[1], s[0]);
		}
		bin.close();
		PrintWriter out = new PrintWriter(new FileWriter(filepath+File.separator+"wiki_ids_c.dat"));
		bin = new BufferedReader(new InputStreamReader(
				new FileInputStream(filepath+File.separator+"wiki_ids_new.dat")));
		while ((readline = bin.readLine()) != null) {
			String s[] = readline.split("_\\|_");
			if(WikiMap.get(s[1])!=null){
				if(s[0].equals(WikiMap.get(s[1]))){
					//do nothing
				}else{
					out.println(readline);
				}
			}else{
				System.out.println(s[1]);
			}
		}
		bin.close();
		
		out.close();
	}
	
	public static boolean isNumeric(String str){
		  for (int i = str.length();--i>=0;){   
		   if (!Character.isDigit(str.charAt(i))){
		    return false;
		   }
		  }
		  return true;
		 }
	
	public static void rewriteWIKIID() throws IOException{
		String filepath = "/Users/Jeff/Documents/Nutstore/file/FET_file";
		BufferedReader bin = new BufferedReader(new InputStreamReader(
				new FileInputStream(filepath+File.separator+"wiki_ids_replace.dat")));
		Map<String,String> WikiMap = new HashMap<String,String>();
		String readline ="";
		while ((readline = bin.readLine()) != null) {
			String s[] = readline.split("_\\|_");
			WikiMap.put(s[1].trim(), s[0].trim());
		}
		bin.close();
		PrintWriter out = new PrintWriter(new FileWriter(filepath+File.separator+"wiki_ids_new.dat"));
		bin = new BufferedReader(new InputStreamReader(
				new FileInputStream(filepath+File.separator+"wiki_ids.dat")));
		while ((readline = bin.readLine()) != null) {
			String s[] = readline.split("_\\|_");
			if(WikiMap.get(s[1])!=null){
				out.println(WikiMap.get(s[1])+"_|_"+s[1]);
			}else{
				out.println(readline);
				//System.out.println(s[1]);
			}
		}
		bin.close();
		
		out.close();
	}
	
	public static void removeDuplicate() throws IOException{
		String filepath = "/Users/Jeff/Documents/Nutstore/file/FET_file";
		BufferedReader bin = new BufferedReader(new InputStreamReader(
				new FileInputStream(filepath+File.separator+"wiki_ids_new.dat")));
		Map<String,String> dMap = new HashMap<String,String>();
		PrintWriter out = new PrintWriter(new FileWriter(filepath+File.separator+"wiki_ids_re.dat"));
		String readline ="";
		while ((readline = bin.readLine()) != null) {
			String s[] = readline.split("_\\|_");
			if(dMap.get(s[0])!=null){
				//do nothing
			}else{
				dMap.put(s[0],readline);
				out.println(readline);
			}
			
		}
		bin.close();
		out.close();
	}

}

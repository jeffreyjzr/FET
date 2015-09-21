package com.graph.pre;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

/**
 * generate formula id file
 * @author Jeff
 *
 */
public class GenerateFormulaID {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		
		String outputPath = "/Users/Jeff/Documents";
		PrintWriter out = new PrintWriter(new FileWriter(outputPath+File.separator+"formula_id.dat"));
		PrintWriter out_1 = new PrintWriter(new FileWriter(outputPath+File.separator+"formulaid2pageid.dat"));
		BufferedReader bin = new BufferedReader(new InputStreamReader(
				new FileInputStream(outputPath+File.separator+"formula2pageid.dat")));
		String readline ="";
		int i =1;
		boolean ifFirst = true;
		while ((readline = bin.readLine()) != null) {
			String s[] = readline.split("\\t");
			if(ifFirst){
				//first is null
				ifFirst = false;
			}else{
				out.println(i+"_|_"+s[0]);
				out_1.println(i+","+s[1]);
				i++;
			}
			
		}
		bin.close();
		out.close();
		out_1.close();

	}

}

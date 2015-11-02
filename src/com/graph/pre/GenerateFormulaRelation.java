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

public class GenerateFormulaRelation {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		String outputPath = "/Users/Jeff/Documents/Nutstore/file/FET_file";
		Map<String,Map<String,Boolean>> wikiMap = new HashMap<String,Map<String,Boolean>>();
		PrintWriter out = new PrintWriter(new FileWriter(outputPath+File.separator+"fid2wikiid.dat"));
		BufferedReader bin = new BufferedReader(new InputStreamReader(
				new FileInputStream(outputPath+File.separator+"formulaid2pageid.dat")));
		String readline ="";
		while ((readline = bin.readLine()) != null) {
			String s[] = readline.split(",");
			String ss[] = s[1].split("\\s+");
			int num = ss.length;
			for(int i =0;i<num;i++){
				float v = 1.0f/(float)num;
				out.println(s[0]+","+ss[i]+","+v);
				if(wikiMap.get(ss[i])!=null){
					Map<String,Boolean> wMap = wikiMap.get(ss[i]);
					wMap.put(s[0], true);
					wikiMap.put(ss[i], wMap);
				}else{
					Map<String,Boolean> wMap = new HashMap<String,Boolean>();
					wMap.put(s[0], true);
					wikiMap.put(ss[i], wMap);
				}
			}
			
		}
		bin.close();
		out.close();
		
		
		Map<String,Map<String,Integer>> formulaMap = new HashMap<String,Map<String,Integer>>();
		for(String wikiID:wikiMap.keySet()){
			Map<String,Boolean> fMap = wikiMap.get(wikiID);
			if(fMap.keySet().size()>1){
				for(String f1id:fMap.keySet()){
					if(formulaMap.get(f1id)!=null){
						Map<String,Integer> f1Map = formulaMap.get(f1id);
						for(String f2id:fMap.keySet()){
							if(!f1id.equals(f2id)){
								if(f1Map.get(f2id)!=null){
									int n = f1Map.get(f2id);
									f1Map.put(f2id,n+1);
								}else{
									f1Map.put(f2id,1);
								}
							}
						}
						formulaMap.put(f1id,f1Map);
					}else{
						Map<String,Integer> f1Map = new HashMap<String,Integer>();
						for(String f2id:fMap.keySet()){
							if(!f1id.equals(f2id)){
								if(f1Map.get(f2id)!=null){
									int n = f1Map.get(f2id);
									f1Map.put(f2id,n+1);
								}else{
									f1Map.put(f2id,1);
								}
							}
						}
						formulaMap.put(f1id,f1Map);
					}
				}
			}
		}
		
		
		
		Map<String,Map<String,Float>> formula_value_Map = new HashMap<String,Map<String,Float>>();
		for(String f1id:formulaMap.keySet()){
			Map<String,Integer> fMap =  formulaMap.get(f1id);
			int total =0;
			for(String f2id:fMap.keySet()){
				int n = fMap.get(f2id);
				total = total + n;
			}
			Map<String,Float> f_value_Map = new HashMap<String,Float>();
			for(String f2id:fMap.keySet()){
				int n = fMap.get(f2id);
				f_value_Map.put(f2id, (float)n/(float)total);
			}
			formula_value_Map.put(f1id, f_value_Map);
		}
		out = new PrintWriter(new FileWriter(outputPath+File.separator+"fcof.dat"));
		for(String f1id:formula_value_Map.keySet()){
			Map<String,Float> fMap =  formula_value_Map.get(f1id);
			
			for(String f2id:fMap.keySet()){
				out.println(f1id+","+f2id+","+fMap.get(f2id));
			}
		}
		
		out.close();

	}

}

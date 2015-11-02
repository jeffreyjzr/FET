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

import edu.jhu.nlp.wikipedia.*;

public class GenerateWikiLink {

	
	
	final static Map<String, String> wikiMap = new HashMap<String, String>();
	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		long sTime, eTime;
		sTime = System.currentTimeMillis();
		System.out.println("Start... ");
		try {
			if (args.length == 2) {
				// TODO Auto-generated method stub
				String inPath = args[0];
				String outPath = args[1];
				//generateWikiLink(inPath, outPath);
				processWikiLink(outPath);
			}
		} catch (Exception e) {
			e.printStackTrace();

		}

		System.out.println("End");
		eTime = System.currentTimeMillis();

		System.out.println("Total TIME (sec): " + (eTime - sTime) / 1000.0);

	}

	public static void generateWikiLink(String path, String outputPath)
			throws IOException {

		
		System.out.println("reading wiki_id...");
		BufferedReader bin = new BufferedReader(new InputStreamReader(
				new FileInputStream(outputPath + File.separator
						+ "wiki_ids.dat")));
		String readline = "";
		while ((readline = bin.readLine()) != null) {
			String s[] = readline.split("_\\|_");
			if (s.length > 1) {
				wikiMap.put(s[1], s[0]);
			}
		}
		bin.close();
		System.out.println("end wiki_id..."+wikiMap.size());

		System.out.println("start parse...");
		WikiXMLParser wxsp = WikiXMLParserFactory.getSAXParser(path);
		final PrintWriter out = new PrintWriter(new FileWriter(outputPath
				+ File.separator + "wiki_links.dat"));
		final PrintWriter out_c = new PrintWriter(new FileWriter(outputPath
				+ File.separator + "wiki_categories.dat"));
        try {     
            wxsp.setPageCallback(new PageCallbackHandler() { 
            	public void process(WikiPage page) {
            		if(!page.isRedirect()&&!page.isSpecialPage()&&!page.isDisambiguationPage()
                			&&!page.isStub()){
            			if (page.getLinks() != null
								&& !page.getLinks().isEmpty()) {
							out.print(page.getID());
							for (String id : page.getLinks()) {
								if (wikiMap.get(id.toLowerCase())!=null) {
									out.print(","+wikiMap.get(id.toLowerCase()));
								}
							}
							out.println();
						}
						if (page.getCategories() != null
								&& !page.getCategories().isEmpty()) {
							out_c.print(page.getID());
							for (String id : page.getCategories()) {
								if (wikiMap.get(id.toLowerCase()) != null) {
									out_c.print(","
											+ wikiMap.get(id.toLowerCase()));
								}
							}
							out_c.println();
						}
            		}
            		//System.out.println(page.getTitle());
                }
            });
                
           wxsp.parse();
           out.close();
           out_c.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void processWikiLink(String outputPath)
			throws IOException {

		
		System.out.println("processing wiki_categories...");
		BufferedReader bin = new BufferedReader(new InputStreamReader(
				new FileInputStream(outputPath + File.separator
						+ "wiki_categories.dat")));
		Map<String,Map<String,Boolean>> cateMap = new HashMap<String,Map<String,Boolean>>();
		
		String readline = "";
		while ((readline = bin.readLine()) != null) {
			String s[] = readline.split(",");
			if (s.length > 1) {
				Map<String,Boolean> cMap = new HashMap<String,Boolean>();
				for(int i=1;i<s.length;i++){
					if(!s[0].equals(s[i])){
						cMap.put(s[i], true);
					}
				}
				if(cMap.keySet().size()>0){
					cateMap.put(s[0], cMap);
				}
				
			}
		}
		bin.close();
		
		PrintWriter out = new PrintWriter(new FileWriter(outputPath
				+ File.separator + "wiki_categories_new.dat"));
		for(String sid:cateMap.keySet()){
			Map<String,Boolean> cMap = cateMap.get(sid);
			int num = cMap.keySet().size();
			float value = 1.0f/(float)num;
			for(String fid:cMap.keySet()){
				out.println(sid+","+fid+","+value);
			}
		}
		
		out.close();
		System.out.println("processing wiki_links...");
		
		Map<String,Map<String,Boolean>> linkMap = new HashMap<String,Map<String,Boolean>>();
		bin = new BufferedReader(new InputStreamReader(
				new FileInputStream(outputPath + File.separator
						+ "wiki_links.dat")));
		
		while ((readline = bin.readLine()) != null) {
			String s[] = readline.split(",");
			if (s.length > 1) {
				Map<String,Boolean> lMap = new HashMap<String,Boolean>();
				for(int i=1;i<s.length;i++){
					if(!s[0].equals(s[i])){
						lMap.put(s[i], true);
					}
				}
				if(lMap.keySet().size()>0){
					linkMap.put(s[0], lMap);
				}
				
			}
		}
		bin.close();
		out = new PrintWriter(new FileWriter(outputPath
				+ File.separator + "wiki_links_new.dat"));
		for(String sid:linkMap.keySet()){
			Map<String,Boolean> lMap = linkMap.get(sid);
			int num = lMap.keySet().size();
			float value = 1.0f/(float)num;
			for(String eid:lMap.keySet()){
				out.println(sid+","+eid+","+value);
			}
		}
		
		out.close();
	}

}

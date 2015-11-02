package com.graph.pre;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import edu.jhu.nlp.wikipedia.PageCallbackHandler;
import edu.jhu.nlp.wikipedia.WikiPage;
import edu.jhu.nlp.wikipedia.WikiXMLParser;
import edu.jhu.nlp.wikipedia.WikiXMLParserFactory;

public class GenerateWikiIDS {

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
				generateWikiId(args[0],args[1]);
			}
		} catch (Exception e) {
			e.printStackTrace();
			
		}

		System.out.println("End");
		eTime = System.currentTimeMillis();

		System.out.println("Total TIME (sec): " + (eTime - sTime) / 1000.0);

		
	}
	
	public static void generateWikiId(String path, String outputPath) throws IOException{
		System.out.println("start parse...");
		WikiXMLParser wxsp = WikiXMLParserFactory.getSAXParser(path);
		final PrintWriter out = new PrintWriter(new FileWriter(outputPath+File.separator+"wiki_ids.dat"));
        try {     
            wxsp.setPageCallback(new PageCallbackHandler() { 
            	public void process(WikiPage page) {
            		if(!page.isRedirect()&&!page.isSpecialPage()&&!page.isDisambiguationPage()
                			&&!page.isStub()){
            			out.println(page.getID()+"_|_"+page.getTitle().toLowerCase());
            		}
            		//System.out.println(page.getTitle());
                }
            });
                
           wxsp.parse();
           out.close();
        }catch(Exception e) {
                e.printStackTrace();
        }
        
	}

}

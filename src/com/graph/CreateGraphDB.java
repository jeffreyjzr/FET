package com.graph;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.io.fs.FileUtils;

/**
 * Main function for creating database
 * 
 * @author Jeff
 * 
 */
public class CreateGraphDB {

	// graph database
	private static GraphDatabaseService graphDb;

	// index for relationships(traditional index)
	// private static Index<Relationship> relationIndex;

	// index for label and node properity
	// private static IndexDefinition indexDefinition;

	private static String db_path;

	private static String filePath;

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		System.out.println("Start... ");
		long sTime, eTime;
		sTime = System.currentTimeMillis();
		if (args.length != 0) {
			db_path = args[0];
			filePath = args[1];
			createDb(db_path);
			shutDown();

		}

		System.out.println("End");
		eTime = System.currentTimeMillis();
		System.out.println("Total TIME (sec): " + (eTime - sTime) / 1000.0);
	}

	/**
	 * function for create database
	 * @throws IOException 
	 */
	public static void createDb(String db_path) throws IOException {
		
		FileUtils.deleteRecursively( new File( db_path ) );
		// START SNIPPET: startDb
		graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(db_path);
		registerShutdownHook(graphDb);
		// END SNIPPET: startDb

		// create wiki index
		IndexDefinition wiki_indexDefinition;
		try (Transaction tx = graphDb.beginTx()) {
			Schema schema = graphDb.schema();
			// wiki
			wiki_indexDefinition = schema.indexFor(DynamicLabel.label("wiki"))
					.on("id").create();

			tx.success();
		}

		// START SNIPPET: transaction
		try (Transaction tx = graphDb.beginTx()) {
			// Database operations go here
			// END SNIPPET: transaction
			// START SNIPPET: addData

			// Create some nodes and index their ids with the IndexService
			System.out.println("adding wiki pages...");
			addWikiNodesWithIndexing();

			tx.success();
		}

		try (Transaction tx = graphDb.beginTx()) {
			// Add resource relationship
			System.out.println("adding wiki category relations...");
			addWikiCategoryRelations();
			tx.success();
		}
		
		try (Transaction tx = graphDb.beginTx()) {
			// Add resource relationship
			System.out.println("adding wiki link relations...");
			addWikiLinkRelations();
			tx.success();
		}

		// create formula index
		IndexDefinition formula_indexDefinition;
		try (Transaction tx = graphDb.beginTx()) {
			Schema schema = graphDb.schema();
			// keyword
			formula_indexDefinition = schema
					.indexFor(DynamicLabel.label("formula")).on("id").create();
			//schema.awaitIndexOnline(keyword_indexDefinition, 10,TimeUnit.SECONDS);

			tx.success();
		}

		try (Transaction tx = graphDb.beginTx()) {
			// Add keywords
			System.out.println("adding formula...");
			addFormulaNodesWithIndexing();

			tx.success();
		}

		try (Transaction tx = graphDb.beginTx()) {
			// Add relationship
			System.out.println("adding formula wiki relations...");
			addFormulaWikiRelations();

			tx.success();
		}
		
		try (Transaction tx = graphDb.beginTx()) {
			// Add relationship
			System.out.println("adding formula formula co_occurrence relations...");
			addFormulaCoFormulaRelations();

			tx.success();
		}

		// END SNIPPET: transaction
	}

	/**
	 * function for shutting down
	 */
	public static void shutDown() {
		System.out.println();
		System.out.println("Shutting down database ...");
		// START SNIPPET: shutdownServer
		graphDb.shutdown();
		// END SNIPPET: shutdownServer
	}

	/**
	 * register shut down hook
	 * 
	 * @param graphDb
	 */
	// START SNIPPET: shutdownHook
	private static void registerShutdownHook(final GraphDatabaseService graphDb) {
		// Registers a shutdown hook for the Neo4j instance so that it
		// shuts down nicely when the VM exits (even if you "Ctrl-C" the
		// running application).
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				graphDb.shutdown();
			}
		});
	}

	// END SNIPPET: shutdownHook

	/**
	 * add Wikipedia page nodes with indexing
	 */
	public static void addWikiNodesWithIndexing() {
		String readline = "";
		try {
			BufferedReader bin = new BufferedReader(new InputStreamReader(
					new FileInputStream(filePath + File.separator
							+ "wikiid_timestamp.dat")));
			Map<String,String> wikiMap =  new HashMap<String,String>();
			while ((readline = bin.readLine()) != null) {
				String s[] = readline.split(",");
				String id = s[0];
				String time = s[1];
				wikiMap.put(id, time);
			}
			bin.close();
			bin = new BufferedReader(new InputStreamReader(
					new FileInputStream(filePath + File.separator
							+ "wiki_ids.dat")));
			int i = 0;
			while ((readline = bin.readLine()) != null) {
				String s[] = readline.split("_\\|_");
				String id = s[0];
				String title = s[1];
				if(wikiMap.get(id)!=null){
					Label label = DynamicLabel.label("wiki");
					Node node = graphDb.createNode(label);
					node.setProperty("id", "W_"+id);
					node.setProperty("title", title);
					node.setProperty("time", wikiMap.get(id));
					i++;
					if (i % 100000 == 0) {
						System.out.println(i);
					}
				}else{
					System.out.println(id);
				}
				
			}
			bin.close();
		} catch (Exception e) {
			System.out.println(readline);
			e.printStackTrace();
		}
	}

	/**
	 * add Wikipedia category relation
	 */
	public static void addWikiCategoryRelations() {
		String readline = "";
		try {
			BufferedReader bin = new BufferedReader(new InputStreamReader(
					new FileInputStream(filePath + File.separator
							+ "wiki_categories.dat")));
			int i = 0;
			while ((readline = bin.readLine()) != null) {
				String s[] = readline.split(",");
				String fromID = "W_" + s[0];
				String toID = "W_" + s[1];
				Label label = DynamicLabel.label("wiki");
				Node foundFrom = graphDb.findNode(label, "id", fromID);
				Node foundTo = graphDb.findNode(label, "id", toID);
				if (foundFrom != null && foundTo != null) {
					Relationship relationship = foundFrom.createRelationshipTo(
							foundTo, RelTypes.Category);
					relationship
							.setProperty("weight", Double.parseDouble(s[2]));
					i++;
				} else {
					if(foundFrom!=null){
						System.out.println("fromID:" + fromID);
					}
					if(toID!=null){
						System.out.println("toID" + toID);
					}
					System.out.println("fromID:" + fromID + " toID" + toID);
				}

				if (i % 100000 == 0) {
					System.out.println(i);
				}
			}
			bin.close();
		} catch (Exception e) {
			System.out.println(readline);
			e.printStackTrace();
		}

	}
	
	/**
	 * add Wikipedia link relation
	 */
	public static void addWikiLinkRelations() {
		String readline = "";
		try {
			BufferedReader bin = new BufferedReader(new InputStreamReader(
					new FileInputStream(filePath + File.separator
							+ "wiki_links.dat")));

			int i = 0;
			while ((readline = bin.readLine()) != null) {
				String s[] = readline.split(",");
				String fromID = "W_" + s[0];
				String toID = "W_" + s[1];
				Label label = DynamicLabel.label("wiki");
				Node foundFrom = graphDb.findNode(label, "id", fromID);
				Node foundTo = graphDb.findNode(label, "id", toID);
				if (foundFrom != null && foundTo != null) {
					Relationship relationship = foundFrom.createRelationshipTo(
							foundTo, RelTypes.Link);
					relationship
							.setProperty("weight", Double.parseDouble(s[2]));
					i++;
				} else {
					System.out.println("fromID:" + fromID + " toID" + toID);
				}

				if (i % 100000 == 0) {
					System.out.println(i);
				}
			}
			bin.close();
		} catch (Exception e) {
			System.out.println(readline);
			e.printStackTrace();
		}

	}

	/**
	 * add formula nodes with indexing
	 */
	public static void addFormulaNodesWithIndexing() {
		try {
			BufferedReader bin = new BufferedReader(new InputStreamReader(
					new FileInputStream(filePath + File.separator
							+ "formula_id.dat")));
			String readline = "";
			int i = 0;
			while ((readline = bin.readLine()) != null) {
				String s[] = readline.split("_\\|_");
				String id = "F_"+s[0];
				String title = s[1];
				Label label = DynamicLabel.label("formula");
				Node node = graphDb.createNode(label);
				node.setProperty("id", id);
				node.setProperty("content", title);
				i++;
				if (i % 100000 == 0) {
					System.out.println(i);
				}
			}
			bin.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * add Formula Wiki Relations
	 */
	public static void addFormulaWikiRelations() {
		try {
			BufferedReader bin = new BufferedReader(new InputStreamReader(
					new FileInputStream(filePath + File.separator
							+ "fid2wikiid.dat")));
			String readline = "";
			int i = 0;
			while ((readline = bin.readLine()) != null) {
				String s[] = readline.split(",");
				String fromID = "F_" + s[0];
				String toID = "W_"+s[1];
				Label wiki_label = DynamicLabel.label("wiki");
				Label f_label = DynamicLabel.label("formula");
				Node foundFrom = graphDb.findNode(f_label, "id", fromID);
				Node foundTo = graphDb.findNode(wiki_label, "id", toID);
				Relationship relationship = foundFrom.createRelationshipTo(
						foundTo, RelTypes.In);
				relationship.setProperty("weight", Double.parseDouble(s[2]));
				i++;
				if (i % 100000 == 0) {
					System.out.println(i);
				}
			}
			bin.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * add Formula Co Formula
	 */
	public static void addFormulaCoFormulaRelations() {
		try {
			BufferedReader bin = new BufferedReader(new InputStreamReader(
					new FileInputStream(filePath + File.separator
							+ "fcof.dat")));
			String readline = "";
			int i = 0;
			while ((readline = bin.readLine()) != null) {
				String s[] = readline.split(",");
				String fromID = "F_" + s[0];
				String toID = "F_"+s[1];
				Label f_label = DynamicLabel.label("formula");
				Node foundFrom = graphDb.findNode(f_label, "id", fromID);
				Node foundTo = graphDb.findNode(f_label, "id", toID);
				Relationship relationship = foundFrom.createRelationshipTo(
						foundTo, RelTypes.Co_occurrence);
				relationship.setProperty("weight", Double.parseDouble(s[2]));
				i++;
				if (i % 100000 == 0) {
					System.out.println(i);
				}
			}
			bin.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}

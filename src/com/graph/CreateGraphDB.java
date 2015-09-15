package com.graph;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

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
		IndexDefinition video_indexDefinition;
		IndexDefinition slides_indexDefinition;
		IndexDefinition sourcecode_indexDefinition;
		try (Transaction tx = graphDb.beginTx()) {
			Schema schema = graphDb.schema();
			// wiki
			wiki_indexDefinition = schema.indexFor(DynamicLabel.label("wiki"))
					.on("id").create();
			//schema.awaitIndexOnline(wiki_indexDefinition, 10, TimeUnit.SECONDS);
			// video
			video_indexDefinition = schema
					.indexFor(DynamicLabel.label("video")).on("id").create();
			//schema.awaitIndexOnline(video_indexDefinition, 10, TimeUnit.SECONDS);
			// slides
			slides_indexDefinition = schema
					.indexFor(DynamicLabel.label("slides")).on("id").create();
			//schema.awaitIndexOnline(slides_indexDefinition, 10,TimeUnit.SECONDS);
			// sourcecode
			sourcecode_indexDefinition = schema
					.indexFor(DynamicLabel.label("sourcecode")).on("id")
					.create();
			//schema.awaitIndexOnline(sourcecode_indexDefinition, 10,TimeUnit.SECONDS);

			tx.success();
		}

		// START SNIPPET: transaction
		try (Transaction tx = graphDb.beginTx()) {
			// Database operations go here
			// END SNIPPET: transaction
			// START SNIPPET: addData

			// Create some nodes and index their ids with the IndexService
			System.out.println("adding resources...");
			addResourceNodesWithIndexing();

			tx.success();
		}

		try (Transaction tx = graphDb.beginTx()) {
			// Add resource relationship
			System.out.println("adding resource relations...");
			addResourceRelations();
			tx.success();
		}

		// create keyword index
		IndexDefinition keyword_indexDefinition;
		try (Transaction tx = graphDb.beginTx()) {
			Schema schema = graphDb.schema();
			// keyword
			keyword_indexDefinition = schema
					.indexFor(DynamicLabel.label("keywords")).on("id").create();
			//schema.awaitIndexOnline(keyword_indexDefinition, 10,TimeUnit.SECONDS);

			tx.success();
		}

		try (Transaction tx = graphDb.beginTx()) {
			// Add keywords
			System.out.println("adding keywords...");
			addKeywordsNodesWithIndexing();

			tx.success();
		}

		try (Transaction tx = graphDb.beginTx()) {
			// Add relationship
			System.out.println("adding keywords relations...");
			addKeywordsRelations();

			tx.success();
		}

		// create paper index
		IndexDefinition paper_indexDefinition;
		try (Transaction tx = graphDb.beginTx()) {
			Schema schema = graphDb.schema();
			// keyword
			paper_indexDefinition = schema
					.indexFor(DynamicLabel.label("paper")).on("id").create();
			//schema.awaitIndexOnline(paper_indexDefinition, 10,TimeUnit.SECONDS);

			tx.success();
		}

		try (Transaction tx = graphDb.beginTx()) {
			// Add papers
			System.out.println("adding papers...");
			addPaperNodesWithIndexing();

			tx.success();
		}

		try (Transaction tx = graphDb.beginTx()) {
			// Add paper cite paper
			System.out.println("adding paper cite relations...");
			addPaperCiteRelations();

			tx.success();
		}

		try (Transaction tx = graphDb.beginTx()) {
			// Add paper relate to keyword
			System.out.println("adding paper keywords relations...");
			addPaperKeywordRelations();

			tx.success();
		}

		try (Transaction tx = graphDb.beginTx()) {
			// Add paper contribute keyword
			System.out.println("adding paper contribute keywords relations...");
			addPaperContributeKeywordRelations();

			tx.success();
		}

		try (Transaction tx = graphDb.beginTx()) {
			// Add keyword cite keyword
			System.out.println("adding keyword cite keywords relations...");
			addKeywordCiteKeywordRelations();

			tx.success();
		}

		try (Transaction tx = graphDb.beginTx()) {
			// Add keyword co keyword
			System.out.println("adding keyword co keywords relations...");
			addKeywordCoKeywordRelations();

			tx.success();
		}

		try (Transaction tx = graphDb.beginTx()) {
			// Add keyword similarity keyword
			System.out.println("adding keyword similarity slides relations...");
			addKeywordSimSlidesRelations();

			tx.success();
		}

		try (Transaction tx = graphDb.beginTx()) {
			// Add keyword similarity sourcecode
			System.out
					.println("adding keyword similarity sourcecode relations...");
			addKeywordSimSourcecodeRelations();

			tx.success();
		}

		try (Transaction tx = graphDb.beginTx()) {
			// Add keyword similarity video
			System.out.println("adding keyword similarity video relations...");
			addKeywordSimVideoRelations();

			tx.success();
		}

		try (Transaction tx = graphDb.beginTx()) {
			// Add keyword similarity wiki
			System.out.println("adding keyword similarity wiki relations...");
			addKeywordSimWikiRelations();

			tx.success();
		}

		// START SNIPPET: transaction
		try (Transaction tx = graphDb.beginTx()) {
			// Database operations go here
			// END SNIPPET: transaction
			// START SNIPPET: addData

			// Create some nodes and index their ids with the IndexService
			System.out.println("adding paper sim slides resources...");
			addPaperSimSlidesRelations();

			tx.success();
		}

		try (Transaction tx = graphDb.beginTx()) {
			// Add keyword similarity sourcecode
			System.out
					.println("adding paper similarity sourcecode relations...");
			addPaperSimSourcecodeRelations();

			tx.success();
		}

		try (Transaction tx = graphDb.beginTx()) {
			// Add keyword similarity video
			System.out.println("adding keyword similarity video relations...");
			addPaperSimVideoRelations();

			tx.success();
		}

		try (Transaction tx = graphDb.beginTx()) {
			// Add keyword similarity wiki
			System.out.println("adding keyword similarity wiki relations...");
			addPaperSimWikiRelations();

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
	 * add resource nodes with indexing
	 */
	public static void addResourceNodesWithIndexing() {
		String readline = "";
		try {
			BufferedReader bin = new BufferedReader(new InputStreamReader(
					new FileInputStream(filePath + File.separator
							+ "rsource.dat")));
			int i = 0;
			while ((readline = bin.readLine()) != null) {
				String s[] = readline.split("_\\|_");
				String ids[] = s[0].split("_");
				String id = "Res_" + ids[1];
				String title = s[1];
				String type = s[2];
				Label label = DynamicLabel.label(type);
				Node node = graphDb.createNode(label);
				node.setProperty("id", id);
				node.setProperty("title", title);
				i++;
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
	 * add resource nodes with indexing
	 */
	public static void addResourceRelations() {
		String readline = "";
		try {
			BufferedReader bin = new BufferedReader(new InputStreamReader(
					new FileInputStream(filePath + File.separator
							+ "r-rsource.dat")));

			int i = 0;
			while ((readline = bin.readLine()) != null) {
				String s[] = readline.split(",");
				String ids0[] = s[0].split("_");
				String fromID = "Res_" + ids0[1];
				String ids1[] = s[1].split("_");
				String toID = "Res_" + ids1[1];
				String type = s[3];
				Label label = DynamicLabel.label(type);
				Node foundFrom = graphDb.findNode(label, "id", fromID);
				Node foundTo = graphDb.findNode(label, "id", toID);
				if (foundFrom != null && foundTo != null) {
					Relationship relationship = foundFrom.createRelationshipTo(
							foundTo, RelTypes.Related);
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
	 * add keywords nodes with indexing
	 */
	public static void addKeywordsNodesWithIndexing() {
		try {
			BufferedReader bin = new BufferedReader(new InputStreamReader(
					new FileInputStream(filePath + File.separator
							+ "keywords.dat")));
			String readline = "";
			int i = 0;
			while ((readline = bin.readLine()) != null) {
				String s[] = readline.split(",");
				String id = s[0];
				String title = s[1];
				Label label = DynamicLabel.label("keywords");
				Node node = graphDb.createNode(label);
				node.setProperty("id", id);
				node.setProperty("title", title);
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
	 * add resource nodes with indexing
	 */
	public static void addKeywordsRelations() {
		try {
			BufferedReader bin = new BufferedReader(new InputStreamReader(
					new FileInputStream(filePath + File.separator
							+ "r-keywords.dat")));
			String readline = "";
			int i = 0;
			while ((readline = bin.readLine()) != null) {
				String s[] = readline.split(",");
				String ids0[] = s[0].split("_");
				String fromID = "Res_" + ids0[1];
				String toID = s[1];
				String resType = s[3];
				Label res_label = DynamicLabel.label(resType);
				Label k_label = DynamicLabel.label("keywords");
				Node foundFrom = graphDb.findNode(res_label, "id", fromID);
				Node foundTo = graphDb.findNode(k_label, "id", toID);
				Relationship relationship = foundFrom.createRelationshipTo(
						foundTo, RelTypes.Related);
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
	 * add paper nodes with indexing
	 */
	public static void addPaperNodesWithIndexing() {
		String readline = "";
		try {
			BufferedReader bin = new BufferedReader(new InputStreamReader(
					new FileInputStream(filePath + File.separator
							+ "papers.dat")));
			int i = 0;
			while ((readline = bin.readLine()) != null) {
				String s[] = readline.split(",");
				String id = s[1];
				String acmID = s[3];
				String year = s[5];
				Label label = DynamicLabel.label("paper");
				Node node = graphDb.createNode(label);
				String ids[] = id.split("_");
				
				node.setProperty("id", "P_"+ids[1]);
				node.setProperty("acmID", acmID);
				node.setProperty("year", year);
				i++;
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
	 * add paper cite paper relation with indexing
	 */
	public static void addPaperCiteRelations() {
		String readline = "";
		try {
			BufferedReader bin = new BufferedReader(new InputStreamReader(
					new FileInputStream(filePath + File.separator
							+ "paper-cite-paper.dat")));

			int i = 0;
			while ((readline = bin.readLine()) != null) {
				String s[] = readline.split(",");
				String fids[] = s[2].split("_");
				String fromID = "P_"+fids[1];
				String tids[] = s[3].split("_");
				String toID = "P_"+tids[1];
				Label label = DynamicLabel.label("paper");
				Node foundFrom = graphDb.findNode(label, "id", fromID);
				Node foundTo = graphDb.findNode(label, "id", toID);
				if (foundFrom != null && foundTo != null) {
					Relationship relationship = foundFrom.createRelationshipTo(
							foundTo, RelTypes.Cite);
					relationship
							.setProperty("weight", Double.parseDouble(s[1]));
					relationship.setProperty("count", Integer.parseInt(s[4]));
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
	 * add paper relate keyword relation with indexing
	 */
	public static void addPaperKeywordRelations() {
		String readline = "";
		try {
			BufferedReader bin = new BufferedReader(new InputStreamReader(
					new FileInputStream(filePath + File.separator
							+ "paper-topic.dat")));

			int i = 0;
			while ((readline = bin.readLine()) != null) {
				String s[] = readline.split(",");
				String fromID = s[2];
				String toID = s[3];
				Label fromlabel = DynamicLabel.label("paper");
				Label tolabel = DynamicLabel.label("keywords");
				Node foundFrom = graphDb.findNode(fromlabel, "id", fromID);
				Node foundTo = graphDb.findNode(tolabel, "id", toID);
				if (foundFrom != null && foundTo != null) {
					Relationship relationship = foundFrom.createRelationshipTo(
							foundTo, RelTypes.Have);
					relationship
							.setProperty("weight", Double.parseDouble(s[1]));
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
	 * add paper contribute keyword relation with indexing
	 */
	public static void addPaperContributeKeywordRelations() {
		String readline = "";
		try {
			BufferedReader bin = new BufferedReader(new InputStreamReader(
					new FileInputStream(filePath + File.separator
							+ "paper-topic-contribute.dat")));

			int i = 0;
			while ((readline = bin.readLine()) != null) {
				String s[] = readline.split(",");
				String fromID = s[3];
				String toID = s[2];
				
				Label tolabel = DynamicLabel.label("paper");
				Label fromlabel = DynamicLabel.label("keywords");
				Node foundFrom = graphDb.findNode(fromlabel, "id", fromID);
				Node foundTo = graphDb.findNode(tolabel, "id", toID);
				if (foundFrom != null && foundTo != null) {
					Relationship relationship = foundFrom.createRelationshipTo(
							foundTo, RelTypes.Contributed_by);
					relationship
							.setProperty("weight", Double.parseDouble(s[1]));
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
	 * add keyword cite keyword relation with indexing
	 */
	public static void addKeywordCiteKeywordRelations() {
		String readline = "";
		try {
			BufferedReader bin = new BufferedReader(new InputStreamReader(
					new FileInputStream(filePath + File.separator
							+ "topic-cite-topic.dat")));

			int i = 0;
			while ((readline = bin.readLine()) != null) {
				String s[] = readline.split(",");
				String fromID = "K_" + s[0];
				String toID = "K_" + s[1];
				
				Label label = DynamicLabel.label("keywords");
				Node foundFrom = graphDb.findNode(label, "id", fromID);
				Node foundTo = graphDb.findNode(label, "id", toID);
				
				if (foundFrom != null && foundTo != null) {
					Relationship relationship = foundFrom.createRelationshipTo(
							foundTo, RelTypes.Cite);
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
	 * add keyword co paper keyword with indexing
	 */
	public static void addKeywordCoKeywordRelations() {
		String readline = "";
		try {
			BufferedReader bin = new BufferedReader(new InputStreamReader(
					new FileInputStream(filePath + File.separator
							+ "topic-co-topic.dat")));

			int i = 0;
			while ((readline = bin.readLine()) != null) {
				String s[] = readline.split(",");
				String fromID = "K_" + s[0];
				String toID = "K_" + s[1];
				Label label = DynamicLabel.label("keywords");
				Node foundFrom = graphDb.findNode(label, "id", fromID);
				Node foundTo = graphDb.findNode(label, "id", toID);
				if (foundFrom != null && foundTo != null) {
					Relationship relationship = foundFrom.createRelationshipTo(
							foundTo, RelTypes.Co_occurrence);
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
	 * add keyword similarity slides with indexing
	 */
	public static void addKeywordSimSlidesRelations() {
		String readline = "";
		try {
			BufferedReader bin = new BufferedReader(new InputStreamReader(
					new FileInputStream(filePath + File.separator
							+ "keyword_slides.dat")));
			int i = 0;
			while ((readline = bin.readLine()) != null) {
				String s[] = readline.split(",");
				String fromID = s[0];
				for (int j = 1; j < s.length; j = j + 2) {
					String toID = s[j];
					Label fromlabel = DynamicLabel.label("keywords");
					Label tolabel = DynamicLabel.label("slides");
					Node foundFrom = graphDb.findNode(fromlabel, "id", fromID);
					Node foundTo = graphDb.findNode(tolabel, "id", toID);
					if (foundFrom != null && foundTo != null) {
						Relationship relationship = foundFrom
								.createRelationshipTo(foundTo, RelTypes.Sim);
						relationship.setProperty("weight",
								Double.parseDouble(s[j + 1]));
						i++;
					} else {
						System.out.println("fromID:" + fromID + " toID" + toID);
					}
					if (i % 100000 == 0) {
						System.out.println(i);
					}
				}

			}
			bin.close();
		} catch (Exception e) {
			System.out.println(readline);
			e.printStackTrace();
		}
	}

	/**
	 * add keyword similarity sourcecode with indexing
	 */
	public static void addKeywordSimSourcecodeRelations() {
		String readline = "";
		try {
			BufferedReader bin = new BufferedReader(new InputStreamReader(
					new FileInputStream(filePath + File.separator
							+ "keyword_sourcecode.dat")));
			int i = 0;
			while ((readline = bin.readLine()) != null) {
				String s[] = readline.split(",");
				String fromID = s[0];
				for (int j = 1; j < s.length; j = j + 2) {
					String toID = s[j];
					Label fromlabel = DynamicLabel.label("keywords");
					Label tolabel = DynamicLabel.label("sourcecode");
					Node foundFrom = graphDb.findNode(fromlabel, "id", fromID);
					Node foundTo = graphDb.findNode(tolabel, "id", toID);
					if (foundFrom != null && foundTo != null) {
						Relationship relationship = foundFrom
								.createRelationshipTo(foundTo, RelTypes.Sim);
						relationship.setProperty("weight",
								Double.parseDouble(s[j + 1]));
						i++;
					} else {
						System.out.println("fromID:" + fromID + " toID" + toID);
					}
					if (i % 100000 == 0) {
						System.out.println(i);
					}
				}

			}
			bin.close();
		} catch (Exception e) {
			System.out.println(readline);
			e.printStackTrace();
		}
	}

	/**
	 * add keyword similarity video with indexing
	 */
	public static void addKeywordSimVideoRelations() {
		String readline = "";
		try {
			BufferedReader bin = new BufferedReader(new InputStreamReader(
					new FileInputStream(filePath + File.separator
							+ "keyword_video.dat")));
			int i = 0;
			while ((readline = bin.readLine()) != null) {
				String s[] = readline.split(",");
				String fromID = s[0];
				for (int j = 1; j < s.length; j = j + 2) {
					String toID = s[j];
					Label fromlabel = DynamicLabel.label("keywords");
					Label tolabel = DynamicLabel.label("video");
					Node foundFrom = graphDb.findNode(fromlabel, "id", fromID);
					Node foundTo = graphDb.findNode(tolabel, "id", toID);
					if (foundFrom != null && foundTo != null) {
						Relationship relationship = foundFrom
								.createRelationshipTo(foundTo, RelTypes.Sim);
						relationship.setProperty("weight",
								Double.parseDouble(s[j + 1]));
						i++;
					} else {
						System.out.println("fromID:" + fromID + " toID" + toID);
					}
					if (i % 100000 == 0) {
						System.out.println(i);
					}
				}

			}
			bin.close();
		} catch (Exception e) {
			System.out.println(readline);
			e.printStackTrace();
		}
	}

	/**
	 * add keyword similarity wiki with indexing
	 */
	public static void addKeywordSimWikiRelations() {
		String readline = "";
		try {
			BufferedReader bin = new BufferedReader(new InputStreamReader(
					new FileInputStream(filePath + File.separator
							+ "keyword_wiki.dat")));
			int i = 0;
			while ((readline = bin.readLine()) != null) {
				String s[] = readline.split(",");
				String fromID = s[0];
				for (int j = 1; j < s.length; j = j + 2) {
					String toID = s[j];
					Label fromlabel = DynamicLabel.label("keywords");
					Label tolabel = DynamicLabel.label("wiki");
					Node foundFrom = graphDb.findNode(fromlabel, "id", fromID);
					Node foundTo = graphDb.findNode(tolabel, "id", toID);
					if (foundFrom != null && foundTo != null) {
						Relationship relationship = foundFrom
								.createRelationshipTo(foundTo, RelTypes.Sim);
						relationship.setProperty("weight",
								Double.parseDouble(s[j + 1]));
						i++;
					} else {
						System.out.println("fromID:" + fromID + " toID" + toID);
					}
					if (i % 100000 == 0) {
						System.out.println(i);
					}
				}

			}
			bin.close();
		} catch (Exception e) {
			System.out.println(readline);
			e.printStackTrace();
		}
	}

	/**
	 * add keyword similarity slides with indexing
	 */
	public static void addPaperSimSlidesRelations() {
		String readline = "";
		try {
			BufferedReader bin = new BufferedReader(new InputStreamReader(
					new FileInputStream(filePath + File.separator
							+ "slides.txt")));
			int i = 0;
			while ((readline = bin.readLine()) != null) {
				String s[] = readline.split(",");
				String fromID = "P_" + s[0];
				for (int j = 1; j < s.length; j = j + 2) {
					String toID = "Res_" + s[j];
					Label fromlabel = DynamicLabel.label("paper");
					Label tolabel = DynamicLabel.label("slides");
					Node foundFrom = graphDb.findNode(fromlabel, "id", fromID);
					Node foundTo = graphDb.findNode(tolabel, "id", toID);
					if (foundFrom != null && foundTo != null) {
						Relationship relationship = foundFrom
								.createRelationshipTo(foundTo, RelTypes.Sim);
						relationship.setProperty("weight",
								Double.parseDouble(s[j + 1]));
						i++;
					} else {
						System.out.println("fromID:" + fromID + " toID" + toID);
					}
					if (i % 100000 == 0) {
						System.out.println(i);
					}
				}
			}
			bin.close();
		} catch (Exception e) {
			System.out.println(readline);
			e.printStackTrace();
		}
	}

	/**
	 * add keyword similarity sourcecode with indexing
	 */
	public static void addPaperSimSourcecodeRelations() {
		String readline = "";
		try {
			BufferedReader bin = new BufferedReader(new InputStreamReader(
					new FileInputStream(filePath + File.separator
							+ "sourcecode.txt")));
			int i = 0;
			while ((readline = bin.readLine()) != null) {
				String s[] = readline.split(",");
				String fromID = "P_" + s[0];
				for (int j = 1; j < s.length; j = j + 2) {
					String toID = "Res_" + s[j];
					Label fromlabel = DynamicLabel.label("paper");
					Label tolabel = DynamicLabel.label("sourcecode");
					Node foundFrom = graphDb.findNode(fromlabel, "id", fromID);
					Node foundTo = graphDb.findNode(tolabel, "id", toID);
					if (foundFrom != null && foundTo != null) {
						Relationship relationship = foundFrom
								.createRelationshipTo(foundTo, RelTypes.Sim);
						relationship.setProperty("weight",
								Double.parseDouble(s[j + 1]));
						i++;
					} else {
						System.out.println("fromID:" + fromID + " toID" + toID);
					}
					if (i % 100000 == 0) {
						System.out.println(i);
					}
				}

			}
			bin.close();
		} catch (Exception e) {
			System.out.println(readline);
			e.printStackTrace();
		}
	}

	/**
	 * add keyword similarity video with indexing
	 */
	public static void addPaperSimVideoRelations() {
		String readline = "";
		try {
			BufferedReader bin = new BufferedReader(
					new InputStreamReader(new FileInputStream(filePath
							+ File.separator + "video.txt")));
			int i = 0;
			while ((readline = bin.readLine()) != null) {
				String s[] = readline.split(",");
				String fromID = "P_" + s[0];
				for (int j = 1; j < s.length; j = j + 2) {
					String toID = "Res_" + s[j];
					Label fromlabel = DynamicLabel.label("paper");
					Label tolabel = DynamicLabel.label("video");
					Node foundFrom = graphDb.findNode(fromlabel, "id", fromID);
					Node foundTo = graphDb.findNode(tolabel, "id", toID);
					if (foundFrom != null && foundTo != null) {
						Relationship relationship = foundFrom
								.createRelationshipTo(foundTo, RelTypes.Sim);
						relationship.setProperty("weight",
								Double.parseDouble(s[j + 1]));
						i++;
					} else {
						System.out.println("fromID:" + fromID + " toID" + toID);
					}
					if (i % 100000 == 0) {
						System.out.println(i);
					}
				}

			}
			bin.close();
		} catch (Exception e) {
			System.out.println(readline);
			e.printStackTrace();
		}
	}

	/**
	 * add keyword similarity wiki with indexing
	 */
	public static void addPaperSimWikiRelations() {
		String readline = "";
		try {
			BufferedReader bin = new BufferedReader(
					new InputStreamReader(new FileInputStream(filePath
							+ File.separator + "wiki.txt")));
			int i = 0;
			while ((readline = bin.readLine()) != null) {
				String s[] = readline.split(",");
				String fromID = "P_" + s[0];
				for (int j = 1; j < s.length; j = j + 2) {
					String toID = "Res_" + s[j];
					Label fromlabel = DynamicLabel.label("paper");
					Label tolabel = DynamicLabel.label("wiki");
					Node foundFrom = graphDb.findNode(fromlabel, "id", fromID);
					Node foundTo = graphDb.findNode(tolabel, "id", toID);
					if (foundFrom != null && foundTo != null) {
						Relationship relationship = foundFrom
								.createRelationshipTo(foundTo, RelTypes.Sim);
						relationship.setProperty("weight",
								Double.parseDouble(s[j + 1]));
						i++;
					} else {
						System.out.println("fromID:" + fromID + " toID" + toID);
					}
					if (i % 100000 == 0) {
						System.out.println(i);
					}
				}

			}
			bin.close();
		} catch (Exception e) {
			System.out.println(readline);
			e.printStackTrace();
		}
	}

}

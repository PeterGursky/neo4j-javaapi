package sk.upjs.nosql.neo4j_javaapi;

import java.io.File;
import java.util.List;
import java.util.Map.Entry;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import sk.gursky.nosql.aislike.DaoFactory;
import sk.gursky.nosql.crawl.DownloadDao;
import sk.gursky.nosql.crawl.entity.Download;
import sk.gursky.nosql.crawl.entity.Page;

public class App 
{
	public static final String LOCAL_STORE_DIR = "neo4jrepozitar";
	private GraphDatabaseService db;
	
	public static enum TypyHran implements RelationshipType {
		CONTAINS,
		LINKS_TO,
		SEED_PAGE
	}
	
    public static void main( String[] args ) {
    	App app = new App();
    	app.vytvorANaplnDb();
    	app.vypisSeedNodes();
    	app.shutdown();
    }
    
    public void vypisSeedNodes() {
    	ResourceIterator<Node> downloadNodesIterator = db.findNodes(Label.label("download"));
    	while(downloadNodesIterator.hasNext()) {
    		Node downloadNode = downloadNodesIterator.next();
    		Node seedPage = downloadNode.getSingleRelationship(TypyHran.SEED_PAGE, Direction.OUTGOING).getEndNode();
    		System.out.println(seedPage.getAllProperties());
    	}
    }
    
    public void shutdown() {
    	db.shutdown();
    }
    
    public void vytvorANaplnDb() {
    	GraphDatabaseService db = new GraphDatabaseFactory()
    			.newEmbeddedDatabase(new File(LOCAL_STORE_DIR));
    	DownloadDao downloadDao = DaoFactory.INSTANCE.getDownloadDao();
    	List<Download> downloads = downloadDao.getAllDownloads();
    	
    	try(Transaction tr = db.beginTx()) {
    		for(Download d : downloads) {
    			Node downloadNode = db.createNode(Label.label("download"));
    			downloadNode.setProperty("id", d.getId());
    			// + dalsie
    			for (Page page: d.getPages()) {
    				Node pageNode = db.createNode(Label.label("page"));
    				pageNode.setProperty("id", page.getId());
    				pageNode.setProperty("url", page.getUrl());
    				pageNode.setProperty("isDetailPage", page.isDetailPage());
    				downloadNode.createRelationshipTo(pageNode, TypyHran.CONTAINS);
    				if (page.getId() == d.getSeedPage().getId()) {
    					downloadNode.createRelationshipTo(pageNode, TypyHran.SEED_PAGE);
    				}
    			}
    		}
    		tr.success();
    	}
		for(Download d : downloads) {
   			for (Page page: d.getPages()) {
		    	try(Transaction tr = db.beginTx()) {
		    		Node p1 = db.findNode(Label.label("page"),"id", page.getId());
		    		for (Entry<String, Page> entry: page.getxPathToChildrenPages().entrySet()) {
		    			Node p2 = db.findNode(Label.label("page"),"id", entry.getValue().getId());
		    			Relationship hrana = p1.createRelationshipTo(p2, TypyHran.LINKS_TO);
		    			hrana.setProperty("xPath", entry.getKey());
		    		}
		    		tr.success();
		    	}
   			}
		}
    }
}

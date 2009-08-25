
package plugins.XMLSpider;

import freenet.keys.FreenetURI;
import freenet.support.Logger;
import freenet.support.LoggerHook.InvalidThresholdException;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Iterator;
import plugins.XMLSpider.db.Page;
import plugins.XMLSpider.db.PerstRoot;
import plugins.XMLSpider.db.Status;
import plugins.XMLSpider.db.Term;
import plugins.XMLSpider.org.garret.perst.Storage;
import plugins.XMLSpider.org.garret.perst.StorageFactory;

/**
 * TODO This class will import an older database file and add entries which dont
 * already exist in the current database, initially it will be made to run executed
 * separately and put into the interface for first run in future.
 * 
 * TODO options
 * TODO Logger
 * 
 * @author MikeB
 */
public class IndexImporter {
	private String sourcePath;
	private Storage sourcedb;
	private PerstRoot sourceRoot;
	
	private String destPath;
	private Storage destdb;
	private PerstRoot destRoot;
	
	private boolean allowOld;
	private boolean allowAppend;
	private boolean allowStopWords;
	
	
	
	public static void main(String[] args) {
		try {
			Logger.setupStdoutLogging(Logger.MINOR, "log");
		} catch (InvalidThresholdException ex) {
			Logger.error(IndexImporter.class, "", ex);
		}
		
		System.out.println("This importer is designed to import XMLSpider database versions v34 - v38, it is entirely possible it will work on earlier versions but I haven't looked at them");
		boolean append = false;
		boolean old = false;
		boolean stopWords = false;
		String destinationpath = null;
		String sourcepath = null;
		int dbcache = 16000;

		for (int i = 0; i < args.length; i++) {
			String string = args[i];
			if(string.startsWith("-m")){
				dbcache=Integer.parseInt(string.substring(2));
				System.out.println("Set memory size to "+dbcache);
			}else if(string.startsWith("-")){
				for (int j = 1; j < string.length(); j++) {
					char ch = string.charAt(i);
					if(ch == 'a')
						append = true;
					else if(ch == 'O')
						old = true;
					else if(ch == 'S')
						stopWords = true;
					else
						System.out.println("Unrecognised argument : "+ch);
				}
				
			}else if(sourcepath == null)
				sourcepath = string;
			else if(destinationpath == null)
				destinationpath = string;
			else
				System.out.println("Did not understand argument \""+string+"\"");
		}
		
		IndexImporter importer;
		try {
			importer = new IndexImporter(sourcepath, destinationpath, old, append, stopWords, dbcache);
			importer.importV38Database();
		} catch (IOException ex) {
			System.out.println(ex.getMessage());
		}
		
	}
	private int dbcache;

	/**
	 * Sets up an index database importer
	 * @param sourcepath the database to import from
	 * @param destinationpath the database to write to, if writing to an existing db you should probably back it up first
	 * @param allowOld if set all pages will be imported, if unset an effort is made to not import old ones
	 * @param allowAppend the normal behaviour is not to allow adding to an existing database, this forces the behaviour
	 * @param allowStopWords the normal behaviour is not to import stopWords, this overrides
	 */
	private IndexImporter(String sourcepath, String destinationpath, boolean allowOld, boolean allowAppend, boolean allowStopWords, int dbcache) throws IOException {
		
		this.sourcePath = sourcepath;
		this.destPath = destinationpath;
		this.allowOld = allowOld;
		this.allowAppend = allowAppend;
		this.allowStopWords = allowStopWords;
		this.dbcache = dbcache;
		
		if(!allowAppend && (new File(destinationpath)).exists())
			throw new IOException("Destination path already exists, you are recommended to not import into an existing database without backing it up, oreride this by setting allowAppend (\"-a\")");
		
		System.out.println("Opening source DB : "+sourcepath);
		sourcedb = getDB(sourcepath);
		sourceRoot = (PerstRoot)sourcedb.getRoot();
		if(sourceRoot==null)
			throw new NullPointerException("Source root is null, thats bad");
		System.out.println("memory="+Runtime.getRuntime().totalMemory());
		
		System.out.println("Opening destination DB : " + destinationpath);
		destdb = getDB(destinationpath);
		destRoot = getPerstRoot(destdb);
		System.out.println("memory="+Runtime.getRuntime().totalMemory());
	}
	
	/**
	 * This will import the succeeded pages, queuedPages and terms. Stopwords will be ignored unless overridden, old usks should also be ignored but this action is not guarenteed
	 * @param source
	 * @param destination
	 */
	public void importV38Database(){
		try{
			System.out.println("Importing v34 - v38 database from "+sourcePath);

			int i =0;
			for (Iterator<Term> it = sourceRoot.getTermIterator(null, null); it.hasNext();) {
				if(i++ % 9 == 0)
					System.out.print("Importing term " + i + "/" + sourceRoot.getTermCount()+"\r");
				Term term = it.next();
				addV38TermToDestination(term);
			}
			System.out.println("Finished importing " + i + "/" + sourceRoot.getTermCount() + " terms");

			System.out.println("\nImporting Queued pages");
			i =0;
			for (Iterator<Page> it = sourceRoot.getPages(Status.QUEUED); it.hasNext();) {
				if(i++ % 9 == 0)
					System.out.print("Importing queued page " + i + "/" + sourceRoot.getPageCount(Status.QUEUED) + "\r");
				Page object = it.next();
				getPageInDestinationFromV38(object);
			}
			System.out.println("Finished importing " + i + "/" + sourceRoot.getPageCount(Status.QUEUED) + " queued pages");

		}finally{
			sourcedb.close();
			destdb.close();
		}
	}


	/**
	 * Get any relevent data from a Page object and add it to the destination and any relevent indexes if it does not already exist
	 * @param object Page object from V34 - V38 db
	 * @throws ClassCastException if object passed does not identify itself as a Page
	 * @throws UnsupportedOperationException if unrecognised field is found in the page, this would be caused by a database version which is not yet supported in this inport code
	 */
	private Page getPageInDestinationFromV38(Page page) throws ClassCastException {

		try{
			Page existingPage = destRoot.getPageByURI(new FreenetURI(page.getURI()), false, null, allowOld);

			if(existingPage != null && ((existingPage.getStatus() == Status.SUCCEEDED || (existingPage.getStatus() == Status.QUEUED && page.getStatus() == Status.QUEUED) || page.getStatus() == Status.FAILED) ))
				return existingPage;		// dont change an existing Page if it stores more than the importing page, just return it


			// Otherwise make a new one
			//System.out.println("Adding "+object.toString()+" as : "+newPage);

			return page.v38ImportPage(destdb, allowOld);
		} catch (SecurityException ex) {
			Logger.error(IndexImporter.class, null, ex);
		} catch (MalformedURLException ex) {
			Logger.error(IndexImporter.class, null, ex);
		}
		return null;
	}

	/**
	 * Imports a Term object from a V34 - V38 db adding it to a new db, adds any pages that contain this term and their TermPositions
	 * @param object a v34 - v38 Term object
	 */
	private void addV38TermToDestination(Term term) {
		if(XMLSpider.isStopWord(term.getWord()))
			return;

		Term newTerm = destRoot.getTermByWord(term.getWord(), true);
		// Iterate through the set of pages containing this term, adding them if neccessary and their TermPositions
		for (Iterator<Page> it = term.v38PageIterator(); it.hasNext();) {
			Page page = it.next();
			Page newPage = getPageInDestinationFromV38(page);
			if(newPage != null)
				newTerm.addPage(newPage.getId(), page.v38GetTermPosition(newTerm.getMD5()).clone(destdb));
		}
	}
	
	/**
	 * Get a perst database
	 */
	private Storage getDB(String dbpath) {
		Storage db = StorageFactory.getInstance().createStorage();
		db.setProperty("perst.object.cache.kind", "pinned");
		db.setProperty("perst.object.cache.init.size", dbcache/2);
		db.setProperty("perst.alternative.btree", true);
		db.setProperty("perst.string.encoding", "UTF-8");
		db.setProperty("perst.concurrent.iterator", true);

		db.open(dbpath);
		return db;
	}

	/**
	 * Gets the perstroot from a new database or creates a new one if the database doesnt have one
	 * @param db
	 * @return
	 */
	private PerstRoot getPerstRoot(Storage db) {
		PerstRoot root = (PerstRoot) db.getRoot();
		if (root == null)
			PerstRoot.createRoot(db);
		root = (PerstRoot) db.getRoot();
		
		return root;
	}
}

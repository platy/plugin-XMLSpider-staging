
package plugins.XMLSpider;

import freenet.keys.FreenetURI;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import plugins.XMLSpider.db.Page;
import plugins.XMLSpider.db.PerstRoot;
import plugins.XMLSpider.db.Status;
import plugins.XMLSpider.db.Term;
import plugins.XMLSpider.db.TermPosition;
import plugins.XMLSpider.org.garret.perst.FieldIndex;
import plugins.XMLSpider.org.garret.perst.IPersistent;
import plugins.XMLSpider.org.garret.perst.IPersistentMap;
import plugins.XMLSpider.org.garret.perst.IPersistentSet;
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
	private IPersistent sourceRoot;
	private Storage sourcedb;
	private Storage destdb;
	private PerstRoot destRoot;
	private String destPath;
	private String sourcePath;
	
	private boolean allowOld;
	private boolean allowAppend;
	private boolean allowStopWords;
	
	
	
	public static void main(String[] args) {
		System.out.println("This importer is designed to import XMLSpider database versions v34 - v38, it is entirely possible it will work on earlier versions but I haven't looked at them");
		boolean append = false;
		boolean old = false;
		boolean stopWords = false;
		String destinationpath = null;
		String sourcepath = null;

		for (int i = 0; i < args.length; i++) {
			String string = args[i];
			if(string.startsWith("-")){
				if(string.contains("a"))
					append = true;
				if(string.contains("O"))
					old = true;
				if(string.contains("S"))
					stopWords = true;
			}else if(sourcepath == null)
				sourcepath = string;
			else if(destinationpath == null)
				destinationpath = string;
			else
				System.out.println("Did not understand argument \""+string+"\"");
		}
		
		IndexImporter importer;
		try {
			importer = new IndexImporter(sourcepath, destinationpath, old, append, stopWords);
			importer.importV38Database();
		} catch (IOException ex) {
			System.out.println(ex.getMessage());
		}
		
	}

	/**
	 * Sets up an index database importer
	 * @param sourcepath the database to import from
	 * @param destinationpath the database to write to, if writing to an existing db you should probably back it up first
	 * @param allowOld if set all pages will be imported, if unset an effort is made to not import old ones
	 * @param allowAppend the normal behaviour is not to allow adding to an existing database, this forces the behaviour
	 * @param allowStopWords the normal behaviour is not to import stopWords, this overrides
	 */
	private IndexImporter(String sourcepath, String destinationpath, boolean allowOld, boolean allowAppend, boolean allowStopWords) throws IOException {
		System.out.println("Importing from "+sourcepath);
		
		this.sourcePath = sourcepath;
		this.destPath = destinationpath;
		this.allowOld = allowOld;
		this.allowAppend = allowAppend;
		this.allowStopWords = allowStopWords;
		
		if(!allowAppend && (new File(destinationpath)).exists())
			throw new IOException("Destination path already exists, you are recommended to not import into an existing database without backing it up, oreride this by setting allowAppend (\"-a\")");
		
		sourcedb = getDB(sourcepath);
		sourceRoot = sourcedb.getRoot();
		if(sourceRoot==null)
			throw new NullPointerException("Source root is null, thats bad");
		System.out.println("memory="+Runtime.getRuntime().totalMemory());
		
		
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

			FieldIndex terms = getFieldIndex(sourceRoot, "md5Term");
			int i =0;
			for (Iterator it = terms.iterator(); it.hasNext();) {
				if(i++ % 9 == 0)
					System.out.print("Importing term " + i + "/" + terms.size()+"\r");
				Object object = it.next();
				addV38TermToDestination(object);
			}
			System.out.println("Finished importing " + i + "/" + terms.size() + " terms");

			System.out.println("\nImporting Queued pages");
			FieldIndex queuedPages = getFieldIndex(sourceRoot, "queuedPages");
			i =0;
			for (Iterator it = queuedPages.iterator(); it.hasNext();) {
				if(i++ % 9 == 0)
					System.out.print("Importing queued page " + i + "/" + queuedPages.size() + "\r");
				Object object = it.next();
				getPageInDestinationFromV38(object);
			}
			System.out.println("Finished importing " + i + "/" + queuedPages.size() + " queued pages");

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
	private Page getPageInDestinationFromV38(Object object) throws ClassCastException {
		if(!object.getClass().getName().equals("plugins.XMLSpider.db.Page"))
			throw new ClassCastException("Need plugins.XMLSpider.db.Page, got "+object.getClass().getName());

		Field[] fields = object.getClass().getDeclaredFields();
		long id;				// not used, it will get a new id
		String uri = null;
		String pageTitle = null;
		Status status = null;
		long lastChange = 0;
		String comment = null;
		IPersistentMap<String, TermPosition> termPosMap = null;
		long filesize = 0;
		String mimeType = null;
		
		try{
			for (Field field : fields) {
				field.setAccessible(true);
				String fieldName = field.getName();

				if(fieldName.equals("id"))
					id = field.getLong(object);
				else if(fieldName.equals("uri"))
					uri = (String)field.get(object);
				else if(fieldName.equals("pageTitle"))
					pageTitle = (String)field.get(object);
				else if(fieldName.equals("status"))
					status = (Status)field.get(object);
				else if(fieldName.equals("filesize"))
					filesize = field.getLong(object);
				else if(fieldName.equals("mimetype"))
					mimeType = (String)field.get(object);
				else if(fieldName.equals("lastChange"))
					lastChange = field.getLong(object);
				else if(fieldName.equals("comment"))
					comment = (String)field.get(object);
				else if(fieldName.equals("termPosMap")) // This will be a little more difficult maybe, or just really slow, this will be accessed on the term side of things, thats where we use it
					termPosMap = (IPersistentMap<String, TermPosition>)field.get(object);
				else if(Arrays.asList("uskuri", "edition", "retries", "termCount", "meta").contains(fieldName))
					;	// ignore
				else
					throw new UnsupportedOperationException("dunno what Exception to use but this db version must not be supported yet, please post on devl with the version of db you are importing from : "+fieldName);
			
			}
			Page newPage = destRoot.getPageByURI(new FreenetURI(uri), false, null, allowOld);

			if(newPage != null && ((newPage.getStatus() == Status.SUCCEEDED || (newPage.getStatus() == Status.QUEUED && status == Status.QUEUED) || status == Status.FAILED) ))
				return newPage;		// dont change an existing Page if it stores more than the importing page, just return it
			// Otherwise make a new one
			newPage = destRoot.getPageByURI(new FreenetURI(uri), true, comment, status, allowOld);

			newPage.setPageTitle(pageTitle);
			if(filesize!=0 && mimeType!=null)
				newPage.setMeta(
					"size=" + Long.toString(filesize),
					"mime=" + mimeType
				);
			else if(mimeType!=null)
				newPage.setMeta("mime=" + mimeType);
			else if(filesize!=0)
				newPage.setMeta("size="+Long.toString(filesize));
			newPage.setRetries(0);
			newPage.setLastChange(lastChange);
			
			int termCount = 0;	// Count the terms in the termPositions map
			if(termPosMap != null) {
				for (TermPosition termPos : termPosMap.values()) {
					termCount += termPos.positions.length;
				}
				newPage.setTermCount(termCount);
			}

			//System.out.println("Adding "+object.toString()+" as : "+newPage);

			return newPage;
		} catch (SecurityException ex) {
			Logger.getLogger(IndexImporter.class.getName()).log(Level.SEVERE, null, ex);
		} catch (MalformedURLException ex) {
			Logger.getLogger(IndexImporter.class.getName()).log(Level.SEVERE, null, ex);
		} catch(IllegalAccessException e) {
			System.out.println("Error should not have been thrown, running under odd security settings");
		}
		return null;
	}

	/**
	 * Imports a Term object from a V34 - V38 db adding it to a new db, adds any pages that contain this term and their TermPositions
	 * @param object a v34 - v38 Term object
	 */
	private void addV38TermToDestination(Object object) {
		if(!object.getClass().getName().equals("plugins.XMLSpider.db.Term"))
			throw new ClassCastException("Need plugins.XMLSpider.db.Term, got "+object.getClass().getName());
		
		//System.out.println("Adding "+object.toString());

		Field[] fields = object.getClass().getDeclaredFields();
		String word = null;
		String md5;				// not used as it will be generated fresh from the word
		IPersistentSet pageSet = null;	// we iterate through these pages to add positions & pages
		
		try{
			for (Field field : fields) {
				field.setAccessible(true);
				String fieldName = field.getName();

				//System.out.println(" field : "+fieldName +" "+field.get(object));
				
				if(fieldName.equals("word")) {
					word = (String)field.get(object);
					if (XMLSpider.isStopWord(word))
						return;		// Stopwords are not imported TODO make this optional
				} else if (fieldName.equals("md5"))
					md5 = (String)field.get(object);
				else if (fieldName.equals("pageSet")){
					pageSet = (IPersistentSet)field.get(object);
					if(pageSet == null)
						return;		// if this is null it means we cant find out which pages have it
				}else if (fieldName.equals("pageMap"))
					; // ignore
				else
					throw new UnsupportedOperationException("dunno what Exception to use but this db version must not be supported yet, please post on devl with the version of db you are importing from : "+fieldName);
			}
			
			Term newTerm = destRoot.getTermByWord(word, true);
			// Iterate through the set of pages containing this term, adding them if neccessary and their TermPositions
			for (Object object1 : pageSet) {
				Page newPage = getPageInDestinationFromV38(object1);
				if(newPage != null)
					newTerm.addPage(newPage.getId(), getTermPositionsV38(object1, newTerm.getMD5()));
			}
			
		} catch (NoSuchFieldException ex) {
			Logger.getLogger(IndexImporter.class.getName()).log(Level.SEVERE, null, ex);
		} catch (IllegalArgumentException ex) {
			Logger.getLogger(IndexImporter.class.getName()).log(Level.SEVERE, null, ex);
		} catch (IllegalAccessException ex) {
			Logger.getLogger(IndexImporter.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
	
	/**
	 * Get a perst database
	 */
	private Storage getDB(String dbpath) {
		Storage db = StorageFactory.getInstance().createStorage();
		db.setProperty("perst.object.cache.kind", "pinned");
		db.setProperty("perst.object.cache.init.size", 8192);
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

	/**
	 * Gets a FieldIndex from a root
	 * @param root database root to look for FieldIndex
	 * @param fieldIndexName name of FieldIndex to find
	 * @return the FieldIndex from the db
	 */
	private FieldIndex getFieldIndex(IPersistent root, String fieldIndexName) {
		try {
			//PerstRoot destinationRoot = de
			Field suceededPagesField = root.getClass().getDeclaredField(fieldIndexName);
			suceededPagesField.setAccessible(true);
			FieldIndex suceededPages = (FieldIndex)suceededPagesField.get(root);
			return suceededPages;
		} catch (IllegalArgumentException ex) {
			Logger.getLogger(IndexImporter.class.getName()).log(Level.SEVERE, null, ex);
		} catch (IllegalAccessException ex) {
			Logger.getLogger(IndexImporter.class.getName()).log(Level.SEVERE, null, ex);
		} catch (NoSuchFieldException ex) {
			Logger.getLogger(IndexImporter.class.getName()).log(Level.SEVERE, null, ex);
		} catch (SecurityException ex) {
			Logger.getLogger(IndexImporter.class.getName()).log(Level.SEVERE, null, ex);
		}
		return null;
	}

	// Takes a page from a 34 - 38 version db and returns a new TermPosition for the md5 supplied
	private TermPosition getTermPositionsV38(Object object1, String mD5) throws NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
		Field termPosField = object1.getClass().getDeclaredField("termPosMap");
		termPosField.setAccessible(true);
		Map<String, ? extends IPersistent> termPosMap = (Map<String, ? extends IPersistent>)termPosField.get(object1);
		IPersistent termPos = termPosMap.get(mD5);
		int[] positions = (int[]) termPos.getClass().getDeclaredField("positions").get(termPos);
		return new TermPosition(destdb, positions);
	}
}

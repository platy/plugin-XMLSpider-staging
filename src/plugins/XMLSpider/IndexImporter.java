
package plugins.XMLSpider;

import freenet.keys.FreenetURI;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
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
 * @author MikeB
 */
public class IndexImporter {
	
	
	public static void main(String[] args) {
		String[] sourcepaths = new String[]{
			//"/home/devl/Freenet/XMLSpider-37.dbs",
			"/home/devl/Freenet/XMLSpider-40.dbs",
			"/home/devl/Freenet/XMLSpider-41.dbs",
			"/home/devl/Freenet/XMLSpider-42.dbs",
			"/home/devl/Freenet/XMLSpider-42.dbso",
			"/home/devl/Freenet/XMLSpider-43.dbs"
		};
		String destinationpath = "XMLSpider-new.dbs";
		
		for (String sourcepath : sourcepaths) {
			IndexImporter importer = new IndexImporter(sourcepath, destinationpath);
			importer.importDatabase();
		}
		
	}
	private IPersistent sourceRoot;
	private Storage sourcedb;
	private Storage destdb;
	private PerstRoot destRoot;
	private int dbType = 0;

	private IndexImporter(String sourcepath, String destinationpath) {
		System.out.println("Importing from "+sourcepath);
		
		sourcedb = getDB(sourcepath);
		System.out.println("memory="+Runtime.getRuntime().totalMemory());
		destdb = getDB(destinationpath);
		
		System.out.println("memory="+Runtime.getRuntime().totalMemory());
		
		sourceRoot = sourcedb.getRoot();
		destRoot = getPerstRoot(destdb);
	}

	/**
	 * This will import the succeeded pages, queuedPages and terms. Stopwords will be ignored, old usks should also be ignored but this action is not guarenteed
	 * @param source
	 * @param destination
	 */
	public void importDatabase(){
		int i;

		System.out.println("Importing Terms");

		FieldIndex terms = getFieldIndex(sourceRoot, "md5Term");
		i =0;
		for (Iterator it = terms.iterator(); it.hasNext() && i++ < 50;) {
			Object object = it.next();
			addTermToDestination(object);
		}

//		// db 36 will produce suceeded pages when it does the terms as they are referenced in the terms TODO
//		FieldIndex suceededPages = getFieldIndex(sourceRoot, "succeededPages");
//		i =0;
//		for (Iterator it = suceededPages.iterator(); it.hasNext() && i++ < 50;) {
//			Object object = it.next();
//			getPageInDestination(object);
//		}

		System.out.println("Importing Queued page");
		FieldIndex queuedPages = getFieldIndex(sourceRoot, "queuedPages");
		i =0;
		for (Iterator it = queuedPages.iterator(); it.hasNext() && i++ < 50;) {
			Object object = it.next();
			getPageInDestination(object);
		}

		
		sourcedb.close();
		destdb.close();
	}

	/**
	 * Get any relevent data from a Page object and add it to the destination and any relevent indexes if it does not already exist
	 * @param object
	 * @throws ClassCastException if object passed does not identify itself as a Page
	 * @throws UnsupportedOperationException if unrecognised field is found in the page, this would be caused by a database version which is not yet supported in this inport code
	 * TODO check for newer versions, as this will keep the db smaller, it could actually speed it up
	 */
	private Page getPageInDestination(Object object) throws ClassCastException {
		if(!object.getClass().getName().equals("plugins.XMLSpider.db.Page"))
			throw new ClassCastException("Need plugins.XMLSpider.db.Page, got "+object.getClass().getName());

		Field[] fields = object.getClass().getDeclaredFields();
		long id;				// not used, it will get a new id
		String uri = null;
		String uskuri;			// not used, found from uri
		long edition;			// not used, found from uri
		String pageTitle = null;
		Status status = null;
		int retries = 0;
		int termCount = 0;		// TODO make sure Library handles 0's here properly
		String[] meta = null;
		long lastChange = 0;
		String comment = null;
		IPersistentMap<String, ? extends IPersistent> termPosMap = null;
		
		try{
			for (Field field : fields) {
				field.setAccessible(true);
				String fieldName = field.getName();

				if(fieldName.equals("id"))
					id = field.getLong(object);
				else if(fieldName.equals("uri"))
					uri = (String)field.get(object);
				else if(fieldName.equals("uskuri"))
					uskuri = (String)field.get(object);
				else if(fieldName.equals("edition"))
					edition = field.getLong(object);
				else if(fieldName.equals("pageTitle"))
					pageTitle = (String)field.get(object);
				else if(fieldName.equals("status"))
					status = (Status)field.get(object);
				else if(fieldName.equals("retries"))
					retries = field.getInt(object);
				else if(fieldName.equals("termCount"))
					termCount = field.getInt(object);
				else if(fieldName.equals("meta"))
					meta = (String[])field.get(object);
				else if(fieldName.equals("lastChange"))
					lastChange = field.getLong(object);
				else if(fieldName.equals("comment"))
					comment = (String)field.get(object);
				else if(fieldName.equals("termPosMap")) // This will be a little more difficult maybe, or just really slow, this will be accessed on the term side of things, thats where we use it
					termPosMap = (IPersistentMap<String, ? extends IPersistent>)field.get(object);
				else
					throw new UnsupportedOperationException("dunno what Exception to use but this db version must not be supported yet, please post on devl with the version of db you are importing from : "+fieldName);
			
			}
			Page newPage = destRoot.getPageByURI(new FreenetURI(uri), false, null);

			if(newPage != null)
				return newPage;		// dont change an existing Page, just return it

			newPage = destRoot.getPageByURI(new FreenetURI(uri), true, comment, status);
			newPage.setMeta(meta);
			newPage.setPageTitle(pageTitle);
			newPage.setTermCount(termCount);
			newPage.setLastChange(lastChange);
			newPage.setRetries(retries);

			System.out.println("Adding "+object.toString()+" as : "+newPage);

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

	private void addTermToDestination(Object object) {
		if(!object.getClass().getName().equals("plugins.XMLSpider.db.Term"))
			throw new ClassCastException("Need plugins.XMLSpider.db.Term, got "+object.getClass().getName());
		System.out.println("Adding "+object.toString());

		Field[] fields = object.getClass().getDeclaredFields();
		String word = null;
		String md5;				// not used as it will be generated fresh from the word
		IPersistentSet pageSet = null;	// we iterate through these pages to add positions
		IPersistentMap pageMap = null;
		
		try{
			for (Field field : fields) {
				field.setAccessible(true);
				String fieldName = field.getName();

				System.out.println(" field : "+fieldName +" "+field.get(object));
				
				if(fieldName.equals("word"))
					word = (String)field.get(object);
				else if (fieldName.equals("md5"))
					md5 = (String)field.get(object);
				else if (fieldName.equals("pageSet")){
					pageSet = (IPersistentSet)field.get(object);
					if(pageSet == null)
						return;		// if one of these are null it means we cant find out which pages have it
				}else if (fieldName.equals("pageMap")){
					pageMap = (IPersistentMap)field.get(object);
					if(pageMap == null)
						return;		// if one of these are null it means we cant find out which pages have it
				}else
					throw new UnsupportedOperationException("dunno what Exception to use but this db version must not be supported yet, please post on devl with the version of db you are importing from : "+fieldName);
			}
			
			Term newTerm = destRoot.getTermByWord(word, true);

			if(pageMap != null){
				dbType = 40;
				newTerm.getPositions().putAll(pageMap);
			} else if (pageSet != null) {
				dbType = 36;
				for (Object object1 : pageSet) {
					Page newPage = getPageInDestination(object1);
					newTerm.addPage(newPage.getId(), getTermPositions36(object1, newTerm.getMD5()));
				}
			} else throw new UnsupportedOperationException("One of those should have worked, this is an older version than the importer copes with");
			
		} catch (NoSuchFieldException ex) {
			Logger.getLogger(IndexImporter.class.getName()).log(Level.SEVERE, null, ex);
		} catch (IllegalArgumentException ex) {
			Logger.getLogger(IndexImporter.class.getName()).log(Level.SEVERE, null, ex);
		} catch (IllegalAccessException ex) {
			Logger.getLogger(IndexImporter.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
	
	/**
	 * Get a database
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
	
	private PerstRoot getPerstRoot(Storage db) {
		PerstRoot root = (PerstRoot) db.getRoot();
		if (root == null)
			PerstRoot.createRoot(db);
		root = (PerstRoot) db.getRoot();
		
		return root;
	}

	private FieldIndex getFieldIndex(IPersistent sourceRoot, String fieldIndexName) {
		try {
			//PerstRoot destinationRoot = de
			Field suceededPagesField = sourceRoot.getClass().getDeclaredField(fieldIndexName);
			suceededPagesField.setAccessible(true);
			FieldIndex suceededPages = (FieldIndex)suceededPagesField.get(sourceRoot);
			System.out.println("field = " + suceededPages);
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

	// Takes a page from a 36 version and returns a new TermPosition for the md5 supplied
	private TermPosition getTermPositions36(Object object1, String mD5) throws NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
		Map<String, ? extends IPersistent> termPosMap = (Map<String, ? extends IPersistent>)object1.getClass().getDeclaredField("termPosMap").get(object1);
		IPersistent termPos = termPosMap.get(mD5);
		int[] positions = (int[]) termPos.getClass().getDeclaredField("positions").get(termPos);
		return new TermPosition(destdb, positions);
	}
}

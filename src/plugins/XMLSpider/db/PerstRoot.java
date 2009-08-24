package plugins.XMLSpider.db;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import plugins.XMLSpider.org.garret.perst.FieldIndex;
import plugins.XMLSpider.org.garret.perst.IterableIterator;
import plugins.XMLSpider.org.garret.perst.Key;
import plugins.XMLSpider.org.garret.perst.Persistent;
import plugins.XMLSpider.org.garret.perst.Storage;
import freenet.keys.FreenetURI;
import plugins.XMLSpider.org.garret.perst.StorageError;

public class PerstRoot extends Persistent {
	protected FieldIndex<Term> md5Term;
	protected FieldIndex<Term> wordTerm;

	protected FieldIndex<Page> idPage;
	protected FieldIndex<Page> uriPage;
	/** Indexes the usk's with editions zeroed so old ones can be discarded easily */
	protected FieldIndex<Page> uskPage;
	protected FieldIndex<Page> queuedPages;
	protected FieldIndex<Page> failedPages;
	protected FieldIndex<Page> succeededPages;

	private Config config;

	public PerstRoot() {
	}

	public static PerstRoot createRoot(Storage storage) {
		PerstRoot root = new PerstRoot();

		root.md5Term = storage.createFieldIndex(Term.class, "md5", true);
		root.wordTerm = storage.createFieldIndex(Term.class, "word", true);

		root.idPage = storage.createFieldIndex(Page.class, "id", true);
		root.uriPage = storage.createFieldIndex(Page.class, "uri", true);
		root.uskPage = storage.createFieldIndex(Page.class, "uskuri", false);
		root.queuedPages = storage.createFieldIndex(Page.class, "retries", false); // Queued pages sorted by retries and change date
		root.failedPages = storage.createFieldIndex(Page.class, "lastChange", false);
		root.succeededPages = storage.createFieldIndex(Page.class, "lastChange", false);

		root.config = new Config(storage);

		storage.setRoot(root);

		return root;
	}

	public ArrayList<Page> getAllEditions(FreenetURI uri) {
		Key key = new Key(uri.setSuggestedEdition(0).toString(), true);
		return uskPage.getList(key, key);
	}

	public Term getTermByMD5(String termMd5) {
		md5Term.exclusiveLock();
		wordTerm.exclusiveLock();
		try {
			return md5Term.get(new Key(termMd5));
		} finally {
			wordTerm.unlock();
			md5Term.unlock();
		}
	}

	public Term getTermByWord(String word, boolean create) {
		md5Term.exclusiveLock();
		wordTerm.exclusiveLock();
		try {
			Term term = wordTerm.get(new Key(word));

			if (create && term == null) {
				word = new String(word); // force a new instance, prevent referring to the old char[]			
				term = new Term(word, getStorage());
				md5Term.put(term);
				wordTerm.put(term);
			}

			return term;
		} finally {
			wordTerm.unlock();
			md5Term.unlock();
		}
	}

	public IterableIterator<Term> getTermIterator(String from, String till) {
		md5Term.sharedLock();
		try {
			return md5Term.iterator(from, till, 0);
		} finally {
			md5Term.unlock();
		}
	}

	public List<Term> getTermList() {
		md5Term.sharedLock();
		try {
			return md5Term.getList(null, null);
		} finally {
			md5Term.unlock();
		}
	}

	public int getTermCount() {
		md5Term.sharedLock();
		try {
			return md5Term.size();
		} finally {
			md5Term.unlock();
		}
	}
	
	public Page getPageByURI(FreenetURI uri, boolean create, String comment, boolean allowOld) {
		return getPageByURI(uri, create, comment, Status.QUEUED, allowOld);
	}

	public Page getPageByURI(FreenetURI uri, boolean create, String comment, Status initialStatus, boolean allowOld) {//TODO old
		idPage.exclusiveLock();
		uriPage.exclusiveLock();
		uskPage.exclusiveLock();

		switch (initialStatus){
			case QUEUED :
				queuedPages.exclusiveLock();
				break;
			case FAILED :
				failedPages.exclusiveLock();
				break;
			case SUCCEEDED :
				succeededPages.exclusiveLock();
				break;
			default:
				throw new RuntimeException(initialStatus + " is bad, so is this error");
		}
		try {
			Page page = uriPage.get(new Key(uri.toString()));

			if (create && page == null && (allowOld || !newerSuceeded(uri))) {
				page = new Page(uri, comment, getStorage());

				idPage.append(page);
				uriPage.put(page);
				if(page.getUSK()!=null)
					uskPage.put(page);
				switch (initialStatus){
					case QUEUED :
						queuedPages.put(page);
						break;
					case FAILED :
						failedPages.put(page);
						break;
					case SUCCEEDED :
						succeededPages.put(page);
						break;
				}
			}

			return page;
		} finally {
			switch (initialStatus){
				case QUEUED :
					queuedPages.unlock();
					break;
				case FAILED :
					failedPages.unlock();
					break;
				case SUCCEEDED :
					succeededPages.unlock();
					break;
			}
			uriPage.unlock();
			uskPage.unlock();
			idPage.unlock();
		}
	}

	public Page getPageById(long id) {
		idPage.sharedLock();
		try {
			Page page = idPage.get(id);
			return page;
		} finally {
			idPage.unlock();
		}
	}

	/**
	 * Checks whether a newer edition of an USK has suceeded, returns true if
	 * this is a usk and a newer one has suceeded, returns false otherwise
	 * @param uri
	 * @return
	 */
	public boolean newerSuceeded(FreenetURI uri) {
		if(uri.isUSK() || uri.isSSKForUSK()) {
			ArrayList<Page> editions = getAllEditions(uri);
			for (Page page : editions) {
				if (page.getStatus()==Status.SUCCEEDED && page.getEdition() > uri.getEdition())
					return true;
			}
		}

		return false;
	}

	/**
	 * remove instances of this page from all the indexes, ignore StorageErrors as they will be thorwn if the page doesnt exist
	 * @param page
	 */
	public void removePage(Page page) {
		try{
			failedPages.remove(page);
		}catch(StorageError e){}
		try{
			queuedPages.remove(page);
		}catch(StorageError e){}
		try{
			succeededPages.remove(page);
		}catch(StorageError e){}
		try{
			idPage.remove(page);
		}catch(StorageError e){}
		try{
			uriPage.remove(page);
		}catch(StorageError e){}
		try{
			uskPage.remove(page);
		}catch(StorageError e){}
		
		page.deallocate();
	}

	FieldIndex<Page> getPageIndex(Status status) {
		switch (status) {
		case FAILED:
			return failedPages;
		case QUEUED:
			return queuedPages;
		case SUCCEEDED:
			return succeededPages;
		default:
			return null;
		}
	}

	public void exclusiveLock(Status status) {
		FieldIndex<Page> index = getPageIndex(status);
		index.exclusiveLock();
	}

	public void sharedLockPages(Status status) {
		FieldIndex<Page> index = getPageIndex(status);
		index.sharedLock();
	}

	public void unlockPages(Status status) {
		FieldIndex<Page> index = getPageIndex(status);
		index.unlock();
	}
	
	public Iterator<Page> getPages(Status status) {
		FieldIndex<Page> index = getPageIndex(status);
		index.sharedLock();
		try {
			return index.iterator();
		} finally {
			index.unlock();
		}
	}

	public int getPageCount(Status status) {
		FieldIndex<Page> index = getPageIndex(status);
		index.sharedLock();
		try {
			return index.size();
		} finally {
			index.unlock();
		}
	}

	public synchronized void setConfig(Config config) {		
		this.config = config;
		modify();
	}

	public synchronized Config getConfig() {
		return config;
	}
}

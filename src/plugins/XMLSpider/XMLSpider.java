/* This code is part of Freenet. It is distributed under the GNU General
 * Public License, version 2 (or at your option any later version). See
 * http://www.gnu.org/ for further details of the GPL. */
package plugins.XMLSpider;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.db4o.ObjectContainer;

import plugins.XMLSpider.db.Config;
import plugins.XMLSpider.db.Page;
import plugins.XMLSpider.db.PerstRoot;
import plugins.XMLSpider.db.Status;
import plugins.XMLSpider.db.Term;
import plugins.XMLSpider.db.TermPosition;
import plugins.XMLSpider.org.garret.perst.Storage;
import plugins.XMLSpider.org.garret.perst.StorageFactory;
import plugins.XMLSpider.web.WebInterface;
import freenet.client.ClientMetadata;
import freenet.client.FetchContext;
import freenet.client.FetchException;
import freenet.client.FetchResult;
import freenet.client.async.ClientContext;
import freenet.client.async.ClientGetCallback;
import freenet.client.async.ClientGetter;
import freenet.client.async.USKCallback;
import freenet.clients.http.PageMaker;
import freenet.clients.http.filter.ContentFilter;
import freenet.clients.http.filter.FoundURICallback;
import freenet.clients.http.filter.UnsafeContentTypeException;
import freenet.keys.FreenetURI;
import freenet.keys.USK;
import freenet.l10n.L10n.LANGUAGE;
import freenet.node.NodeClientCore;
import freenet.node.RequestClient;
import freenet.node.RequestStarter;
import freenet.pluginmanager.FredPlugin;
import freenet.pluginmanager.FredPluginL10n;
import freenet.pluginmanager.FredPluginRealVersioned;
import freenet.pluginmanager.FredPluginThreadless;
import freenet.pluginmanager.FredPluginVersioned;
import freenet.pluginmanager.PluginRespirator;
import freenet.support.Logger;
import freenet.support.api.Bucket;
import freenet.support.io.NativeThread;
import freenet.support.io.NullBucketFactory;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;

/**
 * XMLSpider. Produces xml index for searching words. 
 * In case the size of the index grows up a specific threshold the index is split into several subindices.
 * The indexing key is the md5 hash of the word.
 * 
 *  @author swati goyal
 *  
 */
public class XMLSpider implements FredPlugin, FredPluginThreadless,
		FredPluginVersioned, FredPluginRealVersioned, FredPluginL10n, USKCallback, RequestClient {

	//LibraryAPI1 library;

	public synchronized boolean cancelWrite() {
		if(writingIndex)
			return false;
		writeIndexScheduled = false;
		return true;
	}

	public Config getConfig() {
		return getRoot().getConfig();
	}

	public String getIndexWriterStatus() {
		return indexWriter.getCurrentSubindexPrefix();
	}

	public boolean isGarbageCollecting() {
		return garbageCollecting;
	}

	public synchronized boolean pauseWrite() {
		if(!writingIndex)
			return false;
		indexWriter.pause();
		writingIndex = false;
		writeIndexScheduled = false;
		return true;
	}

	// Set config asynchronously
	public void setConfig(Config config) {
		getRoot().setConfig(config); // hack -- may cause race condition. but this is more user friendly
		callbackExecutor.execute(new SetConfigCallback(config));
	}

	/** Document ID of fetching documents */
	protected Map<Page, ClientGetter> runningFetch = Collections.synchronizedMap(new HashMap<Page, ClientGetter>());

	/**
	 * Lists the allowed mime types of the fetched page. 
	 */
	protected Set<String> allowedMIMETypes;	

	static int dbVersion = 43;
	static int version = 37;
	public static final String pluginName = "XML spider " + version;

	public String getVersion() {
		return version + "(" + dbVersion + ") r" + Version.getSvnRevision();
	}
	
	public long getRealVersion() {
		return version;
	}

	private FetchContext ctx;
	private ClientContext clientContext;
	private boolean stopped = true;

	private NodeClientCore core;
	private PageMaker pageMaker;	
	private PluginRespirator pr;

	/**
	 * Adds the found uri to the list of to-be-retrieved uris. <p>Every usk uri added as ssk.
	 * @param uri the new uri that needs to be fetched for further indexing
	 */
	public void queueURI(FreenetURI uri, String comment, boolean force) {
		// dont queue if a newer version has succeeded
		if(getConfig().getDiscardOldEditions() && !force && newerSuceeded(uri) )
			return;
		
		String sURI = uri.toString();
		String lowerCaseURI = sURI.toLowerCase(Locale.US);
		for (String ext : getRoot().getConfig().getBadlistedExtensions())
			if (lowerCaseURI.endsWith(ext))
				return; // be smart

		if (uri.isUSK()) {
			if (uri.getSuggestedEdition() < 0)
				uri = uri.setSuggestedEdition((-1) * uri.getSuggestedEdition());
			try {
				uri = ((USK.create(uri)).getSSK()).getURI();
				(clientContext.uskManager).subscribe(USK.create(uri), this, false, this);
			} catch (Exception e) {
			}
		}

		db.beginThreadTransaction(Storage.EXCLUSIVE_TRANSACTION);
		boolean dbTransactionEnded = false;
		try {
			Page page = getRoot().getPageByURI(uri, true, comment);
			if (force && page.getStatus() != Status.QUEUED) {
				page.setStatus(Status.QUEUED);
				page.setComment(comment);
			}

			db.endThreadTransaction();
			dbTransactionEnded = true;
		} catch (RuntimeException e) {
			Logger.error(this, "Runtime Exception: " + e, e);		
			throw e;
		} finally {
			if (!dbTransactionEnded) {
				Logger.minor(this, "rollback transaction", new Exception("debug"));
				db.rollbackThreadTransaction();
			}
		}
	}

	/**
	 * Start requests from the queue if less than 80% of the max requests are running until the max requests are running.
	 * If discardOldEditions is set this will only start requests which are newer than any already succeeded
	 */
	public void startSomeRequests() {
		ArrayList<ClientGetter> toStart = null;
		synchronized (this) {
			if (stopped)
				return;
			synchronized (runningFetch) {
				int running = runningFetch.size();
				int maxParallelRequests = getRoot().getConfig().getMaxParallelRequests();

				if (running >= maxParallelRequests * 0.8)
					return;

				// Prepare to start
				toStart = new ArrayList<ClientGetter>(maxParallelRequests - running);
				db.beginThreadTransaction(Storage.COOPERATIVE_TRANSACTION);
				getRoot().sharedLockPages(Status.QUEUED);
				try {
					Iterator<Page> it = getRoot().getPages(Status.QUEUED);

					while (running + toStart.size() < maxParallelRequests && it.hasNext()) {
						Page page = it.next();
						try {
							// Skip if getting this page already or newer edition of it already suceeded
							if (runningFetch.containsKey(page) || (getConfig().getDiscardOldEditions() && newerSuceeded(new FreenetURI(page.getURI()))))
								continue;
							
							ClientGetter getter = makeGetter(page);

							Logger.minor(this, "Starting " + getter + " " + page);
							toStart.add(getter);
							runningFetch.put(page, getter);
						} catch (MalformedURLException e) {
							Logger.error(this, "IMPOSSIBLE-Malformed URI: " + page, e);
							page.setStatus(Status.FAILED);
						}
					}
				} finally {
					getRoot().unlockPages(Status.QUEUED);
					db.endThreadTransaction();
				}
			}
		}

		for (ClientGetter g : toStart) {
			try{
				g.start(null, clientContext);
			}catch(FetchException e){

			}
			Logger.minor(this, g + " started");
		}
	}

	/**
	 * Checks whether a newer edition of an USK has suceeded, returns true if
	 * this is a usk and a newer one has suceeded, returns false otherwise
	 * @param uri
	 * @return
	 */
	private boolean newerSuceeded(FreenetURI uri) {
		if(uri.isUSK() || uri.isSSKForUSK()) {
			ArrayList<Page> editions = getRoot().getAllEditions(uri);
			for (Page page : editions) {
				if (page.getEdition() > uri.getEdition())
					return true;
			}
		}

		return false;
	}

	/**
	 * Remove any older editions of this USK if it is one
	 * @param uri
	 */
	private void removeOlderSuceeded(FreenetURI uri) {
		if(uri.isUSK() || uri.isSSKForUSK()) {
			ArrayList<Page> editions = getRoot().getAllEditions(uri);
			for (Page page : editions) {
				if (page.getEdition() < uri.getEdition())
					getRoot().removePage(page);
			}
		}
	}

	/**
	 * Callback for fetching the pages
	 */
	private class ClientGetterCallback implements ClientGetCallback {
		final Page page;

		public ClientGetterCallback(Page page) {
			this.page = page;
		}

		public void onFailure(FetchException e, ClientGetter state, ObjectContainer container) {
			if (stopped)
				return;

			callbackExecutor.execute(new OnFailureCallback(e, state, page));
			Logger.minor(this, "Queued OnFailure: " + page + " (q:" + callbackExecutor.getQueue().size() + ")");
		}

		public void onSuccess(final FetchResult result, final ClientGetter state, ObjectContainer container) {
			if (stopped)
				return;

			callbackExecutor.execute(new OnSuccessCallback(result, state, page));
			Logger.minor(this, "Queued OnSuccess: " + page + " (q:" + callbackExecutor.getQueue().size() + ")");
		}

		public void onMajorProgress(ObjectContainer oc){}

		public String toString() {
			return super.toString() + ":" + page;
		}		
	}

	private ClientGetter makeGetter(Page page) throws MalformedURLException {
		ClientGetter getter = new ClientGetter(new ClientGetterCallback(page),
				new FreenetURI(page.getURI()), ctx,
		        getPollingPriorityProgress(), this, null, null);
		return getter;
	}

	protected class OnFailureCallback implements Runnable {
		private FetchException e;
		private ClientGetter state;
		private Page page;

		OnFailureCallback(FetchException e, ClientGetter state, Page page) {
			this.e = e;
			this.state = state;
			this.page = page;
		}

		public void run() {
			onFailure(e, state, page);
		}
	}

	/**
	 * Callback for asyncronous handling of a success
	 */
	protected class OnSuccessCallback implements Runnable {
		private FetchResult result;
		private ClientGetter state;
		private Page page;

		OnSuccessCallback(FetchResult result, ClientGetter state, Page page) {
			this.result = result;
			this.state = state;
			this.page = page;
		}

		public void run() {
			onSuccess(result, state, page);
		}
	}

	/**
	 * Start a thread to make an index
	 */
	public synchronized void scheduleMakeIndex() {
		scheduleMakeIndex("", false, "xml");
	}

	/**
	 * Start a thread to make an index
	 * @param indexdir
	 * @param separatepageindex
	 * @param indexformat
	 */
	public synchronized void scheduleMakeIndex(String indexdir, boolean separatepageindex, String indexformat) {
		if (writeIndexScheduled || writingIndex)
			return;

		callbackExecutor.execute(new MakeIndexCallback(indexdir, separatepageindex, indexformat));
		writeIndexScheduled = true;
	}

	protected class MakeIndexCallback implements Runnable {
		String indexdir;
		boolean separatepageindex;
		String indexformat;
		
		public MakeIndexCallback(String indexdir, boolean separatepageindex, String indexformat){
			this.indexdir = indexdir;
			this.separatepageindex = separatepageindex;
			this.indexformat = indexformat;
		}
		
		public void run() {
			synchronized(this){
				if(!writeIndexScheduled)
					return;
				writeIndexScheduled=false;
			}
			try {
				Logger.normal(this, "Making index");
				synchronized (this) {
					writingIndex = true;
				}

				garbageCollecting = true;
				db.gc();
				garbageCollecting = false;
				if(indexformat.equals("xml"))
					indexWriter.makeIndex(getRoot(), indexdir, separatepageindex);

				synchronized (this) {
					writingIndex = false;
					writeIndexScheduled = false;
				}				
			} catch (Exception e) {
				Logger.error(this, "Could not generate index: "+e, e);
			} finally {
				synchronized (this) {
					writingIndex = false;
					writeIndexScheduled = false;
					notifyAll();
				}
			}
		}
	}

	/**
	 * Set config asynchronously
	 */
	protected class SetConfigCallback implements Runnable {
		private Config config;

		SetConfigCallback(Config config) {
			this.config = config;
		}

		public void run() {
			synchronized (getRoot()) {
				getRoot().setConfig(config);
				startSomeRequests();
			}
		}
	}

	protected class StartSomeRequestsCallback implements Runnable {
		StartSomeRequestsCallback() {
		}

		public void run() {
			try {
				Thread.sleep(30000);
			} catch (InterruptedException e) {
				// ignore
			}
			startSomeRequests();
		}
	}

	protected static class CallbackPrioritizer implements Comparator<Runnable> {
		public int compare(Runnable o1, Runnable o2) {
			if (o1.getClass() == o2.getClass())
				return 0;

			return getPriority(o1) - getPriority(o2);
		}

		private int getPriority(Runnable r) {
			if (r instanceof SetConfigCallback)
				return 0;
			else if (r instanceof MakeIndexCallback)
				return 1;
			else if (r instanceof OnFailureCallback)
				return 2;
			else if (r instanceof OnSuccessCallback)
				return 3;
			else if (r instanceof StartSomeRequestsCallback)
				return 4;

			return -1;
		}
	}

	// this is java.util.concurrent.Executor, not freenet.support.Executor
	// always run with one thread --> more thread cause contention and slower!
	public ThreadPoolExecutor callbackExecutor = new ThreadPoolExecutor( //
			1, 1, 600, TimeUnit.SECONDS, //
	        new PriorityBlockingQueue<Runnable>(5, new CallbackPrioritizer()), //
	        new ThreadFactory() {
		        public Thread newThread(Runnable r) {
			        Thread t = new NativeThread(r, "XMLSpider", NativeThread.NORM_PRIORITY - 1, true);
			        t.setDaemon(true);
			        t.setContextClassLoader(XMLSpider.this.getClass().getClassLoader());
			        return t;
		        }
	        });

	/**
	 * Processes the successfully fetched uri for further outlinks.
	 * 
	 * @param result
	 * @param state
	 * @param page
	 */
	// single threaded
	protected void onSuccess(FetchResult result, ClientGetter state, Page page) {
		synchronized (this) {
			if (stopped)
				return;    				
		}
		
		FreenetURI uri = state.getURI();

		if(getConfig().getDiscardOldEditions()){
			// ignore if a newer one already suceeded
			if(newerSuceeded(uri))
				return;

			// Remove old versions of this usk
			removeOlderSuceeded(uri);
		}
		
		ClientMetadata cm = result.getMetadata();
		Bucket data = result.asBucket();
		String mimeType = cm.getMIMEType();
		
		// Set info on page
		page.setMeta(new String[]{
			"size=" + Long.toString(data.size()),
			"mime=" + mimeType
		});
		Logger.normal(this, "mime : " + mimeType);

		boolean dbTransactionEnded = false;
		db.beginThreadTransaction(Storage.EXCLUSIVE_TRANSACTION);
		try {
			/*
			 * instead of passing the current object, the pagecallback object for every page is
			 * passed to the content filter this has many benefits to efficiency, and allows us to
			 * identify trivially which page is being indexed. (we CANNOT rely on the base href
			 * provided).
			 */
			PageCallBack pageCallBack = new PageCallBack(page);
			Logger.minor(this, "Successful: " + uri + " : " + page.getId());

			try {
				if(!"text/plain".equals(mimeType))
					ContentFilter.filter(data, new NullBucketFactory(), mimeType,
							uri.toURI("http://127.0.0.1:8888/"), pageCallBack);
				else{
					BufferedReader br = new BufferedReader(new InputStreamReader(data.getInputStream()));
					String line;
					while((line = br.readLine())!=null)
						pageCallBack.onText(line, mimeType, uri.toURI("http://127.0.0.1:8888/"));
				}

			} catch (UnsafeContentTypeException e) {
				// wrong mime type
				page.setStatus(Status.SUCCEEDED);
				db.endThreadTransaction();
				dbTransactionEnded = true;

				Logger.minor(this, "UnsafeContentTypeException " + uri + " : " + page.getId(), e);
				return; // Ignore
			} catch (IOException e) {
				// ugh?
				Logger.error(this, "Bucket error?: " + e, e);
				return;
			} catch (Exception e) {
				// we have lots of invalid html on net - just normal, not error
				Logger.normal(this, "exception on content filter for " + page, e);
				return;
			}

			page.setStatus(Status.SUCCEEDED);
			db.endThreadTransaction();
			dbTransactionEnded  = true;

			Logger.minor(this, "Filtered " + uri + " : " + page.getId());
		} catch (RuntimeException e) {
			// other runtime exceptions
			Logger.error(this, "Runtime Exception: " + e, e);		
			throw e;
		} finally {
			try {
				data.free();

				synchronized (this) {
					runningFetch.remove(page);
				}
				if (!stopped)
					startSomeRequests();
			} finally {
				if (!dbTransactionEnded) {
					Logger.minor(this, "rollback transaction", new Exception("debug"));
					db.rollbackThreadTransaction();
					db.beginThreadTransaction(Storage.EXCLUSIVE_TRANSACTION);
					page.setStatus(Status.FAILED);
					db.endThreadTransaction();
				}
			}
		}
	}

	protected void onFailure(FetchException fe, ClientGetter state, Page page) {
		Logger.minor(this, "Failed: " + page + " : " + state, fe);

		synchronized (this) {
			if (stopped)
				return;
		}

		boolean dbTransactionEnded = false;
		db.beginThreadTransaction(Storage.EXCLUSIVE_TRANSACTION);
		try {
			synchronized (page) {
				if (fe.newURI != null) {
					// redirect, mark as succeeded
					queueURI(fe.newURI, "redirect from " + state.getURI(), false);
					page.setStatus(Status.SUCCEEDED);
				} else if (fe.isFatal()) {
					// too many tries or fatal, mark as failed
					page.setStatus(Status.FAILED);
				} else {
					// requeue at back
					page.setStatus(Status.QUEUED);
					page.incrementRetries();
				}
			}
			db.endThreadTransaction();
			dbTransactionEnded = true;
		} catch (Exception e) {
			Logger.error(this, "Unexcepected exception in onFailure(): " + e, e);
			throw new RuntimeException("Unexcepected exception in onFailure()", e);
		} finally {
			runningFetch.remove(page);
			if (!dbTransactionEnded) {
				Logger.minor(this, "rollback transaction", new Exception("debug"));
				db.rollbackThreadTransaction();
			}
		}

		startSomeRequests();
	} 

	private boolean writingIndex;
	private boolean writeIndexScheduled;
	private boolean garbageCollecting = false;

	protected IndexWriter indexWriter;

	public IndexWriter getIndexWriter() {
		return indexWriter;
	}

	/**
	 * Stop the plugin, pausing any writing which is happening
	 */
	public void terminate(){
		Logger.normal(this, "XMLSpider terminating");

		synchronized (this) {
			try {
				Runtime.getRuntime().removeShutdownHook(exitHook);
			} catch (IllegalStateException e) {
				// shutting down, ignore
			}
			stopped = true;

			for (Map.Entry<Page, ClientGetter> me : runningFetch.entrySet()) {
				ClientGetter getter = me.getValue();
				Logger.minor(this, "Canceling request" + getter);
				getter.cancel();
			}
			runningFetch.clear();
			callbackExecutor.shutdownNow();
		}
		try { callbackExecutor.awaitTermination(30, TimeUnit.SECONDS); } catch (InterruptedException e) {}
		try { db.close(); } catch (Exception e) {}
		
		webInterface.unload();

		Logger.normal(this, "XMLSpider terminated");
	}

	public class ExitHook implements Runnable {
		public void run() {
			Logger.normal(this, "XMLSpider exit hook called");
			terminate();
		}
	}

	private Thread exitHook = new Thread(new ExitHook());

	/**
	 * Start plugin
	 * @param pr
	 */
	public synchronized void runPlugin(PluginRespirator pr) {
		this.core = pr.getNode().clientCore;
		this.pr = pr;
		pageMaker = pr.getPageMaker();

		Runtime.getRuntime().addShutdownHook(exitHook);

		/* Initialize Fetch Context */
		this.ctx = pr.getHLSimpleClient().getFetchContext();
		ctx.maxSplitfileBlockRetries = 1;
		ctx.maxNonSplitfileRetries = 1;
		ctx.maxTempLength = 2 * 1024 * 1024;
		ctx.maxOutputLength = 2 * 1024 * 1024;
		allowedMIMETypes = new HashSet<String>();
		allowedMIMETypes.add("text/html");
		allowedMIMETypes.add("text/plain");
		allowedMIMETypes.add("application/xhtml+xml");
		ctx.allowedMIMETypes = new HashSet<String>(allowedMIMETypes);
		clientContext = pr.getNode().clientCore.clientContext;

		stopped = false;

		// Initial Database
		db = initDB();

		indexWriter = new IndexWriter(getConfig());
		webInterface = new WebInterface(this, pr.getHLSimpleClient(), pr.getToadletContainer(), pr.getNode().clientCore);
		webInterface.load();

		FreenetURI[] initialURIs = core.getBookmarkURIs();
		for (int i = 0; i < initialURIs.length; i++)
			queueURI(initialURIs[i], "bookmark", false);

		callbackExecutor.execute(new StartSomeRequestsCallback());
		// Get the Library API
		//library = pr.getPluginAPI("plugins.Library.Main");
	}

	private WebInterface webInterface;

	/**
	 * creates the callback object for each page.
	 *<p>Used to create inlinks and outlinks for each page separately.
	 * @author swati
	 *
	 */
	public class PageCallBack implements FoundURICallback{
		protected final Page page;

		protected final boolean logDEBUG = Logger.shouldLog(Logger.DEBUG, this); // per instance, allow changing on the fly

		PageCallBack(Page page) {
			this.page = page; 
		}

		public void foundURI(FreenetURI uri){
			// Ignore
		}

		/**
		 * When a link is found in html
		 * @param uri
		 * @param inline
		 */
		public void foundURI(FreenetURI uri, boolean inline){
			if (stopped)
				throw new RuntimeException("plugin stopping");
			if (logDEBUG)
				Logger.debug(this, "foundURI " + uri + " on " + page);
			queueURI(uri, "Added from " + page.getURI(), false);
		}

		protected Integer lastPosition = null;

		/**
		 * When text is found
		 * @param s
		 * @param type
		 * @param baseURI
		 */
		public void onText(String s, String type, URI baseURI){
			if (stopped)
				throw new RuntimeException("plugin stopping");
			if (logDEBUG)
				Logger.debug(this, "onText on " + page.getId() + " (" + baseURI + ")");

			if ("title".equalsIgnoreCase(type) && (s != null) && (s.length() != 0) && (s.indexOf('\n') < 0)) {
				/*
				 * title of the page 
				 */
				page.setPageTitle(s);
				type = "title";
			}
			else type = null;
			/*
			 * determine the position of the word in the retrieved page
			 * FIXME - replace with a real tokenizor
			 */
			String[] words = s.split("[^\\p{L}\\{N}]+");

			if(lastPosition == null)
				lastPosition = 1; 
			for (int i = 0; i < words.length; i++) {
				String word = words[i];
				if ((word == null) || (word.length() == 0))
					continue;
				word = word.toLowerCase();
				try{
					if(type == null)
						addWord(word, lastPosition + i);
					else
						addWord(word, Integer.MIN_VALUE + i); // Put title words in the right order starting at MIN_VALUE
				}
				catch (Exception e){} // If a word fails continue
			}

			if(type == null) {
				lastPosition = lastPosition + words.length;
			}
		}

		/**
		 * Add a word to the database for this page
		 * @param word
		 * @param position
		 * @throws java.lang.Exception
		 */
		private void addWord(String word, int position) {
			if (logDEBUG)
				Logger.debug(this, "addWord on " + page.getId() + " (" + word + "," + position + ")");

			// Skip word if it is a stop word
			if (word.length() < 3 || isStopWord(word))
				return;
			Term term = getTermByWord(word, true);
			TermPosition termPos = term.getTermPosition(page.getId(), true);
			termPos.addPositions(position);
			page.termCountIncrement();
		}
	}

	private static List<String> stopWords = Arrays.asList(new String[]{
		"the", "and", "that", "have", "for"		// English stop words
	});
	public static boolean isStopWord(String word) {
		return stopWords.contains(word);
	}

	public void onFoundEdition(long l, USK key, ObjectContainer container, ClientContext context, boolean metadata,
            short codec, byte[] data, boolean newKnownGood, boolean newSlotToo) {
		FreenetURI uri = key.getURI();
		/*-
		 * FIXME this code don't make sense 
		 *  (1) runningFetchesByURI contain SSK, not USK
		 *  (2) onFoundEdition always have the edition set
		 *  
		if(runningFetchesByURI.containsKey(uri)) runningFetchesByURI.remove(uri);
		uri = key.getURI().setSuggestedEdition(l);
		 */
		queueURI(uri, "USK found edition", true);
		startSomeRequests();
	}

	public short getPollingPriorityNormal() {
		return (short) Math.min(RequestStarter.MINIMUM_PRIORITY_CLASS, getRoot().getConfig().getRequestPriority() + 1);
	}

	public short getPollingPriorityProgress() {
		return getRoot().getConfig().getRequestPriority();
	}

	protected Storage db;

	/**
	 * Initializes Database
	 */
	private Storage initDB() {
		Storage db = StorageFactory.getInstance().createStorage();
		db.setProperty("perst.object.cache.kind", "pinned");
		db.setProperty("perst.object.cache.init.size", 8192);
		db.setProperty("perst.alternative.btree", true);
		db.setProperty("perst.string.encoding", "UTF-8");
		db.setProperty("perst.concurrent.iterator", true);

		db.open("XMLSpider-" + dbVersion + ".dbs");

		PerstRoot root = (PerstRoot) db.getRoot();
		if (root == null)
			PerstRoot.createRoot(db);

		return db;
	}

	public PerstRoot getRoot() {
		return (PerstRoot) db.getRoot();
	}

	protected Page getPageById(long id) {
		return getRoot().getPageById(id);
	}

	// language for I10N
	private LANGUAGE language;

	protected Term getTermByWord(String word, boolean create) {
		return getRoot().getTermByWord(word, create);
	}

	public String getString(String key) {
		// TODO return a translated string
		return key;
	}

	public void setLanguage(LANGUAGE newLanguage) {
		language = newLanguage;
	}

	public PageMaker getPageMaker() {
		return pageMaker;
	}

	public List<Page> getRunningFetch() {
		synchronized (runningFetch) {
			return new ArrayList<Page>(runningFetch.keySet());
		}
	}

	public boolean isWriteIndexScheduled() {
		return writeIndexScheduled;
	}

	public boolean isWritingIndex() {
		return writingIndex;
	}

	public PluginRespirator getPluginRespirator() {
		return pr;
	}

	public boolean persistent() {
		return false;
	}

	public void removeFrom(ObjectContainer container) {
		throw new UnsupportedOperationException();
	}
}

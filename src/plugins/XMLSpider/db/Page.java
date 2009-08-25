/**
 * @author j16sdiz (1024D/75494252)
 */
package plugins.XMLSpider.db;

import freenet.keys.FreenetURI;
import freenet.support.Logger;
import java.net.MalformedURLException;
import plugins.XMLSpider.org.garret.perst.FieldIndex;
import plugins.XMLSpider.org.garret.perst.IPersistentMap;
import plugins.XMLSpider.org.garret.perst.Persistent;
import plugins.XMLSpider.org.garret.perst.Storage;

public class Page extends Persistent implements Comparable<Page> {
	/** Page Id */
	protected long id;
	/** URI of the page */
	protected String uri;
	/** Zeroed edition of USK uris */
	protected String uskuri;
	/** usk edition of uri if usk, negative elsewise */
	protected long edition;
	/** Title */
	protected String pageTitle;
	/** Status */
	protected Status status;
	/** Number of retries at fetching this page */
	private int retries;
	
	/** Number of terms in this page */
	private int termCount;
	/** Arbitrary meta */
	protected String[] meta;

	/** Last Change Time */
	protected long lastChange;
	/** Comment, for debugging */
	protected String comment;
	
	


	public Page() {
	}

	Page(FreenetURI uri, String comment, Storage storage) {
		this.uri = uri.toString();
		if(uri.isSSKForUSK() || uri.isUSK()){
			this.uskuri = uri.setSuggestedEdition(0).toString();
			this.edition = uri.getEdition();
		}else{
			this.uskuri = null;
			this.edition = -1;
		}
		this.comment = comment;
		this.status = Status.QUEUED;
		this.retries = 0;
		this.lastChange = System.currentTimeMillis();

		this.termCount = 0;
		meta = null;
		storage.makePersistent(this);
	}

	public long getPageCount() {
		return termCount;
	}

	public void incrementRetries() {
		retries++;
	}

	/**
	 * Only for importer
	 * @param lastChange
	 * @deprecated TODO remove
	 */
	public void setLastChange(long lastChange) {
		this.lastChange = lastChange;
	}

	/**
	 * Only for importer, will be removed
	 * @param retries
	 * @deprecated TODO remove
	 */
	public void setRetries(int retries) {
		this.retries = retries;
	}
	
	public synchronized void setStatus(Status status) {
		preModify();
		this.status = status;
		postModify();
	}

	public Status getStatus() {
		return status;
	}

	public synchronized void setComment(String comment) {
		this.comment = comment;
	}
	
	public String getComment() {
		return comment;
	}

	public String getURI() {
		return uri;
	}

	public long getEdition() {
		return edition;
	}
	
	public long getId() {
		return id;
	}
	
	public void setPageTitle(String pageTitle) {
		this.pageTitle = pageTitle;
	}

	public String getPageTitle() {
		return pageTitle;
	}
	
	public void setMeta(String... meta) {
		this.meta = meta;
	}

	public String[] getMeta(){
		return meta;
	}
	
	@Override
	public int hashCode() {
		return (int) (id ^ (id >>> 32));
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;

		return id == ((Page) obj).id;
	}

	/**
	 * Sets the termCount, only for use in importer
	 * @param termCount number of words in this page
	 * @deprecated TODO remove
	 */
	public void setTermCount(int termCount) {
		this.termCount = termCount;
	}

	public void termCountIncrement() {
		termCount++;
	}

	@Override
	public String toString() {
		return "[PAGE: id=" + id + ", title=" + pageTitle + ", uri=" + getURI() + ( edition>=0 ? (", edition=" + edition) : "" ) + ", status=" + status + ", comment="
		+ comment
		+ "]";
	}

	public int compareTo(Page o) {
		return new Long(id).compareTo(o.id);
	}

	String getUSK() {
		return uskuri;
	}
	
	private void preModify() {
		Storage storage = getStorage();

		if (storage != null) {
			PerstRoot root = (PerstRoot) storage.getRoot();
			FieldIndex<Page> coll = root.getPageIndex(status);
			coll.exclusiveLock();
			try {
				coll.remove(this);
			} finally {
				coll.unlock();
			}
		}
	}

	private void postModify() {
		lastChange = System.currentTimeMillis();
		
		modify();

		Storage storage = getStorage();

		if (storage != null) {
			PerstRoot root = (PerstRoot) storage.getRoot();
			FieldIndex<Page> coll = root.getPageIndex(status);
			coll.exclusiveLock();
			try {
				coll.put(this);
			} finally {
				coll.unlock();
			}
		}
	}



	/** Only for importing
	 */
	private IPersistentMap<String, TermPosition> termPosMap;
	private long filesize;
	private String mimetype;



	/**
	 * Creates a clone of this Page in another db and converted into the current Page format from the v38 format
	 * @param storage db to put the result in
	 * @param allowOld whether to copy old Pages too, if false, old USK editions will be ignored where possible
	 * @return
	 * @throws java.net.MalformedURLException
	 */
	public Page v38ImportPage(Storage storage, boolean allowOld) throws MalformedURLException {
		Page p = ((PerstRoot)storage.getRoot()).getPageByURI(new FreenetURI(uri), true, comment, status, allowOld);
		if (p==null){
			Logger.minor(this, "Not importing from "+toString());
			return null;
		}

		p.pageTitle = pageTitle;
		p.status = status;
		p.postModify();
		p.lastChange = lastChange;

		if(filesize!=0 && mimetype!=null)
			p.setMeta(
				"size=" + Long.toString(filesize),
				"mime=" + mimetype
			);
		else if(mimetype!=null)
			p.setMeta("mime=" + mimetype);
		else if(filesize!=0)
			p.setMeta("size="+Long.toString(filesize));

		p.termCount = 0;
		if(termPosMap != null)
			for (TermPosition termPosition : termPosMap.values()) {
				p.termCount += termPosition.getPositions().length;
			}
		return p;
	}

	public TermPosition v38GetTermPosition(String md5) {
		return termPosMap.get(md5);
	}

}

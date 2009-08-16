/**
 * @author j16sdiz (1024D/75494252)
 */
package plugins.XMLSpider.db;

import freenet.keys.FreenetURI;
import plugins.XMLSpider.org.garret.perst.FieldIndex;
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
	
	public synchronized void setStatus(Status status) {
		preModify();
		this.status = status;
		postModify();
	}

	public Status getStatus() {
		return status;
	}

	public synchronized void setComment(String comment) {
		preModify();
		this.comment = comment;
		postModify();
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
		preModify();
		this.pageTitle = pageTitle;
		postModify();
	}

	public String getPageTitle() {
		return pageTitle;
	}
	
	public void setMeta(String[] meta) {
		preModify();
		this.meta = meta;
		postModify();
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
}

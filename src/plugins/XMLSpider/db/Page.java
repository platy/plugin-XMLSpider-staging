/**
 * @author j16sdiz (1024D/75494252)
 */
package plugins.XMLSpider.db;

import java.util.Map.Entry;
import java.util.Set;
import plugins.XMLSpider.org.garret.perst.FieldIndex;
import plugins.XMLSpider.org.garret.perst.IPersistentMap;
import plugins.XMLSpider.org.garret.perst.Persistent;
import plugins.XMLSpider.org.garret.perst.PersistentString;
import plugins.XMLSpider.org.garret.perst.Storage;

public class Page extends Persistent implements Comparable<Page> {
	/** Page Id */
	protected long id;
	/** URI of the page */
	protected String uri;
	/** Title */
	protected String pageTitle;
	/** Status */
	protected Status status;
	
	/** Number of terms in this page */
	private int termCount;
	/** Arbitrary meta */
	protected IPersistentMap<String, PersistentString> meta;

	/** Last Change Time */
	protected long lastChange;
	/** Comment, for debugging */
	protected String comment;

	public Page() {
	}

	Page(String uri, String comment, Storage storage) {
		this.uri = uri;
		this.comment = comment;
		this.status = Status.QUEUED;
		this.lastChange = System.currentTimeMillis();

		this.termCount = 0;
		meta = storage.<String, PersistentString> createMap(String.class);
		storage.makePersistent(this);
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
	
	public void addMeta(String name, String value) {
		preModify();
		meta.put(name, new PersistentString(value));
		postModify();
	}

	public Set<Entry<String, PersistentString>> getMeta(){
		return meta.entrySet();
	}
	
	public String getMeta(String name) {
		return meta.get(name).toString();
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
		return "[PAGE: id=" + id + ", title=" + pageTitle + ", uri=" + uri + ", status=" + status + ", comment="
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

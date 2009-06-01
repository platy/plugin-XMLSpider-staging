/**
 * Main page
 * @author j16sdiz (1024D/75494252)
 */
package plugins.XMLSpider.web;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import plugins.XMLSpider.XMLSpider;
import plugins.XMLSpider.db.Config;
import plugins.XMLSpider.db.Page;
import plugins.XMLSpider.db.PerstRoot;
import plugins.XMLSpider.db.Status;
import freenet.clients.http.InfoboxNode;
import freenet.clients.http.PageMaker;
import freenet.keys.FreenetURI;
import freenet.pluginmanager.PluginRespirator;
import freenet.support.HTMLNode;
import freenet.support.Logger;
import freenet.support.api.HTTPRequest;

class IndexWriterPage implements WebPage {
	static class PageStatus {
		long count;
		List<Page> pages;

		PageStatus(long count, List<Page> pages) {
			this.count = count;
			this.pages = pages;
		}
	}

	private final XMLSpider xmlSpider;
	private final PageMaker pageMaker;
	private final PluginRespirator pr;

	IndexWriterPage(XMLSpider xmlSpider) {
		this.xmlSpider = xmlSpider;
		pageMaker = xmlSpider.getPageMaker();
		pr = xmlSpider.getPluginRespirator();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see plugins.XMLSpider.WebPage#processPostRequest(freenet.support.api.HTTPRequest,
	 * freenet.support.HTMLNode)
	 */
	public void processPostRequest(HTTPRequest request, HTMLNode contentNode) {
		// Create Index
		if (request.isPartSet("createIndex")) {
			synchronized (this) {
				String indexdir = request.getPartAsString("indexdir", 512);
				String separatepageindex = request.getPartAsString("separatepageindex", 10);
				String indexformat = request.getPartAsString("indexformat", 10);
								
				xmlSpider.scheduleMakeIndex(indexdir, separatepageindex, indexformat);

				pageMaker.getInfobox("infobox infobox-success", "Scheduled Creating Index", contentNode).
					addChild("#", "Index will start create soon.");
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see plugins.XMLSpider.WebPage#writeContent(freenet.support.api.HTTPRequest,
	 * freenet.support.HTMLNode)
	 */
	public void writeContent(HTTPRequest request, HTMLNode contentNode) {
		HTMLNode overviewTable = contentNode.addChild("table", "class", "column");
		HTMLNode overviewTableRow = overviewTable.addChild("tr");

		PageStatus succeededStatus = getPageStatus(Status.SUCCEEDED);

		List<Page> runningFetch = xmlSpider.getRunningFetch();
		Config config = xmlSpider.getConfig();

		// Column 1
		HTMLNode nextTableCell = overviewTableRow.addChild("td", "class", "first");
		HTMLNode statusContent = pageMaker.getInfobox("#", "Spider Status", nextTableCell);
		statusContent.addChild("#", "Running Request: " + runningFetch.size() + "/"
		        + config.getMaxParallelRequests());
		statusContent.addChild("br");
		statusContent.addChild("#", "Pages to be written: " + succeededStatus.count);
		statusContent.addChild("br");
		statusContent.addChild("#", "Words to be written: " );
		statusContent.addChild("br");
		statusContent.addChild("br");
		statusContent.addChild("#", "Queued Event: " + xmlSpider.callbackExecutor.getQueue().size());
		statusContent.addChild("br");
		statusContent.addChild("#", "Index Writer: ");
		synchronized (this) {
			if (xmlSpider.isWritingIndex())
				statusContent.addChild("span", "style", "color: red; font-weight: bold;", "RUNNING");
			else if (xmlSpider.isWriteIndexScheduled())
				statusContent.addChild("span", "style", "color: blue; font-weight: bold;", "SCHEDULED");
			else
				statusContent.addChild("span", "style", "color: green; font-weight: bold;", "IDLE");
		}
		statusContent.addChild("br");
		statusContent.addChild("#", "Last Written: "
		        + (xmlSpider.getIndexWriter().tProducedIndex == 0 ? "NEVER" : new Date(
		                xmlSpider.getIndexWriter().tProducedIndex).toString()));


		


		InfoboxNode indexwrite = pageMaker.getInfobox("Write an index");
		HTMLNode indexWriteBox = indexwrite.outer;
		indexWriteBox.addAttribute("style", "right: 0;");
		HTMLNode indexwriteContent = indexwrite.content;
		
		HTMLNode indexForm = pr.addFormChild(indexwriteContent, "/xmlspider/indexwriter", "indexForm");
		indexForm.addChild("#", "Write index to: " );
		indexForm.addChild("input",
				new String[] { "name", "type", "value" },//
		        new String[] { "indexdir", "text", xmlSpider.getConfig().getIndexDir() });
		indexForm.addChild("br");
		indexForm.addChild("#", "Put page index in separate file: " );
		indexForm.addChild("input",
				new String[] { "name", "type", "value" },//
		        new String[] { "separatepageindex", "checkbox", "true" });
		indexForm.addChild("br");
		indexForm.addChild("#", "include hmm: " );
		indexForm.addChild("input",
				new String[] { "name", "type", "value" },//
		        new String[] { "includelist", "text", "" });
		indexForm.addChild("br");
		indexForm.addChild("#", "Exclude hmm: " );
		indexForm.addChild("input",
				new String[] { "name", "type", "value" },//
		        new String[] { "excludelist", "text", "" });
		indexForm.addChild("br");
		indexForm.addChild("#", "XML Format " );
		indexForm.addChild("input",
				new String[] { "name", "type", "value", "checked" },//
		        new String[] { "indexformat", "radio", "xml", "checked" });
		indexForm.addChild("#", ", JSON Format " );
		indexForm.addChild("input",
				new String[] { "name", "type", "value" },//
		        new String[] { "indexformat", "radio", "json" });
		indexForm.addChild("input", //
		        new String[] { "name", "type", "value" },//
		        new String[] { "createIndex", "hidden", "createIndex" });
		indexForm.addChild("br");
		indexForm.addChild("input", //
		        new String[] { "type", "value" }, //
		        new String[] { "submit", "Create Index Now" });
		contentNode.addChild(indexWriteBox);
	}

	//-- Utilities
	private PageStatus getPageStatus(Status status) {
		PerstRoot root = xmlSpider.getRoot();
		synchronized (root) {
			int count = root.getPageCount(status);
			Iterator<Page> it = root.getPages(status);

			int showURI = xmlSpider.getConfig().getMaxShownURIs();
			List<Page> page = new ArrayList();
			while (page.size() < showURI && it.hasNext())
				page.add(it.next());

			return new PageStatus(count, page);
		}
	}
}

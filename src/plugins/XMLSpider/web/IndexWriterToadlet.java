package plugins.XMLSpider.web;

import java.io.IOException;
import java.net.URI;

import plugins.XMLSpider.XMLSpider;

import freenet.client.HighLevelSimpleClient;
import freenet.clients.http.PageNode;
import freenet.clients.http.RedirectException;
import freenet.clients.http.Toadlet;
import freenet.clients.http.ToadletContext;
import freenet.clients.http.ToadletContextClosedException;
import freenet.node.NodeClientCore;
import freenet.support.HTMLNode;
import freenet.support.MultiValueTable;
import freenet.support.api.HTTPRequest;

public class IndexWriterToadlet extends Toadlet {

	final XMLSpider spider;
	private final NodeClientCore core;
	
	protected IndexWriterToadlet(HighLevelSimpleClient client, XMLSpider spider, NodeClientCore core) {
		super(client);
		this.spider = spider;
		this.core = core;
	}

	@Override
	public String path() {
		return "/xmlspider/indexwriter";
	}

	@Override
	public String supportedMethods() {
		return "GET, POST";
	}

	@Override
	public void handleGet(URI uri, final HTTPRequest request, final ToadletContext ctx) 
	throws ToadletContextClosedException, IOException, RedirectException {
		ClassLoader origClassLoader = Thread.currentThread().getContextClassLoader();
		Thread.currentThread().setContextClassLoader(XMLSpider.class.getClassLoader());
		try {
			IndexWriterPage page = new IndexWriterPage(spider);
			PageNode p = ctx.getPageMaker().getPageNode(XMLSpider.pluginName, ctx);
			HTMLNode pageNode = p.outer;
			HTMLNode contentNode = p.content;
			page.writeContent(request, contentNode);
			writeHTMLReply(ctx, 200, "OK", null, pageNode.generate());
		} finally {
			Thread.currentThread().setContextClassLoader(origClassLoader);
		}
	}
	
	@Override
	public void handlePost(URI uri, HTTPRequest request, final ToadletContext ctx) throws ToadletContextClosedException, IOException, RedirectException {
		ClassLoader origClassLoader = Thread.currentThread().getContextClassLoader();
		Thread.currentThread().setContextClassLoader(XMLSpider.class.getClassLoader());

		String formPassword = request.getPartAsString("formPassword", 32);
		if((formPassword == null) || !formPassword.equals(core.formPassword)) {
			MultiValueTable<String,String> headers = new MultiValueTable<String,String>();
			headers.put("Location", path());
			ctx.sendReplyHeaders(302, "Found", headers, null, 0);
			return;
		}

		try {
			PageNode p = ctx.getPageMaker().getPageNode(XMLSpider.pluginName, ctx);
			HTMLNode pageNode = p.outer;
			HTMLNode contentNode = p.content;
	
			WebPage page = new IndexWriterPage(spider);
	
			page.processPostRequest(request, contentNode);
			page.writeContent(request, contentNode);
	
			writeHTMLReply(ctx, 200, "OK", null, pageNode.generate());
		} finally {
			Thread.currentThread().setContextClassLoader(origClassLoader);
		}
	}
}

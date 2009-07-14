
package plugins.XMLSpider;

import freenet.pluginmanager.FredPluginTalker;
import freenet.pluginmanager.PluginNotFoundException;
import freenet.pluginmanager.PluginRespirator;
import freenet.pluginmanager.PluginTalker;
import freenet.support.SimpleFieldSet;
import freenet.support.api.Bucket;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import plugins.Library.fcp.FCPExposedMethodsInterface;

/**
 * This is the remote FCP interface for Library
 *
 * @author MikeB
 */
public class RemoteLibrary implements FCPExposedMethodsInterface, FredPluginTalker {
	PluginRespirator pr;
	private final HashMap<String, String> returnValues;

	public RemoteLibrary(PluginRespirator pr){
		this.pr = pr;
		returnValues = new HashMap();
	}

	private String invokeRemoteMethod(String method, Object... params) throws PluginNotFoundException{
		String identifier = method+":"+Thread.currentThread().getName();
		PluginTalker pt = pr.getPluginTalker(this, "Library", identifier);
		SimpleFieldSet plugparams = new SimpleFieldSet(true);
		plugparams.putOverwrite("method", method);
		for (Object object : params) {
			plugparams.putOverwrite(object.getClass().getSimpleName(), object.toString());
		}
		returnValues.put(identifier, identifier);
		pt.send(plugparams, null);
		try {
			// wait some amount of time for a reply, if none throw timeout exception or something, if theres a response return that
			identifier.wait(30000);
		} catch (InterruptedException ex) {
			Logger.getLogger(RemoteLibrary.class.getName()).log(Level.SEVERE, null, ex);
		}
		return returnValues.get(identifier);
	}

	public void onReply(String pluginname, String identifier, SimpleFieldSet params, Bucket data) {
		if(params.get("return") != null)
			returnValues.put(identifier, params.get("return")).notifyAll();
		else if(params.get("exception") != null)
			returnValues.put(identifier, "Exception");	// Something is wrong
		// Something is wrong
	}
	

	public Integer getVersion() throws PluginNotFoundException {
		return Integer.valueOf(invokeRemoteMethod("getVersion"));
	}

	public Integer findTerm(String indexid, String term) throws Exception, PluginNotFoundException {
		return Integer.valueOf(invokeRemoteMethod("findTerm", indexid, term));
	}

	public Integer addPage(String uri, String title, Map<String, String> meta) throws PluginNotFoundException{
		return Integer.valueOf(invokeRemoteMethod("addPage", uri, title, meta));
	}
}

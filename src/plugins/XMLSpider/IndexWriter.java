/* This code is part of Freenet. It is distributed under the GNU General
 * Public License, version 2 (or at your option any later version). See
 * http://www.gnu.org/ for further details of the GPL. */
package plugins.XMLSpider;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;
import java.util.Iterator;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Text;

import plugins.XMLSpider.db.Config;
import plugins.XMLSpider.db.Page;
import plugins.XMLSpider.db.PerstRoot;
import plugins.XMLSpider.db.Status;
import plugins.XMLSpider.db.Term;
import plugins.XMLSpider.db.TermPosition;
import plugins.XMLSpider.org.garret.perst.IterableIterator;
import plugins.XMLSpider.org.garret.perst.Storage;
import plugins.XMLSpider.org.garret.perst.StorageFactory;
import freenet.support.Logger;
import freenet.support.io.Closer;

/**
 * Write index to disk file
 */
public class IndexWriter {
	private static final String[] HEX = { "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e",
	        "f" }; 
	
	//- Writing Index
	public long tProducedIndex;
	private Vector<String> indices;
	private int match;
	private long time_taken;
	private boolean logMINOR = Logger.shouldLog(Logger.MINOR, this);

	IndexWriter() {
	}

	public synchronized void makeIndex(PerstRoot perstRoot, String indexdir, boolean separatepageindex) throws Exception {
		logMINOR = Logger.shouldLog(Logger.MINOR, this);
		try {
			time_taken = System.currentTimeMillis();

			Config config = perstRoot.getConfig();

			if(indexdir.equals("") || indexdir == null)
				indexdir = config.getIndexDir();
			else
				config.setIndexDir(indexdir);
			File indexDir = new File(indexdir);
			if(((!indexDir.exists()) && !indexDir.mkdirs()) || (indexDir.exists() && !indexDir.isDirectory())) {
				Logger.error(this, "Cannot create index directory: " + indexDir);
				return;
			}

			if (logMINOR)
				Logger.minor(this, "Spider: regenerating index. MAX_SIZE=" + config.getIndexSubindexMaxSize() +
					", MAX_ENTRIES=" + config.getIndexMaxEntries());

			if(separatepageindex)
				makePageIndex(perstRoot);
			makeSubIndices(perstRoot, separatepageindex);
			makeMainIndex(config, separatepageindex);

			time_taken = System.currentTimeMillis() - time_taken;

			if (logMINOR)
				Logger.minor(this, "Spider: indexes regenerated - tProducedIndex="
				        + (System.currentTimeMillis() - tProducedIndex) + "ms ago time taken=" + time_taken + "ms");

			tProducedIndex = System.currentTimeMillis();
		} finally {
		}
	}

	/**
	 * generates the main index file that can be used by librarian for searching in the list of
	 * subindices
	 * 
	 * @param void
	 * @author swati
	 * @throws IOException
	 * @throws NoSuchAlgorithmException
	 */
	private void makeMainIndex(Config config, boolean separatepageindex ) throws IOException, NoSuchAlgorithmException {
		// Produce the main index file.
		if (logMINOR)
			Logger.minor(this, "Producing top index...");

		//the main index file 
		File outputFile = new File(config.getIndexDir() + "index.xml");
		// Use a stream so we can explicitly close - minimise number of filehandles used.
		BufferedOutputStream fos = new BufferedOutputStream(new FileOutputStream(outputFile));
		StreamResult resultStream;
		resultStream = new StreamResult(fos);

		try {
			/* Initialize xml builder */
			Document xmlDoc = null;
			DocumentBuilderFactory xmlFactory = null;
			DocumentBuilder xmlBuilder = null;
			DOMImplementation impl = null;
			Element rootElement = null;

			xmlFactory = DocumentBuilderFactory.newInstance();

			try {
				xmlBuilder = xmlFactory.newDocumentBuilder();
			} catch (javax.xml.parsers.ParserConfigurationException e) {

				Logger.error(this, "Spider: Error while initializing XML generator: " + e.toString(), e);
				return;
			}

			impl = xmlBuilder.getDOMImplementation();
			/* Starting to generate index */
			xmlDoc = impl.createDocument(null, "main_index", null);
			rootElement = xmlDoc.getDocumentElement();

			/* Adding header to the index */
			Element headerElement = xmlDoc.createElementNS(null, "header");

			/* -> title */
			Element subHeaderElement = xmlDoc.createElementNS(null, "title");
			Text subHeaderText = xmlDoc.createTextNode(config.getIndexTitle());

			subHeaderElement.appendChild(subHeaderText);
			headerElement.appendChild(subHeaderElement);

			/* -> owner */
			subHeaderElement = xmlDoc.createElementNS(null, "owner");
			subHeaderText = xmlDoc.createTextNode(config.getIndexOwner());

			subHeaderElement.appendChild(subHeaderText);
			headerElement.appendChild(subHeaderElement);

			/* -> owner email */
			if (config.getIndexOwnerEmail() != null) {
				subHeaderElement = xmlDoc.createElementNS(null, "email");
				subHeaderText = xmlDoc.createTextNode(config.getIndexOwnerEmail());

				subHeaderElement.appendChild(subHeaderText);
				headerElement.appendChild(subHeaderElement);
			}
			/*
			 * the max number of digits in md5 to be used for matching with the search query is
			 * stored in the xml
			 */
			Element prefixElement = xmlDoc.createElementNS(null, "prefix");
			prefixElement.setAttributeNS(null, "value", match + "");
			
			/* Adding word index */
			Element keywordsElement = xmlDoc.createElementNS(null, "keywords");
			for (int i = 0; i < indices.size(); i++) {

				Element subIndexElement = xmlDoc.createElementNS(null, "subIndex");
				subIndexElement.setAttributeNS(null, "key", indices.elementAt(i));
				//the subindex element key will contain the bits used for matching in that subindex
				keywordsElement.appendChild(subIndexElement);
			}
			
			/* Specify whether index has separated page index */
			Element pagesElement = xmlDoc.createElementNS(null, "pages");
			pagesElement.setAttributeNS(null, "separateindex", separatepageindex ? "true" : "false");


			rootElement.appendChild(prefixElement);
			rootElement.appendChild(headerElement);
			rootElement.appendChild(pagesElement);
			rootElement.appendChild(keywordsElement);

			/* Serialization */
			DOMSource domSource = new DOMSource(xmlDoc);
			TransformerFactory transformFactory = TransformerFactory.newInstance();
			Transformer serializer;

			try {
				serializer = transformFactory.newTransformer();
			} catch (javax.xml.transform.TransformerConfigurationException e) {
				Logger.error(this, "Spider: Error while serializing XML (transformFactory.newTransformer()): "
				        + e.toString(), e);
				return;
			}

			serializer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
			serializer.setOutputProperty(OutputKeys.INDENT, "yes");

			/* final step */
			try {
				serializer.transform(domSource, resultStream);
			} catch (javax.xml.transform.TransformerException e) {
				Logger.error(this, "Spider: Error while serializing XML (transform()): " + e.toString(), e);
				return;
			}
		} finally {
			fos.close();
		}

		//The main xml file is generated 
		//As each word is generated enter it into the respective subindex
		//The parsing will start and nodes will be added as needed 

	}
	
	private void makePageIndex(PerstRoot perstRoot) throws Exception{
		final Config config = perstRoot.getConfig();
		final long MAX_SIZE = config.getIndexSubindexMaxSize();
		final int MAX_ENTRIES = config.getIndexMaxEntries();
		
		File outputFile = new File(config.getIndexDir() + "fileindex.xml");
		BufferedOutputStream fos = null;

		int count = 0;
		int estimateSize = 0;
		try {
			DocumentBuilderFactory xmlFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder xmlBuilder;

			try {
				xmlBuilder = xmlFactory.newDocumentBuilder();
			} catch (javax.xml.parsers.ParserConfigurationException e) {
				throw new RuntimeException("Spider: Error while initializing XML generator", e);
			}

			DOMImplementation impl = xmlBuilder.getDOMImplementation();
			/* Starting to generate index */
			Document xmlDoc = impl.createDocument("", "file_index", null);
			
			Element rootElement = xmlDoc.getDocumentElement();
			if (config.isDebug()) {
				rootElement.setAttributeNS("http://www.w3.org/2000/xmlns/", "xmlns:debug", "urn:freenet:xmlspider:debug");
				rootElement.appendChild(xmlDoc.createComment(new Date().toGMTString()));
			}
			
			/* Adding header to the index */
			Element headerElement = xmlDoc.createElementNS(null, "header");
			/* -> title */
			Element subHeaderElement = xmlDoc.createElementNS(null, "title");
			Text subHeaderText = xmlDoc.createTextNode(config.getIndexTitle());
			subHeaderElement.appendChild(subHeaderText);
			headerElement.appendChild(subHeaderElement);

			/* List of files referenced in this index */
			Element filesElement = xmlDoc.createElementNS(null, "files"); /*
																		 * filesElement !=
																		 * fileElement
																		 */
			
			Iterator<Page> pageIterator = perstRoot.getPages(Status.SUCCEEDED);
			while (pageIterator.hasNext()) {
				Page page = pageIterator.next();
				synchronized (page) {
					Element fileElement = xmlDoc.createElementNS(null, "file");
					fileElement.setAttributeNS(null, "id", Long.toString(page.getId()));
					fileElement.setAttributeNS(null, "key", page.getURI());
					fileElement.setAttributeNS(null, "title", page.getPageTitle() != null ? page
							.getPageTitle() : page.getURI());
					
					filesElement.appendChild(fileElement);
					
					count++;
					
					estimateSize += 15;
					estimateSize += filesElement.getAttributeNS(null, "id").length();
					estimateSize += filesElement.getAttributeNS(null, "key").length();
					estimateSize += filesElement.getAttributeNS(null, "title").length();
				}
			}
			
			Element entriesElement = xmlDoc.createElementNS(null, "entries");
			entriesElement.setAttributeNS(null, "value", count + "");

			rootElement.appendChild(entriesElement);
			rootElement.appendChild(headerElement);
			rootElement.appendChild(filesElement);

			/* Serialization */
			DOMSource domSource = new DOMSource(xmlDoc);
			TransformerFactory transformFactory = TransformerFactory.newInstance();
			Transformer serializer;
			
			try {
				serializer = transformFactory.newTransformer();
			} catch (javax.xml.transform.TransformerConfigurationException e) {
				throw new RuntimeException("Spider: Error while serializing XML (transformFactory.newTransformer())", e);
			}
			serializer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
			serializer.setOutputProperty(OutputKeys.INDENT, "yes");

			fos = new BufferedOutputStream(new FileOutputStream(outputFile));
			StreamResult resultStream = new StreamResult(fos);
			
			/* final step */
			try {
				serializer.transform(domSource, resultStream);
			} catch (javax.xml.transform.TransformerException e) {
				throw new RuntimeException("Spider: Error while serializing XML (transform())", e);
			}
		} finally {
			Closer.close(fos);
		}
		

		if (logMINOR)
			Logger.minor(this, "Spider: indexes regenerated.");
	}

	/**
	 * Generates the subindices. Each index has less than {@code MAX_ENTRIES} words. The original
	 * treemap is split into several sublists indexed by the common substring of the hash code of
	 * the words
	 * 
	 * @throws Exception
	 */
	private void makeSubIndices(PerstRoot perstRoot, boolean separatepageindex) throws Exception {
		Logger.normal(this, "Generating index...");

		indices = new Vector<String>();
		match = 1;

		for (String hex : HEX)
			generateSubIndex(perstRoot, hex, separatepageindex);
	}

	private void generateSubIndex(PerstRoot perstRoot, String prefix, boolean separatepageindex) throws Exception {
		if (logMINOR)
			Logger.minor(this, "Generating subindex for (" + prefix + ")");
		if (prefix.length() > match)
			match = prefix.length();

		if (generateXML(perstRoot, prefix, separatepageindex ))
			return;
		
		if (logMINOR)
			Logger.minor(this, "Too big subindex for (" + prefix + ")");
		
		for (String hex : HEX)
			generateSubIndex(perstRoot, prefix + hex, separatepageindex );
	}

	/**
	 * generates the xml index with the given list of words with prefix number of matching bits in
	 * md5
	 * 
	 * @param prefix
	 *            prefix string
	 * @return successful
	 * @throws IOException
	 */
	private boolean generateXML(PerstRoot perstRoot, String prefix, boolean separatepageindex ) throws IOException {
		final Config config = perstRoot.getConfig();
		final long MAX_SIZE = config.getIndexSubindexMaxSize();
		final int MAX_ENTRIES = config.getIndexMaxEntries();
		
		File outputFile = new File(config.getIndexDir() + "index_" + prefix + ".xml");
		BufferedOutputStream fos = null;

		int count = 0;
		int estimateSize = 0;
		try {
			DocumentBuilderFactory xmlFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder xmlBuilder;

			try {
				xmlBuilder = xmlFactory.newDocumentBuilder();
			} catch (javax.xml.parsers.ParserConfigurationException e) {
				throw new RuntimeException("Spider: Error while initializing XML generator", e);
			}

			DOMImplementation impl = xmlBuilder.getDOMImplementation();
			/* Starting to generate index */
			Document xmlDoc = impl.createDocument("", "sub_index", null);
			
			Element rootElement = xmlDoc.getDocumentElement();
			if (config.isDebug()) {
				rootElement.setAttributeNS("http://www.w3.org/2000/xmlns/", "xmlns:debug", "urn:freenet:xmlspider:debug");
				rootElement.appendChild(xmlDoc.createComment(new Date().toGMTString()));
			}
			
			/* Adding header to the index */
			Element headerElement = xmlDoc.createElementNS(null, "header");
			/* -> title */
			Element subHeaderElement = xmlDoc.createElementNS(null, "title");
			Text subHeaderText = xmlDoc.createTextNode(config.getIndexTitle());
			subHeaderElement.appendChild(subHeaderText);
			headerElement.appendChild(subHeaderElement);

			/* List of files referenced in this subindex */
			Element filesElement = xmlDoc.createElementNS(null, "files"); /*
																		 * filesElement !=
																		 * fileElement
																		 */
			Set<Long> fileid = new HashSet<Long>();
			
			/* Adding word index */
			Element keywordsElement = xmlDoc.createElementNS(null, "keywords");
			IterableIterator<Term> termIterator = perstRoot.getTermIterator(prefix, prefix + "g");
			for (Term term : termIterator) {
				Element wordElement = xmlDoc.createElementNS(null, "word");
				wordElement.setAttributeNS(null, "v", term.getWord());
				if (config.isDebug()) {
					wordElement.setAttributeNS("urn:freenet:xmlspider:debug", "debug:md5", term.getMD5());
				}
				count++;
				estimateSize += 12;
				estimateSize += term.getWord().length();

				Set<Page> pages = term.getPages();
				
				if ((count > 1 && (estimateSize + pages.size() * 13) > MAX_SIZE) || //
						(count > MAX_ENTRIES))
					return false;

				for (Page page : pages) {
					TermPosition termPos = page.getTermPosition(term, false);
					if (termPos == null)
						continue;
					
					synchronized (termPos) {
						synchronized (page) {
							/*
							 * adding file information uriElement - lists the id of the file
							 * containing a particular word fileElement - lists the id,key,title of
							 * the files mentioned in the entire subindex
							 */
							Element uriElement = xmlDoc.createElementNS(null, "file");
							uriElement.setAttributeNS(null, "id", Long.toString(page.getId()));

							/* Position by position */
							int[] positions = termPos.positions;

							StringBuilder positionList = new StringBuilder();

							for (int k = 0; k < positions.length; k++) {
								if (k != 0)
									positionList.append(',');
								positionList.append(positions[k]);
							}
							uriElement.appendChild(xmlDoc.createTextNode(positionList.toString()));
							wordElement.appendChild(uriElement);
							
							estimateSize += 13;
							estimateSize += positionList.length();
						
							if (!separatepageindex && !fileid.contains(page.getId())) { // Add pages to index
								fileid.add(page.getId());

								Element fileElement = xmlDoc.createElementNS(null, "file");
								fileElement.setAttributeNS(null, "id", Long.toString(page.getId()));
								fileElement.setAttributeNS(null, "key", page.getURI());
								fileElement.setAttributeNS(null, "title", page.getPageTitle() != null ? page
								        .getPageTitle() : page.getURI());
								
								filesElement.appendChild(fileElement);
								
								estimateSize += 15;
								estimateSize += filesElement.getAttributeNS(null, "id").length();
								estimateSize += filesElement.getAttributeNS(null, "key").length();
								estimateSize += filesElement.getAttributeNS(null, "title").length();
							}
						}
					}
				}
				keywordsElement.appendChild(wordElement);
			}
			
			Element entriesElement = xmlDoc.createElementNS(null, "entries");
			entriesElement.setAttributeNS(null, "value", count + "");

			rootElement.appendChild(entriesElement);
			rootElement.appendChild(headerElement);
			if(!separatepageindex)
				rootElement.appendChild(filesElement);
			rootElement.appendChild(keywordsElement);

			/* Serialization */
			DOMSource domSource = new DOMSource(xmlDoc);
			TransformerFactory transformFactory = TransformerFactory.newInstance();
			Transformer serializer;
			
			try {
				serializer = transformFactory.newTransformer();
			} catch (javax.xml.transform.TransformerConfigurationException e) {
				throw new RuntimeException("Spider: Error while serializing XML (transformFactory.newTransformer())", e);
			}
			serializer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
			serializer.setOutputProperty(OutputKeys.INDENT, "yes");

			fos = new BufferedOutputStream(new FileOutputStream(outputFile));
			StreamResult resultStream = new StreamResult(fos);
			
			/* final step */
			try {
				serializer.transform(domSource, resultStream);
			} catch (javax.xml.transform.TransformerException e) {
				throw new RuntimeException("Spider: Error while serializing XML (transform())", e);
			}
		} finally {
			Closer.close(fos);
		}
		
		if (outputFile.length() > MAX_SIZE && count > 1) {
			outputFile.delete();
			return false;
		}

		if (logMINOR)
			Logger.minor(this, "Spider: indexes regenerated.");
		indices.add(prefix);
		return true;
	}

	public static void main(String[] arg) throws Exception {
		Storage db = StorageFactory.getInstance().createStorage();
		db.setProperty("perst.object.cache.kind", "pinned");
		db.setProperty("perst.object.cache.init.size", 8192);
		db.setProperty("perst.alternative.btree", true);
		db.setProperty("perst.string.encoding", "UTF-8");
		db.setProperty("perst.concurrent.iterator", true);
		db.setProperty("perst.file.readonly", true);

		db.open(arg[0]);
		PerstRoot root = (PerstRoot) db.getRoot();
		IndexWriter writer = new IndexWriter();
		
		int benchmark = 0;
		long[] timeTaken = null;
		if (arg[1] != null) {
			benchmark = Integer.parseInt(arg[1]);
			timeTaken = new long[benchmark];
		}
		
		for (int i = 0; i < benchmark; i++) {
			long startTime = System.currentTimeMillis();
			writer.makeIndex(root, "", true);
			long endTime = System.currentTimeMillis();
			long memFree = Runtime.getRuntime().freeMemory();
			long memTotal = Runtime.getRuntime().totalMemory();

			System.out.println("Index generated in " + (endTime - startTime) //
			        + "ms. Used memory=" + (memTotal - memFree));

			if (benchmark > 0) {
				timeTaken[i] = (endTime - startTime);

				System.out.println("Cooling down.");
				for (int j = 0; j < 3; j++) {
					System.gc();
					System.runFinalization();
					Thread.sleep(3000);
				}
			}
		}

		if (benchmark > 0) {
			long totalTime = 0;
			long totalSqTime = 0;
			for (long t : timeTaken) {
				totalTime += t;
				totalSqTime += t * t;
			}

			double meanTime = (totalTime / benchmark);
			double meanSqTime = (totalSqTime / benchmark);

			System.out.println("Mean time = " + (long) meanTime + "ms");
			System.out.println("       sd = " + (long) Math.sqrt(meanSqTime - meanTime * meanTime) + "ms");
		}
	}
}

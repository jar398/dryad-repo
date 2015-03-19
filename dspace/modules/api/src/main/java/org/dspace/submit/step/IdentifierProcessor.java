/* Separated out from SubmitPublicationStep */

package org.dspace.submit.step;

import java.util.List;
import java.util.Iterator;

import org.apache.log4j.Logger;

import org.dspace.core.Context;
import org.dspace.content.Item;
import org.dspace.content.crosswalk.IngestionCrosswalk;
import org.dspace.core.PluginManager;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.input.SAXBuilder;
import org.jdom.Content;
import org.jdom.Text;


public class IdentifierProcessor {

    public final static String crossRefApiRoot = "http://api.crossref.org/works/";
    public final static String crossRefApiFormat = "/transform/application/vnd.crossref.unixref+xml";
    
    private static Logger log = Logger.getLogger(IdentifierProcessor.class);

    /**
       Process a DOI entered by the submitter. Use the DOI metadata to initialize publication information.
     **/
    public static boolean processDOI(Context context, Item item, String identifier) {

	// normalize and validate the identifier
	identifier = identifier.toLowerCase().trim();
        if(identifier.startsWith("doi:")) {
            identifier = identifier.replaceFirst("doi:", "");
	} else if (identifier.startsWith("http://dx.doi.org")) {
            identifier = identifier.replaceFirst("http://dx.doi.org", "");
        }
	
        try{
            Element jElement = retrieveXML(crossRefApiRoot + identifier + crossRefApiFormat);
            if(jElement != null){

                List<Element> children = jElement.getChildren();
                if(children.size()==0){
                    return false;
                }

                if(!isAValidDOI(jElement)) return false;

                // Use the ingest process to parse the XML document, transformation is done
                // using XSLT
                IngestionCrosswalk xwalk = (IngestionCrosswalk) PluginManager.getNamedPlugin(IngestionCrosswalk.class, "DOI");

                xwalk.ingest(context, item, jElement);

		//add the PMID if NCBI can return one
		addPMID(context,item,identifier);  

                return true;
            }
        }catch (Exception ex){
            log.error("unable to process DOI metadata", ex);
            return false;
        }
        return false;

    }

    private static void addPMID(Context context, Item item, String identifier){

	log.warn("Trying to add a PMID for " + identifier);
	final String queryString = "http://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&retmod=xml&term=" + identifier + "%5Bdoi%5D"; 

        String pmid = null;
	try{
	    Element jElement = retrieveXML(queryString);
	    
	    if (jElement == null)
		log.warn("query for " + identifier + " failed");

	    if (jElement != null){
		List<Element> children = jElement.getChildren();
		for (Element child : children){
		    //if there is an ErrorList child, assume lookup failure - no need to log?
		    if ("ErrorList".equals(child.getName())){
			log.info("Didn't find PMID for " + identifier);
			return;  
		    }
		    if ("IdList".equals(child.getName())){
			List<Element> idList = child.getChildren();
			for (Element idElement : idList){
			    List <Content> cList = idElement.getContent();
			    Iterator<Content> it = cList.iterator();
			    while (pmid == null && it.hasNext()){
				Content c = it.next();
				if (c instanceof Text){
				    if (!"".equals(((Text)c).getTextNormalize())){
					pmid = ((Text)c).getTextNormalize();
				    }
				}
			    }
			    log.info("Content is " + pmid);
			}
		    }
		}
	    }

        }catch (Exception ex){
            log.error("Error while trying to retrieve PMID " + identifier, ex);
	    return;
        }

	if (pmid != null)
	    item.addMetadata("dc","relation", "isreferencedby", null, "PMID:" + pmid);
	
    }

    private static boolean isAValidDOI(Element element) {
        List<Element> children = element.getChildren();
        for(Element e : children){
            if(e.getName().equals("doi_record")){
                List<Element> doiRecordsChildren = e.getChildren();
                for(Element e1 : doiRecordsChildren){

                    if(e1.getName().equals("crossref")){
                        List<Element> crossRefChildren = e1.getChildren();
                        for(Element e2 : crossRefChildren){
                            if(e2.getName().equals("error")){
                                return false;
                            }
                            return true;
                        }
                    }
                }
            }
        }
        return true;
    }

    private static Element retrieveXML(String urls) throws Exception{
        SAXBuilder builder = new SAXBuilder();
        org.jdom.Document doc = builder.build(urls);
        return doc.getRootElement();
    }

}
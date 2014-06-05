package org.dspace.app.authority;

import org.dspace.content.authority.AuthoritySearchService;
import org.dspace.content.authority.AuthorityValue;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.PosixParser;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.dspace.content.AuthorityObject;
import org.dspace.content.Concept;
import org.dspace.content.Scheme;
import org.dspace.content.Term;
import org.dspace.core.ConfigurationManager;
import org.dspace.core.Context;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import org.apache.commons.cli.CommandLine;

import org.apache.commons.cli.Options;


import org.dspace.utils.DSpace;

/**
 * Utility to transfer properties file into database tables.
 *
 * @author Lantian Gai, Mark Diggory
 */
public final class DryadJournalConceptImporter {


    /** DSpace Context object */
    private Context context;
    // Reading DryadJournalSubmission.properties
    public static final String FULLNAME = "fullname";
    public static final String METADATADIR = "metadataDir";
    public static final String INTEGRATED = "integrated";
    public static final String PUBLICATION_BLACKOUT = "publicationBlackout";
    public static final String NOTIFY_ON_REVIEW = "notifyOnReview";
    public static final String NOTIFY_ON_ARCHIVE = "notifyOnArchive";
    public static final String JOURNAL_ID = "journalID";
    public static final String SUBSCRIPTION_PAID = "subscriptionPaid";

    /**
     * For invoking via the command line.  If called with no command line arguments,
     * it will negotiate with the user for the administrator details
     *
     * @param argv
     *            command-line arguments
     */
    public static void main(String[] argv)
            throws Exception
    {
        CommandLineParser parser = new PosixParser();
        Options options = new Options();

        DryadJournalConceptImporter ca = new DryadJournalConceptImporter();
        options.addOption("t", "test", true, "test mode");

        CommandLine line = parser.parse(options, argv);
        ca.importAuthority(line.getOptionValue("t"));
    }



    /**
     * constructor, which just creates and object with a ready context
     *
     * @throws Exception
     */
    private DryadJournalConceptImporter()
            throws Exception
    {
        context = new Context();
    }

    /**
     * Create the administrator with the given details.  If the user
     * already exists then they are simply upped to administrator status
     *
     * @throws Exception
     */
    private void importAuthority(String test)
            throws Exception
    {
        // Of course we aren't an administrator yet so we need to
        // circumvent authorisation
        context.setIgnoreAuthorization(true);

        try {
            SolrQuery queryArgs = new SolrQuery();
            queryArgs.setQuery("*:*");
            queryArgs.setRows(-1);
            QueryResponse searchResponse = getSearchService().search(queryArgs);
            SolrDocumentList authDocs = searchResponse.getResults();
            int max = (int) searchResponse.getResults().getNumFound();

            queryArgs.setQuery("*:*");
            if(test!=null)
            {
                queryArgs.setRows(Integer.parseInt(test));
            }
            else
            {
                queryArgs.setRows(max);
            }

            searchResponse = getSearchService().search(queryArgs);
            authDocs = searchResponse.getResults();
            Date date = new Date();


            Scheme authScheme = Scheme.findByIdentifier(context, "prism.publicationName");
            if(authScheme==null){
                authScheme = Scheme.create(context,"prism.publicationName");
                authScheme.addMetadata("dc","title",null,"en","Dryad Journal Authority",null,1);
                authScheme.setLastModified(date);
                authScheme.setCreated(date);
                authScheme.setLang("en");
                authScheme.setStatus("Published");
                authScheme.update();
            }

            context.commit();
            //Get all journal configurations
            java.util.Map<String, Map<String, String>> journalProperties = getJournals();


            if(authDocs != null){
                int maxDocs = authDocs.size();

                //import all the authors
                for (int i = 0; i < maxDocs; i++) {
                    SolrDocument solrDocument = authDocs.get(i);
                    if(solrDocument != null){
                        AuthorityValue authorityValue = new AuthorityValue(solrDocument);
                        if(authorityValue.getId() != null){
                            ArrayList<Concept> aConcepts = Concept.findByIdentifier(context,authorityValue.getId());
                            if(aConcepts==null||aConcepts.size()==0)  {
                                Concept aConcept = authScheme.createConcept(authorityValue.getId());

                                if(solrDocument.getFieldValue("source")!=null) {
                                    String source = String.valueOf(solrDocument.getFieldValue("source"));
                                    aConcept.setSource(source);
                                    if(source.equals("LOCAL-DryadJournal"))
                                    {
                                        Map<String,String> val = journalProperties.get(authorityValue.getValue());
                                        if(val!=null){
                                            journalProperties.remove(authorityValue.getValue());
                                        if(val.get("fullname")!=null)
                                        {
                                            aConcept.addMetadata("internal","journal","fullname","",val.get("fullname"),authorityValue.getId(),0);
                                        }

                                        if(val.get("metadataDir")!=null)
                                        {
                                            aConcept.addMetadata("internal","journal","metadataDir","",val.get("metadataDir"),authorityValue.getId(),0);
                                        }
                                        if(val.get("parsingScheme")!=null)
                                        {
                                            aConcept.addMetadata("internal","journal","parsingScheme","",val.get("parsingScheme"),authorityValue.getId(),0);
                                        }
                                        if(val.get("integrated")!=null)
                                        {
                                            aConcept.addMetadata("internal","journal","integrated","",val.get("integrated"),authorityValue.getId(),0);
                                        }
                                        if(val.get("embargoAllowed")!=null)
                                        {
                                            aConcept.addMetadata("internal","journal","embargoAllowed","",val.get("embargoAllowed"),authorityValue.getId(),0);
                                        }
                                        if(val.get("allowReviewWorkflow")!=null)
                                        {
                                            aConcept.addMetadata("internal","journal","allowReviewWorkflow","",val.get("allowReviewWorkflow"),authorityValue.getId(),0);
                                        }
                                        if(val.get("publicationBlackout")!=null)
                                        {
                                            aConcept.addMetadata("internal","journal","publicationBlackout","",val.get("publicationBlackout"),authorityValue.getId(),0);
                                        }
                                        if(val.get("allowReviewWorkflow")!=null)
                                        {
                                            aConcept.addMetadata("internal","journal","allowReviewWorkflow","",val.get("allowReviewWorkflow"),authorityValue.getId(),0);
                                        }
                                        if(val.get("publicationBlackout")!=null)
                                        {
                                            aConcept.addMetadata("internal","journal","publicationBlackout","",val.get("publicationBlackout"),authorityValue.getId(),0);
                                        }
                                        if(val.get("subscriptionPaid")!=null)
                                        {
                                            aConcept.addMetadata("internal","journal","subscriptionPaid","",val.get("subscriptionPaid"),authorityValue.getId(),0);
                                        }
                                        if(val.get("sponsorName")!=null)
                                        {
                                            aConcept.addMetadata("internal","journal","sponsorName","",val.get("sponsorName"),authorityValue.getId(),0);
                                        }
                                        if(val.get("notifyOnReview")!=null)
                                        {
                                            aConcept.addMetadata("internal","journal","notifyOnReview","",val.get("notifyOnReview"),authorityValue.getId(),0);
                                        }
                                        if(val.get("notifyOnArchive")!=null)
                                        {
                                            aConcept.addMetadata("internal","journal","notifyOnArchive","",val.get("notifyOnArchive"),authorityValue.getId(),0);
                                        }
                                        if(val.get("notifyWeekly")!=null)
                                        {
                                            aConcept.addMetadata("internal","journal","notifyWeekly","",val.get("notifyWeekly"),authorityValue.getId(),0);
                                        }
                                        }
                                    }
                                }
                                aConcept.update();
                                Term term = aConcept.createTerm(authorityValue.getValue(),1);
                                context.commit();
                            }
                        }

                    }
                }

            }

            Set<String> keys = journalProperties.keySet();
            for(String key : keys)
            {
                Map<String,String> val = journalProperties.get(key);

                // TODO: THIS SHOULD BE NARROWED BY SCHEME
                Concept[] aConcepts = Concept.findByPreferredLabel(context,val.get("fullname"));
                if(aConcepts==null||aConcepts.length==0)  {
                    String id = AuthorityObject.createIdentifier();

                    Concept aConcept = authScheme.createConcept();
                    aConcept.setSource("LOCAL-DryadJournal");

                    if(val.get("fullname")!=null)
                    {
                        aConcept.addMetadata("internal","journal","fullname","",val.get("fullname"),id,0);
                    }

                    if(val.get("metadataDir")!=null)
                    {
                        aConcept.addMetadata("internal","journal","metadataDir","",val.get("metadataDir"),id,0);
                    }
                    if(val.get("parsingScheme")!=null)
                    {
                        aConcept.addMetadata("internal","journal","parsingScheme","",val.get("parsingScheme"),id,0);
                    }
                    if(val.get("integrated")!=null)
                    {
                        aConcept.addMetadata("internal","journal","integrated","",val.get("integrated"),id,0);
                    }
                    if(val.get("embargoAllowed")!=null)
                    {
                        aConcept.addMetadata("internal","journal","embargoAllowed","",val.get("embargoAllowed"),id,0);
                    }
                    if(val.get("allowReviewWorkflow")!=null)
                    {
                        aConcept.addMetadata("internal","journal","allowReviewWorkflow","",val.get("allowReviewWorkflow"),id,0);
                    }
                    if(val.get("publicationBlackout")!=null)
                    {
                        aConcept.addMetadata("internal","journal","publicationBlackout","",val.get("publicationBlackout"),id,0);
                    }
                    if(val.get("allowReviewWorkflow")!=null)
                    {
                        aConcept.addMetadata("internal","journal","allowReviewWorkflow","",val.get("allowReviewWorkflow"),id,0);
                    }
                    if(val.get("publicationBlackout")!=null)
                    {
                        aConcept.addMetadata("internal","journal","publicationBlackout","",val.get("publicationBlackout"),id,0);
                    }
                    if(val.get("subscriptionPaid")!=null)
                    {
                        aConcept.addMetadata("internal","journal","subscriptionPaid","",val.get("subscriptionPaid"),id,0);
                    }
                    if(val.get("sponsorName")!=null)
                    {
                        aConcept.addMetadata("internal","journal","sponsorName","",val.get("sponsorName"),id,0);
                    }
                    if(val.get("notifyOnReview")!=null)
                    {
                        aConcept.addMetadata("internal","journal","notifyOnReview","",val.get("notifyOnReview"),id,0);
                    }
                    if(val.get("notifyOnArchive")!=null)
                    {
                        aConcept.addMetadata("internal","journal","notifyOnArchive","",val.get("notifyOnArchive"),id,0);
                    }
                    if(val.get("notifyWeekly")!=null)
                    {
                        aConcept.addMetadata("internal","journal","notifyWeekly","",val.get("notifyWeekly"),id,0);
                    }
                    aConcept.update();
                    aConcept.createTerm(val.get("fullname"),1);
                    context.commit();
                }
            }

        }catch (Exception e)
        {
            System.out.print(e);
            System.out.print(e.getStackTrace());
        }

        context.complete();

        System.out.println("Authority imported");
    }


    private HashMap<String, Map<String, String>> getJournals(){

        HashMap<String, Map<String, String>> journalProperties = new HashMap<String, Map<String, String>>();

        String journalPropFile = ConfigurationManager.getProperty("submit.journal.config");
        Properties properties = new Properties();
        try {
            properties.load(new InputStreamReader(new FileInputStream(journalPropFile), "UTF-8"));
            String journalTypes = properties.getProperty("journal.order");

            for (int i = 0; i < journalTypes.split(",").length; i++) {
                String journalType = journalTypes.split(",")[i].trim();

                String str = "journal." + journalType + ".";

                Map<String, String> map = new HashMap<String, String>();
                map.put(FULLNAME, properties.getProperty(str + FULLNAME));
                map.put(METADATADIR, properties.getProperty(str + METADATADIR));
                map.put(INTEGRATED, properties.getProperty(str + INTEGRATED));
                map.put(PUBLICATION_BLACKOUT, properties.getProperty(str + PUBLICATION_BLACKOUT, "false"));
                map.put(NOTIFY_ON_REVIEW, properties.getProperty(str + NOTIFY_ON_REVIEW));
                map.put(NOTIFY_ON_ARCHIVE, properties.getProperty(str + NOTIFY_ON_ARCHIVE));
                map.put(JOURNAL_ID, journalType);
                map.put(SUBSCRIPTION_PAID, properties.getProperty(str + SUBSCRIPTION_PAID));

                String key = properties.getProperty(str + FULLNAME);
                if(key!=null&&key.length()>0){
                    journalProperties.put(key, map);
                }
            }

        }catch (IOException e) {
            //log.error("Error while loading journal properties", e);
        }
        return journalProperties;

    }

    private AuthoritySearchService getSearchService(){
        DSpace dspace = new DSpace();

        org.dspace.kernel.ServiceManager manager = dspace.getServiceManager() ;

        return manager.getServiceByName(AuthoritySearchService.class.getName(),AuthoritySearchService.class);
    }
}

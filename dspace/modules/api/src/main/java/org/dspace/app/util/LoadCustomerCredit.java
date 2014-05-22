package org.dspace.app.util;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.jxpath.xml.DOMParser;
import org.apache.log4j.Logger;
import org.apache.xml.serialize.OutputFormat;
import org.apache.xml.serialize.XMLSerializer;
import org.dspace.content.DCDate;
import org.dspace.content.Item;
import org.dspace.core.ConfigurationManager;
import org.dspace.core.Context;

import javax.net.ssl.HttpsURLConnection;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.*;
import org.xml.sax.InputSource;

/**
 * User: lantian @ atmire . com
 * Date: 7/21/14
 * Time: 2:27 PM
 */
public class LoadCustomerCredit {

        /** DSpace Context object */
        private Context context;

        protected Logger log = Logger.getLogger(LoadCustomerCredit.class);
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

            LoadCustomerCredit ca = new LoadCustomerCredit();
            options.addOption("i", "customer id", true, "customer id");

            CommandLine line = parser.parse(options, argv);
            String credit = ca.loadCredit(line.getOptionValue("i"));
            System.out.println("credit : "+ credit);
        }



        /**
         * constructor, which just creates and object with a ready context
         *
         * @throws Exception
         */
        public LoadCustomerCredit()
                throws Exception
        {

        }

        /**
         * Create the administrator with the given details.  If the user
         * already exists then they are simply upped to administrator status
         *
         * @throws Exception
         */
        public String loadCredit(String customerId)
                throws Exception
        {
            // Of course we aren't an administrator yet so we need to
            // circumvent authorisation

            String credit=null;
            try {


                String requestUrl = ConfigurationManager.getProperty("association.anywhere.link");

                try {
                    String url = requestUrl;
                    URL obj = new URL(url);
                    URLConnection con = obj.openConnection();

                    String userName = ConfigurationManager.getProperty("association.anywhere.username");
                    String passWord = ConfigurationManager.getProperty("association.anywhere.password");

//                    <?xml version="1.0" encoding="UTF-8" ?>
//                    <creditRequest>
//                    <integratorUsername>datadryad<integratorUsername>
//                    <integratorPassword>D8ta1ntgr8tn</integratorPassword>
//                    <custId>1</custId>
//                    <txTy>PREPAID</txTy>
//                    </creditRequest>

                    String paiedType = "paiedType";

                    DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
                    DocumentBuilder docBuilder = docFactory.newDocumentBuilder();

                    // root elements
                    Document doc = docBuilder.newDocument();
                    doc.setXmlVersion("1.0");

                    Element rootElement = doc.createElement("creditRequest");
                    doc.appendChild(rootElement);

                    Element user = doc.createElement("integratorUsername");
                    user.appendChild(doc.createTextNode(userName));
                    rootElement.appendChild(user);

                    Element pass = doc.createElement("integratorPassword");
                    pass.appendChild(doc.createTextNode(passWord));
                    rootElement.appendChild(pass);

                    Element customer = doc.createElement("custId");
                    customer.appendChild(doc.createTextNode(customerId));
                    rootElement.appendChild(customer);

                    Element txTy = doc.createElement("txTy");
                    txTy.appendChild(doc.createTextNode(paiedType));
                    rootElement.appendChild(txTy);

                    OutputFormat format    = new OutputFormat (doc);
                    // as a String
                    StringWriter stringOut = new StringWriter ();
                    XMLSerializer serial   = new XMLSerializer (stringOut,
                            format);
                    serial.serialize(doc);
                    // Display the XML


                    String urlParameters ="p_input_xml_doc="+stringOut.toString().replaceAll("\n|\r", "");
                    // Send post request
                    con.setDoOutput(true);
                    DataOutputStream wr = new DataOutputStream(con.getOutputStream());
                    wr.writeBytes(urlParameters);
                    wr.flush();
                    wr.close();

                    System.out.println("\nSending 'POST' request to URL : " + url);
                    System.out.println("Post parameters : " + urlParameters);


                    BufferedReader in = new BufferedReader(
                            new InputStreamReader(con.getInputStream()));
                    String inputLine;
                    StringBuffer response = new StringBuffer();

                    while ((inputLine = in.readLine()) != null) {
                        response.append(inputLine);
                    }
                    in.close();

                    InputSource is = new InputSource(new StringReader(response.toString()));
                    Document docResponse = docBuilder.parse(is);
                    NodeList errors =  docResponse.getElementsByTagName("errors");
                    if(errors!=null)
                    {
                       System.out.println("errors when updateing customer credit:"+response.toString());
                       log.error("errors when updateing customer credit:"+response.toString());
                    }
                    else{
                        NodeList list = docResponse.getElementsByTagName("client");
                        if(list!=null&&list.item(0)!=null)
                        {
                            credit = list.item(0).getTextContent();
                        }
                    }




//            if(ConfigurationManager.getProperty("payment-system","paypal.returnurl").length()>0)
//            get.addParameter("RETURNURL", ConfigurationManager.getProperty("payment-system","paypal.returnurl"));
                }
                catch (Exception e) {
                    log.error("errors when updateing customer credit:", e);
                    return null;
                }

            }catch (Exception e)
            {
                System.out.print(e);
                System.out.print(e.getStackTrace());
            }

            return credit;
        }


        public String updateCredit(String customerId)
                throws Exception
        {
            // Of course we aren't an administrator yet so we need to
            // circumvent authorisation

            String credit=null;
            try {


                String requestUrl = ConfigurationManager.getProperty("association.anywhere.link");

                try {
                    String url = requestUrl;
                    URL obj = new URL(url);
                    URLConnection con = obj.openConnection();

                    String userName = ConfigurationManager.getProperty("association.anywhere.username");
                    String passWord = ConfigurationManager.getProperty("association.anywhere.password");

    //                    <?xml version="1.0" encoding="UTF-8" ?>
    //                    <creditRequest>
    //                    <integratorUsername>datadryad<integratorUsername>
    //                    <integratorPassword>D8ta1ntgr8tn</integratorPassword>
    //                    <custId>1</custId>
    //                    <txTy>PREPAID</txTy>
    //                    </creditRequest>

                    String paiedType = "DEFERRED";

                    DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
                    DocumentBuilder docBuilder = docFactory.newDocumentBuilder();

                    // root elements
                    Document doc = docBuilder.newDocument();
                    doc.setXmlVersion("1.0");

                    Element rootElement = doc.createElement("credit-request");
                    doc.appendChild(rootElement);

                    Element user = doc.createElement("vendor-id");
                    user.appendChild(doc.createTextNode(userName));
                    rootElement.appendChild(user);

                    Element pass = doc.createElement("vendor-password");
                    pass.appendChild(doc.createTextNode(passWord));
                    rootElement.appendChild(pass);

                    Element customer = doc.createElement("cust-id");
                    customer.appendChild(doc.createTextNode(customerId));
                    rootElement.appendChild(customer);

                    Element txTy = doc.createElement("trans-type");
                    txTy.appendChild(doc.createTextNode(paiedType));
                    rootElement.appendChild(txTy);

                    DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
                    java.util.Date date = new java.util.Date();
                    Element dateElement = doc.createElement("trans-date");
                    dateElement.appendChild(doc.createTextNode(dateFormat.format(date)));
                    rootElement.appendChild(txTy);

                    Element deduct = doc.createElement("cred-accepted");
                    deduct.appendChild(doc.createTextNode("-1"));
                    rootElement.appendChild(deduct);


                    OutputFormat format    = new OutputFormat (doc);
                    // as a String
                    StringWriter stringOut = new StringWriter ();
                    XMLSerializer serial   = new XMLSerializer (stringOut,
                            format);
                    serial.serialize(doc);
                    // Display the XML


                    String urlParameters ="p_input_xml_doc="+stringOut.toString().replaceAll("\n|\r", "");
                    // Send post request
                    con.setDoOutput(true);
                    DataOutputStream wr = new DataOutputStream(con.getOutputStream());
                    wr.writeBytes(urlParameters);
                    wr.flush();
                    wr.close();

                    System.out.println("\nSending 'POST' request to URL : " + url);
                    System.out.println("Post parameters : " + urlParameters);


                    BufferedReader in = new BufferedReader(
                            new InputStreamReader(con.getInputStream()));
                    String inputLine;
                    StringBuffer response = new StringBuffer();

                    while ((inputLine = in.readLine()) != null) {
                        response.append(inputLine);
                    }
                    in.close();

                    InputSource is = new InputSource(new StringReader(response.toString()));
                    Document docResponse = docBuilder.parse(is);
                    NodeList errors =  docResponse.getElementsByTagName("errors");
                    if(errors!=null)
                    {
                        System.out.println(response.toString());
                        log.error("errors when updateing customer credit:"+response.toString());
                    }
                    else{
                        NodeList list = docResponse.getElementsByTagName("client");
                        if(list!=null&&list.item(0)!=null)
                        {
                            credit = list.item(0).getTextContent();
                        }
                    }




    //            if(ConfigurationManager.getProperty("payment-system","paypal.returnurl").length()>0)
    //            get.addParameter("RETURNURL", ConfigurationManager.getProperty("payment-system","paypal.returnurl"));
                }
                catch (Exception e) {
                    log.error("errors when updateing customer credit:", e);
                    return null;
                }

            }catch (Exception e)
            {
                System.out.print(e);
                System.out.print(e.getStackTrace());
            }

            return credit;
        }
    }


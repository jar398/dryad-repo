package org.dspace.app.util;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

import org.apache.commons.httpclient.HttpClient;


import org.apache.commons.httpclient.methods.GetMethod;

import org.apache.log4j.Logger;

import org.apache.xml.serialize.OutputFormat;
import org.apache.xml.serialize.XMLSerializer;

import org.dspace.core.ConfigurationManager;
import org.dspace.core.Context;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.text.DateFormat;
import java.text.SimpleDateFormat;


import org.w3c.dom.*;
import org.xml.sax.InputSource;



import java.io.BufferedReader;

import java.io.InputStreamReader;




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

                    String url = requestUrl.replace("customerId",customerId);
                    HttpClient client = new HttpClient();

                    GetMethod get = new GetMethod(url);

//                    HttpMethodParams params = new HttpMethodParams();
//
//                    params.setParameter("test","test");
//
//                    get.setParams(params);

                    client.executeMethod(get);

                    if( get.getStatusCode() <= 299 )
                    {
                        System.out.println("Response Code : "
                                + get.getStatusLine().getStatusCode());
                        String result = get.getResponseBodyAsString();
                        System.out.println("Response body : "+result);

                        String[] result1 = result.split("totCreditsAccepted");
                        if(result1!=null&&result1.length>3){
                            credit = result1[1];
                        }
                        else
                        {
                            log.error("error when loading customer credit");
                        }
//
//                        DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
//                        DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
//                        InputSource is = new InputSource(get.getResponseBodyAsStream());
//                        Document docResponse = docBuilder.parse(is);
//                        NodeList errors=  docResponse.getElementsByTagName("errors");
//                        if(errors!=null)
//                        {
//                            int i = 0;
//                            while(i < errors.getLength())
//                            {
//                                Node error = errors.item(i);
//                                String value = error.getNodeValue();
//                                log.error("error in loading client credit : "+ value);
//                                i++;
//                            }
//                            log.error("errors when loading customer credit:"+result);
//                        }
//                        else{
//                            NodeList list = docResponse.getElementsByTagName("totCreditsAccepted");
//                            if(list!=null&&list.item(0)!=null)
//                            {
//                                credit = list.item(0).getTextContent();
//                            }
//                        }

                    }


                }
                catch (Exception e) {
                    log.error("errors when loading customer credit:", e);
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

                String requestUrl = ConfigurationManager.getProperty("association.anywhere.updatelink");

                try {
                        String url = requestUrl.replace("customerId",customerId);
                        HttpClient client = new HttpClient();

                        GetMethod get = new GetMethod(url);

//                    HttpMethodParams params = new HttpMethodParams();
//
//                    params.setParameter("test","test");
//
//                    get.setParams(params);

                        client.executeMethod(get);

                        if( get.getStatusCode() <= 299 )
                        {
                            System.out.println("Response Code : "
                                    + get.getStatusLine().getStatusCode());
                            String result = get.getResponseBodyAsString();
                            System.out.println("Response body : "+result);

                            int start = result.indexOf("totCreditsAccepted");
                            int end = result.lastIndexOf("totCreditsAccepted") ;
                            System.out.println("start : "+start+", end"+end);
                            if(start>0&&end>0){
                                credit = result.substring(start,end);
                            }
                            else
                            {
                                log.error("error when loading customer credit");
                            }
                        }
                    }

            catch (Exception e)
            {
                System.out.print(e);
                System.out.print(e.getStackTrace());
            }

            return credit;
        }
    }


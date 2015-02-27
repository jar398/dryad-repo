package org.dspace.content.packager;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import org.dspace.authorize.AuthorizeException;
import org.dspace.content.crosswalk.CrosswalkException;
import org.dspace.content.DSpaceObject;
import org.dspace.content.packager.PackageException;
import org.dspace.content.packager.PackageParameters;
import org.dspace.core.ConfigurationManager;

import org.dspace.content.packager.AbstractPackageIngester;
import org.dspace.content.packager.PackageValidationException;
import org.dspace.core.Context;

import org.datadryad.api.DryadDataPackage;
import org.datadryad.api.DryadDataFile;

/*
  Modeled after:
  dspace-api/src/main/java/org/dspace/content/packager/AbstractMETSIngester.java:
  dspace-api/src/main/java/org/dspace/content/packager/DSpaceMETSIngester.java
  dspace/modules/bagit/dspace-bagit-api/src/main/java/org/dspace/content/packager/BagItDisseminator.java
  dspace/modules/bagit/dspace-bagit-api/src/main/java/org/dspace/content/packager/BagItBuilder.java
  See also:
  dspace-api/src/main/java/org/dspace/content/packager/METSManifest.java

  How do we get here?  See
  plugin.named.org.dspace.content.packager.PackageIngester in
  dspace.cfg.
  org.dspace.sword.DryadBagItIngester = http://purl.org/net/sword-types/bagit \

  To turn on logging, edit dspace/config/log4j.properties
*/

public class DryadBagItIngester
        extends AbstractPackageIngester
{
    /** Log4j logger */
    private static final Logger mylog = Logger.getLogger(DryadBagItIngester.class);

    List<ZipEntry[]> entries;
    ZipEntry dryadpkg = null, dryadpub = null;

    @Override
    public DSpaceObject ingest(Context context, DSpaceObject parent, File pkgFile,
                        PackageParameters params, String license)
        throws PackageException, CrosswalkException,
               AuthorizeException, SQLException, IOException
    {
	mylog.info("Parsing package, file=" + pkgFile.getName());
        ZipFile zip = new ZipFile(pkgFile);
        parseZip(zip);

        mylog.info(String.format("pkg: %s %s", dryadpkg.getName(), dryadpkg.getSize()));
	mylog.info(String.format("pub: %s %s", dryadpub.getName(), dryadpub.getSize()));

        DryadDataPackage dp = DryadDataPackage.createInWorkflow(context);
	// Do data package crosswalk
	// Do publication crosswalk (!?)
	// DryadDataPackage.setPublicationDOI(...)

        // {metadata, content}
        for (ZipEntry[] entryPair : entries) {
	    ZipEntry metadata = entryPair[0];
	    ZipEntry data = entryPair[1];
            mylog.info(String.format("Processing: %s %s", metadata.getName(), data.getName()));
            DryadDataFile df = DryadDataFile.create(context, dp);
            df.addBitstream(zip.getInputStream(data));
            // Do data file crosswalk
        }
        zip.close();
        return dp.getItem();
    }

    // Computes:
    //   entries   -- a list of {metadata, datafile} entry pairs
    //   dryadpub   -- metadata for publication
    //   dryadpkg   -- metadata for data package

    void parseZip(ZipFile zip) throws IOException, PackageValidationException
    {
        List<ZipEntry> allEntries = Collections.list((Enumeration<ZipEntry>)zip.entries());
        int pos = -1;
        Map<String, String> datafiles = new HashMap<String, String>();
        Map<String, String> metafiles = new HashMap<String, String>();
        for (ZipEntry entry : allEntries) {
            String wholename = entry.getName();
            if (wholename.endsWith("/") || wholename.startsWith("__MACOSX"))
                continue;
            int where = wholename.indexOf("data/");
            if (where >= 0) {
                where += 5;
                if (pos >= 0) {
                    if (where != pos)
                        throw new PackageValidationException("Multiple data/ directories");
                } else
                    pos = where;
                String name = wholename.substring(pos);
                if (name.equals("dryadpub.xml"))
                    dryadpub = entry;
                else if (name.equals("dryadpkg.xml"))
                    dryadpkg = entry;
                else {
                    // E.g. dryadfile-1/ApineCYTB.nexus
                    //  and dryadfile-1/dryadfile-1.xml
                    int slash = name.indexOf('/');
                    if (slash >= 0) {
                        String before = name.substring(0, slash);
                        String after = name.substring(slash+1);
                        if (after.startsWith("dryadfile-") && 
                              after.endsWith(".xml") &&
                              metafiles.get(before) == null)
                            metafiles.put(before, wholename);
                        else if (datafiles.get(before) == null)
                            datafiles.put(before, wholename);
                        else
                            throw new PackageValidationException
                                ("fifth wheel: %s\t%s".format(wholename, entry.getSize()));
                    } else
                        throw new PackageValidationException
                            ("unknown: %s\t%s".format(wholename, entry.getSize()));
                }

                if (false) {
                    InputStreamReader r = new InputStreamReader(zip.getInputStream(entry));
                    char[] buf = new char[1000];
                    int howmany = r.read(buf, 0, 1000);
                    mylog.info(new String(buf, 0, howmany));
                }
            }
        }
        if (dryadpub == null || dryadpkg == null)
            throw new PackageValidationException("missing pub or pkg");
        entries = new ArrayList<ZipEntry[]>();
        for (String before : datafiles.keySet()) {
            String m = metafiles.get(before);
            String d = datafiles.get(before);
            if (m != null && d != null) {
                ZipEntry me = zip.getEntry(metafiles.get(before));
                ZipEntry de = zip.getEntry(datafiles.get(before));
                if (me != null && de != null)
                    entries.add(new ZipEntry[]{me, de});
                else
                    // shouldn't happen
                    throw new PackageValidationException
                        ("missing entry: me=%s de=%s".format(m, d));
            } else
                // could happen if input is corrupt
                throw new PackageValidationException
                    ("missing file: m=%s d=%s".format(m, d));
        }
    }

    @Override
    public DSpaceObject replace(Context context, DSpaceObject dso,
                         File pkgFile, PackageParameters params)
        throws PackageException, UnsupportedOperationException,
               CrosswalkException, AuthorizeException,
               SQLException, IOException
    {
        return null;
    }

    @Override
    public String getParameterHelp() {
        return null;
    }



}

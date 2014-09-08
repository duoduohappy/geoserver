package org.geoserver.rest.test;

import static org.restlet.data.ChallengeScheme.HTTP_BASIC;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.restlet.Response;
import org.restlet.data.MediaType;
import org.restlet.data.Method;
import org.restlet.data.Reference;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.ClientResource;
import org.restlet.resource.ResourceException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;

public class RunTest {

    private static final List<String> BASE_URLS = ImmutableList.of(//
            "http://eva01:8081/geoserver/rest/",//
            "http://eva01:8082/geoserver/rest/");

    private final Iterator<String> roundRobbinUrls = Iterators.cycle(BASE_URLS);

    public static void main(String args[]) {
        try {
            new RunTest().run();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.exit(0);
        }
    }

    final int numConcClients = 8;

    final ExecutorService executor = Executors.newFixedThreadPool(numConcClients);

    private void run() throws Exception {

        final AtomicInteger count = new AtomicInteger();

        for (int i = 0; i < 1000; i++) {
            executor.submit(new Runnable() {

                @Override
                public void run() {
                    final int index = count.incrementAndGet();
                    final String wsName = createWorkspace(index);
                    final String dsName = createDataStore(wsName, index);
                    final String ftName = createFeatureTypeAndLayer(wsName, dsName, index);

                    // try {
                    // Thread.sleep(200);
                    // } catch (InterruptedException e) {
                    // e.printStackTrace();
                    // return;
                    // }

                    verifyFeatureType(wsName, dsName, ftName, 23);
                    modifyFeatureType(wsName, dsName, ftName);
                    verifyFeatureType(wsName, dsName, ftName, 1);
                    addFeatureTypeAttribute(wsName, dsName, ftName);
                    verifyFeatureType(wsName, dsName, ftName, 2);

                    delete("layers/" + ftName + ".xml");
                    delete("workspaces/" + wsName + "/datastores/" + dsName + "/featuretypes/"
                            + ftName + ".xml");
                    delete("workspaces/" + wsName + "/datastores/" + dsName + ".xml");
                    delete("workspaces/" + wsName + ".xml");

                }
            });
        }

        executor.shutdown();
        while (!executor.isTerminated()) {
            executor.awaitTermination(1, TimeUnit.MINUTES);
        }
    }

    private void delete(final String relativePath) {
        final ClientResource client = newClient(relativePath);
        client.getRequest().setResourceRef(relativePath);

        Reference targetRef = client.getRequest().getResourceRef().getTargetRef();
        try {
            client.delete();
            Status status = client.getResponse().getStatus();
            System.out.printf("Delete response for %s: %s\n", targetRef, status);
        } catch (Exception e) {
            System.err.printf("Exception deleting %s: %s\n", targetRef, e.getMessage());
        }
    }

    private String createFeatureTypeAndLayer(final String wsName, final String dsName,
            final int index) {
        final String ftName = "states-" + index;
        final String relativePath = "workspaces/" + wsName + "/datastores/" + dsName
                + "/featuretypes";
        final String ftXml = "<featureType>\n" + //
                "  <name>" + ftName + "</name>\n" + //
                "  <nativeName>states</nativeName>\n" + //
                "  <title>States " + index + "</title>\n" + //
                "  <srs>EPSG:4326</srs>\n" + //
                "</featureType>\n";
        postXml(relativePath, ftXml);
        return ftName;
    }

    private void verifyFeatureType(final String wsName, final String dsName, final String ftName,
            final int expectedAttributeCount) {
        final String relativePath = "workspaces/" + wsName + "/datastores/" + dsName
                + "/featuretypes/" + ftName + ".xml";
        for (int i = 0; i < BASE_URLS.size(); i++) {
            final ClientResource client = newClient(relativePath);
            client.getRequest().setResourceRef(relativePath);

            try {
                Representation representation = client.get();
                Status status = client.getResponse().getStatus();

                Reference targetRef = client.getRequest().getResourceRef().getTargetRef();
                System.out.println("GET " + targetRef + ": " + status);
                StringWriter writer = new StringWriter();
                representation.write(writer);
                String stringRep = writer.toString();
                Pattern p = Pattern.compile("<attribute>");
                Matcher m = p.matcher(stringRep);
                int count = 0;
                while (m.find()) {
                    count += 1;
                }
                if (expectedAttributeCount != count) {
                    System.err.println(String.format("Expected %d attributes, got %d:\n%s\n",
                            expectedAttributeCount, count, stringRep));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void addFeatureTypeAttribute(String wsName, String dsName, String ftName) {
        final String relativePath = "workspaces/" + wsName + "/datastores/" + dsName
                + "/featuretypes/" + ftName + ".xml";

        final String ftXml = "<featureType>\n"
                + "  <name>"
                + ftName
                + "</name>\n"//
                + "  <nativeName>states</nativeName>\n"//
                + "  <title>States "
                + ftName
                + " + modified</title>\n"//
                + "  <srs>EPSG:4326</srs>\n"//
                + "  <attributes>"//
                + "     <attribute><name>state_fips</name><binding>java.lang.String</binding></attribute>"
                + "     <attribute><name>geom</name><binding>com.vividsolutions.jts.geom.MultiPolygon</binding></attribute>"//
                + "  </attributes>"//
                + "</featureType>\n";
        putXml(relativePath, ftXml);
    }

    private void modifyFeatureType(final String wsName, final String dsName, final String ftName) {

        final String relativePath = "workspaces/" + wsName + "/datastores/" + dsName
                + "/featuretypes/" + ftName + ".xml";

        final String ftXml = "<featureType>\n"
                + "  <name>"
                + ftName
                + "</name>\n"//
                + "  <nativeName>states</nativeName>\n"//
                + "  <title>States "
                + ftName
                + " + modified</title>\n"//
                + "  <srs>EPSG:4326</srs>\n"//
                + "  <attributes>"//
                + "     <attribute>"//
                + "             <name>geom</name><minOccurs>0</minOccurs><maxOccurs>1</maxOccurs><nillable>true</nillable><binding>com.vividsolutions.jts.geom.MultiPolygon</binding>"
                + "     </attribute>"//
                + "  </attributes>"//
                + "</featureType>\n";
        putXml(relativePath, ftXml);
    }

    private String createDataStore(final String wsName, final int index) {
        final String dsName = "ds-" + index;
        String dsxml = "<dataStore>\n" + //
                "<name>" + dsName + "</name>\n" + //
                " <connectionParameters>\n" + //
                "  <host>hal</host>\n" + //
                "  <port>5432</port>\n" + //
                "  <database>postgis</database>\n" + //
                "  <user>postgres</user>\n" + //
                "  <passwd>geo123</passwd>\n" + //
                "  <dbtype>postgis</dbtype>\n" + //
                " </connectionParameters>\n" + //
                "</dataStore>";
        final String relativePath = "workspaces/" + wsName + "/datastores";

        postXml(relativePath, dsxml);
        return dsName;
    }

    private String createWorkspace(final int index) {
        final String wsName = "ws-" + index;
        final String wsxml = "<workspace><name>" + wsName + "</name></workspace>";
        postXml("workspaces", wsxml);
        return wsName;
    }

    private Representation putXml(final String relativePath, final String xml) {
        return sendXml(relativePath, xml, Method.PUT);
    }

    private Representation postXml(final String relativePath, final String xml) {
        return sendXml(relativePath, xml, Method.POST);
    }

    private Representation sendXml(final String relativePath, final String xml, Method method) {
        ClientResource client = newClient(relativePath);
        StringRepresentation reqRep = new StringRepresentation(xml);
        reqRep.setMediaType(MediaType.APPLICATION_XML);
        Representation result;
        try {
            if (Method.PUT.equals(method)) {
                result = client.put(reqRep);
            } else if (Method.POST.equals(method)) {
                result = client.post(reqRep);
            } else {
                throw new IllegalArgumentException("Method: " + method);
            }
        } catch (ResourceException re) {
            Representation responseEntity = client.getResponseEntity();
            if (responseEntity != null) {
                System.err.println(relativePath + ": server response: ");
                // try {
                // responseEntity.write(System.err);
                // } catch (IOException e) {
                // throw Throwables.propagate(e);
                // }
            }
            re.printStackTrace();
            return null;
        }
        Response response = client.getResponse();
        Reference targetRef = client.getRequest().getResourceRef().getTargetRef();
        System.out.println(method.getName() + " to " + targetRef + ": " + response.getStatus());
        return result;
    }

    ThreadLocal<Iterator<ClientResource>> CLIENTS_BY_THREAD = new ThreadLocal<Iterator<ClientResource>>() {

        @Override
        protected Iterator<ClientResource> initialValue() {
            List<ClientResource> clients = new ArrayList<ClientResource>();
            for (String baseUrl : BASE_URLS) {
                ClientResource client = new ClientResource(baseUrl);
                client.getRequest().getResourceRef().setBaseRef(roundRobbinUrls.next());
                client.setChallengeResponse(HTTP_BASIC, "admin", "geoserver");
                clients.add(client);
            }
            return Iterators.cycle(clients);
        }
    };

    private synchronized ClientResource newClient(final String relativePath) {
        ClientResource client = CLIENTS_BY_THREAD.get().next();
        if (null != relativePath) {
            client.getRequest().setResourceRef(relativePath);
        }
        return client;

        // ClientResource client = new ClientResource("");
        // client.getRequest().getResourceRef().setBaseRef(roundRobbinUrls.next());
        // if (null != relativePath) {
        // client.getRequest().setResourceRef(relativePath);
        // }
        // client.setChallengeResponse(HTTP_BASIC, "admin", "geoserver");
        // return client;
    }
}

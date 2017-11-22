/*
 * CU9 WP940 Validation
 * Copyright (C) 2006-2014 Gaia Data Processing and Analysis Consortium
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 *
 */
package ics.astro.tap;

//import gaia.cu9.validation.common.ValidationException;
//import gaia.cu9.validation.configuration.SharedGroups;
//import gaia.cu9.validation.tools.ValidationRunner;
import ics.astro.tap.security.DumbHostnameVerifier;
import ics.astro.tap.security.DumbX509TrustManager;
//import net.xml.ivoa.uws.JobSummary;
//import net.xml.ivoa.votable.VOTABLE;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.X509TrustManager;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.*;
import java.net.*;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;

/**
 * Class for connecting to GACS TAP service
 * <p>
 * The cookies are managed by Java's cookieManager.
 *
 * @author I-Chun Shih (i-chun.shih at obspm.fr)
 * @version $Id: TapConnector.java 586302 2017-11-10 13:08:47Z ishih $
 */
public class TapConnector {

    private String TAP_URL = null;


//    private Credentials credentials;

    private static final String ENC = "UTF-8";
    private static final String LINE_FEED = "\r\n";
    private static final String BOUNDARY = "===" + System.currentTimeMillis() + "===";

    private static final Logger LOG = LoggerFactory.getLogger(TapConnector.class);

    /**
     * Default Constructor
     */
    public TapConnector() {
        CookieHandler.setDefault(new CookieManager());
    }

    /**
     * Sets TAP HTTP URL address
     *
     * @param tapUrl
     */
    public void setTapUrl(String tapUrl) {
        TAP_URL = tapUrl;
    }

    /**
     * Sets TAP HTTP URL address
     *
     * @param tapService
     */
    public void setTapUrl(TapService tapService) {
        TAP_URL = tapService.name();
    }

    /**
     * Gets the TAP URL address
     *
     * @return the tap top url address
     */
    public String getTapUrl() {
        return TAP_URL;
    }


//    /**
//     * Gets sign-in user name
//     * <p>This function is only valid to GACS TAP+ service</p>
//     *
//     * @return
//     */
//    public String getUserName() {
//        return credentials.username;
//    }

    /**
     * Gets HttpURLConnection object.
     *
     * @param httpUrl
     * @return
     * @throws IOException
     * @throws NoSuchAlgorithmException
     * @throws KeyManagementException
     */
    protected HttpURLConnection getHttpURLConnection(String httpUrl)
            throws IOException, KeyManagementException, NoSuchAlgorithmException {
        URL url;
        try {
            url = new URL(httpUrl);
            String protocol = url.getProtocol().toUpperCase();
            switch (protocol) {
                case "HTTP":
                    return setHttpURLConnection(url);
                case "HTTPS":
                    return setHttpsURLConnection(url);
                default:
                    LOG.error("{} is currently not supported, returning null value.", protocol);
                    return null;
            }
        } catch (MalformedURLException e) {
            LOG.error("The URL form is incorrect, see ", e);
            return null;
        }

    }

    /**
     * Initialises a HTTP Connection
     *
     * @param url
     * @return
     * @throws IOException
     */
    HttpURLConnection setHttpURLConnection(URL url) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setUseCaches(false);
        return conn;
    }

    /**
     * Initialises a HTTP Connection
     * <p>
     * <p>The method is similar to {@code getHttpURLConnection} but deals with
     * secure HTTP. This is part of TAP+ specification</p>
     *
     * @param url
     * @return
     * @throws IOException
     * @throws NoSuchAlgorithmException
     * @throws KeyManagementException
     */
    HttpsURLConnection setHttpsURLConnection(URL url)
            throws IOException, KeyManagementException, NoSuchAlgorithmException {
        HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
        // To simplify the certificate
        // Set a socket factory that has the manager trusting all certificates
        SSLSocketFactory socketFactory = getSocketFactory();
        conn.setSSLSocketFactory(socketFactory);
        // And overrule the standard hostname verifier (due to absent
        // hostname in certificates generated with keytool).
        conn.setHostnameVerifier(new DumbHostnameVerifier());
        conn.setUseCaches(false);
        // by Jesus Salgado
        return conn;
    }

    /**
     * Views the header of the HTTP page
     *
     * @param connection
     */
    protected void viewHeader(HttpURLConnection connection) {
        Map<String, List<String>> headers = connection.getHeaderFields();
        headers.keySet().stream().forEach(k -> System.out.println(String.format("%s: %s", k, headers.get(k))));
    }

    /**
     * Views the body of the HTTP page
     *
     * @param connection
     * @param lines      display first number of lines, if null display all
     * @throws IOException
     */
    protected void viewContent(HttpURLConnection connection, Integer lines) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        if (lines != null)
            br.lines().limit(lines).forEach(l -> System.out.println(l));
        else
            br.lines().forEach(l -> System.out.println(l));
        br.close();
    }

    /**
     * SSL Socket factory implementation for Gacs
     *
     * @throws NoSuchAlgorithmException
     * @throws KeyManagementException
     * @author Jesus Salgado
     */
    private SSLSocketFactory getSocketFactory() throws NoSuchAlgorithmException, KeyManagementException {
        SSLSocketFactory socketFactory = null;
        SSLContext context = SSLContext.getInstance("SSLv3");
        ;
        context.init(null, new X509TrustManager[]{new DumbX509TrustManager()}, null);
        socketFactory = context.getSocketFactory();
        return socketFactory;
    }


    /**
     * Gets Response code
     *
     * @param conn
     * @return HTTP response code
     * @throws IOException
     */
    int getResponseCode(HttpURLConnection conn) throws IOException {
        int code = conn.getResponseCode();
        switch (code) {
            case HttpURLConnection.HTTP_OK:
                LOG.debug("Response: {}", code);
                break;
            default:
                LOG.warn("Response: {} -> Checking why it is not 200.", code);
        }
        return code;
    }

//    /**
//     * This method works with GACS TAP service only.
//     * <p>
//     * Ideally, login through secure HTTP is safer.
//     *
//     * @return the HTTP response code, otherwise null
//     * @throws Exception
//     */
//    public Integer login() throws Exception {
//        Integer reponseCode = null;
//        String httpUrl = TAP_URL + "/login";
//        try {
//            HttpURLConnection conn = setHttpURLConnection(httpUrl);
//            conn.setRequestMethod("POST");
//            conn.setDoOutput(true);
//            conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
//            conn.setRequestProperty("User-Agent", "CodeJava Agent");
//            DataOutputStream printOut = new DataOutputStream(conn.getOutputStream());
//            StringBuilder content = new StringBuilder();
//            content.append("username=").append(URLEncoder.encode(credentials.getUserName(), ENC));
//            content.append("&password=").append(URLEncoder.encode(credentials.getPassWord(), ENC));
//            printOut.writeBytes(content.toString());
//            printOut.flush();
//            printOut.close();
//            conn.connect();
//            reponseCode = getResponseCode(conn);
//            conn.disconnect();
//        } catch (IOException | KeyManagementException | NoSuchAlgorithmException e) {
//            throw new Exception("Problem with TapConnector, see ", e);
//        }
//        return reponseCode;
//    }

//    /**
//     * This method works with GACS TAP service only.
//     *
//     * @return the HTTP response code, otherwise null
//     * @throws Exception
//     */
//    public Integer logout() throws Exception {
//        Integer responseCode = null;
//        String httpUrl = TAP_URL + "/logout";
//        try {
//            HttpURLConnection conn = setHttpURLConnection(httpUrl);
//            conn.setRequestMethod("POST");
//            conn.connect();
//            responseCode = getResponseCode(conn);
//            conn.disconnect();
//        } catch (IOException | KeyManagementException | NoSuchAlgorithmException e) {
//            throw new Exception("Problem with TapConnector, see ", e);
//        }
//        return responseCode;
//    }

    /**
     * Gets the InputStream of the HTTP connection
     *
     * @param conn
     * @return inputStream
     * @throws IOException
     */
    InputStream getInputStreamOf(HttpURLConnection conn) throws IOException {
        if (getResponseCode(conn) == HttpURLConnection.HTTP_OK) {
            return conn.getInputStream();
        } else
            return null;
    }

    /**
     * Gets the table list
     *
     * @return InputStream or null if HTTP response code is not 200
     * @throws Exception
     */
    public InputStream getAvailableTables() throws Exception {
        String httpUrl = TAP_URL + "/tap/tables?share_info=TRUE&share_accessible=TRUE";
        try {
            HttpURLConnection conn = getHttpURLConnection(httpUrl);
            conn.connect();
            return getInputStreamOf(conn);
        } catch (KeyManagementException | NoSuchAlgorithmException | IOException e) {
            throw new Exception("Problem with TapConnector, see ", e);
        }
    }

    /**
     * Gets asynchronous jobs list
     *
     * @return InputStream or null if HTTP response code is not 200
     * @throws Exception
     */
    public InputStream getJobList() throws Exception {
        String httpUrl = TAP_URL + "/tap/async";
        try {
            HttpURLConnection conn = getHttpURLConnection(httpUrl);
            return getInputStreamOf(conn);
        } catch (KeyManagementException | NoSuchAlgorithmException | IOException e) {
            throw new Exception("Problem with TapConnector, see ", e);
        }
    }

    /**
     * Gets the asynchronous job summary
     *
     * @param jobId
     * @return InputStream or null if HTTP response code is not 200
     * @throws Exception
     */
    public InputStream getJobSummary(String jobId) throws Exception {
        String httpUrl = TAP_URL + "/tap/async/" + jobId;
        try {
            HttpURLConnection conn = getHttpURLConnection(httpUrl);
            return getInputStreamOf(conn);
        } catch (KeyManagementException | NoSuchAlgorithmException | IOException e) {
            throw new Exception("Problem with TapConnector, see ", e);
        }

    }

//    /**
//     * Sends a synchronous query
//     *
//     * @return VOTableReader, or null if HTTP response code is not 200 or other
//     * connection problem
//     * @throws Exception
//     */
//    protected VOTableReader runSynchronousQuery(String query, String format) throws Exception {
//        InputStream is;
//        VOTableReader votable = null;
//        try {
//            String httpUrl = TAP_URL + "/tap/sync?" + String.format("REQUEST=doQuery&LANG=ADQL&FORMAT=%s&QUERY=%s",
//                    format, URLEncoder.encode((query), ENC));
//            HttpURLConnection conn = getHttpURLConnection(httpUrl);
//            conn.connect();
//            is = getInputStreamOf(conn);
//            if (is != null) {
//                votable = new VOTableReader(is);
//                is.close();
//            }
//            conn.disconnect();
//        } catch (KeyManagementException | NoSuchAlgorithmException | IOException e) {
//            throw new Exception("[Cause: TapConnector] ", e);
//        }
//        return votable;
//    }

//    /**
//     * Sends a synchronous query
//     *
//     * @param query
//     * @return VOTableReader or null if the query is not refused by the Tap
//     * service
//     * @throws Exception
//     */
//    public VOTableReader runSynchronousQuery(String query) throws Exception {
//        return runSynchronousQuery(query, "votable");
//    }

//    /**
//     * Sends an asynchronous query
//     *
//     * @param query
//     * @return jobId to the corresponded query if it is accepted, otherwise
//     * null.
//     * @throws Exception
//     */
//    protected String runAsynchronousJob(String query, String format) throws Exception {
//        String jobId = null;
//        try {
//            String httpUrl = TAP_URL + "/tap/async?" + String.format(
//                    "PHASE=run&REQUEST=doQuery&LANG=ADQL&FORMAT=%s&QUERY=%s", format, URLEncoder.encode((query), ENC));
//            HttpURLConnection conn = getHttpURLConnection(httpUrl);
//            conn.setRequestMethod("POST");
//            conn.connect();
//            if (getResponseCode(conn) == HttpURLConnection.HTTP_OK)
//                jobId = getJobId(conn);
//            conn.disconnect();
//        } catch (KeyManagementException | NoSuchAlgorithmException | IOException e) {
//            throw new Exception("[Cause: TapConnector] ", e);
//        }
//        return jobId;
//
//    }

//    /**
//     * Sends an asynchronous query
//     *
//     * @param query
//     * @return jobId to the corresponded query if it is submitted, otherwise
//     * null.
//     * @throws Exception
//     */
//    public String runAsynchronousJob(String query) throws Exception {
//        return runAsynchronousJob(query, "votable");
//    }

//    public void executeJobNonGACSTap(String jobId) throws ValidationException {
//        try {
//            String httpUrl = TAP_URL + String.format("/tap/async/%s/phase", jobId);
//            HttpURLConnection conn = getHttpURLConnection(httpUrl);
//            conn.setRequestMethod("POST");
//            conn.setDoOutput(true);
//            DataOutputStream printOut = new DataOutputStream(conn.getOutputStream());
//            printOut.writeBytes("PHASE=RUN");
//            printOut.flush();
//            printOut.close();
//            conn.connect();
//            getResponseCode(conn);
//            conn.disconnect();
//        } catch (KeyManagementException | NoSuchAlgorithmException | IOException e) {
//            throw new ValidationException("[Cause: TapConnector] ", e);
//        }
//    }

//    /**
//     * Gets the jobId of the asynchronous query
//     *
//     * @param connection
//     * @return
//     * @throws IOException
//     */
//    String getJobId(HttpURLConnection connection) throws IOException, Exception {
//        InputStream is = connection.getInputStream();
//        JobSummary js;
//        JAXBContext context;
//        Unmarshaller um;
//        try {
//            context = JAXBContext.newInstance(JobSummary.class);
//            um = context.createUnmarshaller();
//            js = (JobSummary) ((JAXBElement<?>) um.unmarshal(is)).getValue();
//            is.close();
//        } catch (JAXBException | IOException e) {
//            throw new Exception("Cannot parse job summary xml, see ", e);
//        }
//        return js.getJobId();
//    }

//    /**
//     * Updates the asynchronous job's current phase
//     *
//     * <p>This method checks the phase every {@code millis} millisecond until it
//     * reaches one of the ending phases (COMPLETED, ERROR, or ABORTED)</p>
//     *
//     * @param jobId
//     * @param millis
//     * @return
//     * @throws Exception
//     */
//    public String updateJobPhase(String jobId, long millis) throws Exception {
//        String phase;
//        int noSleep = 0;
//        do {
//            if (noSleep != 0)
//                Thread.sleep(millis);
//            phase = getJobPhase(jobId);
//            LOG.info("{}: {}", jobId, phase);
//            if ((phase.equals(AsyncQueryPhases.ERROR.toString()))
//                    || (phase.equals(AsyncQueryPhases.ABORTED.toString()))) {
//                LOG.error("The query ends in {}, see {}", phase, getJobError(jobId));
//                break;
//            }
//            noSleep++;
//        } while (!phase.equals(AsyncQueryPhases.COMPLETED.toString()));
//        return phase;
//    }

    /**
     * Gets the asynchronous job's current phase
     *
     * @param jobId
     * @return String otherwise null due to a connection problem
     * @throws Exception
     */
    String getJobPhase(String jobId) throws Exception {
        String httpUrl = TAP_URL + "/tap/async/" + jobId + "/phase";
        String phase = null;
        try {
            HttpURLConnection conn = getHttpURLConnection(httpUrl);
            conn.connect();
            if (getResponseCode(conn) == HttpURLConnection.HTTP_OK) {
                try (InputStream is = conn.getInputStream();
                     BufferedReader br = new BufferedReader(new InputStreamReader(is))) {
                    phase = br.lines().findFirst().get();
                }
            }
            conn.disconnect();
        } catch (KeyManagementException | NoSuchAlgorithmException | IOException e) {
            throw new Exception("Problem with TapConnector, see ", e);
        }
        return phase;
    }

//    /**
//     * Gets the asynchronous job's error message
//     *
//     * <p>This method is invokeed when the job ends in ERROR.</p>
//     *
//     * @param jobId
//     * @return String otherwise null if no error message or due to a connection
//     * problem
//     * @throws Exception
//     */
//    public String getJobError(String jobId) throws Exception {
//        String httpUrl = TAP_URL + "/tap/async/" + jobId + "/error";
//        String errorMessage = null;
//        try {
//            HttpURLConnection conn = setHttpURLConnection(httpUrl);
//            conn.connect();
//            if (getResponseCode(conn) == HttpURLConnection.HTTP_OK) {
//                JAXBContext context = JAXBContext.newInstance(VOTABLE.class);
//                Unmarshaller um = context.createUnmarshaller();
//                VOTABLE vt = (VOTABLE) um.unmarshal(conn.getInputStream());
//                errorMessage = vt.getRESOURCE().get(0).getINFO().stream()
//                        .filter(i -> i.getName().equals("QUERY_STATUS")).map(i -> i.getValue()).findFirst().get();
//                if (errorMessage != null)
//                    errorMessage = errorMessage.trim();
//            }
//            conn.disconnect();
//        } catch (KeyManagementException | NoSuchAlgorithmException | IOException | JAXBException e) {
//            throw new Exception("Problem with TapConnector, see ", e);
//        }
//        return errorMessage;
//    }

//    /**
//     * Gets the asynchronous job's result
//     *
//     * <p>This method is invoked when the job ends in COMPLETED.</p>
//     *
//     * @param jobId
//     * @return VOTableReader otherwise null
//     * @throws Exception
//     */
//    public VOTableReader getJobResult(String jobId) throws Exception {
//        String httpUrl = TAP_URL + "/tap/async/" + jobId + "/results/result";
//        InputStream is;
//        VOTableReader votable = null;
//        try {
//            HttpURLConnection conn = getHttpURLConnection(httpUrl);
//            conn.connect();
//            if (getResponseCode(conn) == HttpURLConnection.HTTP_OK) {
//                is = conn.getInputStream();
//                votable = new VOTableReader(is);
//                is.close();
//            }
//            conn.disconnect();
//        } catch (KeyManagementException | NoSuchAlgorithmException | IOException e) {
//            throw new Exception("Problem with TapConnector, see ", e);
//        }
//        return votable;
//    }

    /**
     * Deletes an asynchronous job
     *
     * @param jobId
     * @return the HTTP response code, otherwise null
     * @throws Exception
     */
    public Integer deleteJob(String jobId) throws Exception {
        Integer responseCode = null;
        String httpUrl = TAP_URL + "/tap/async/" + jobId;
        try {
            HttpURLConnection conn = getHttpURLConnection(httpUrl);
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            DataOutputStream printOut = new DataOutputStream(conn.getOutputStream());
            printOut.writeBytes("ACTION=DELETE");
            printOut.flush();
            printOut.close();
            conn.connect();
            responseCode = getResponseCode(conn);
            if ((int) responseCode == HttpURLConnection.HTTP_OK)
                LOG.info("Job {} is deleted.", jobId);
            conn.disconnect();
        } catch (KeyManagementException | NoSuchAlgorithmException | IOException e) {
            throw new Exception("Problem with TapConnector, see ", e);
        }
        return responseCode;
    }

    void addFormField(DataOutputStream printOut, String name, String value) throws IOException {
        StringBuilder content = new StringBuilder();
        content.append("--" + BOUNDARY).append(LINE_FEED);
        content.append("Content-Disposition: form-data; name=\"" + name + "\"").append(LINE_FEED);
        content.append("Content-Type: text/plain; charset=" + ENC).append(LINE_FEED);
        content.append(LINE_FEED);
        content.append(value).append(LINE_FEED);
        printOut.writeBytes(content.toString());
        printOut.flush();
    }

    void addFileField(HttpURLConnection connection, DataOutputStream printOut, String fieldName, File uploadFile)
            throws IOException {
        StringBuilder content = new StringBuilder();
        content.append("--" + BOUNDARY).append(LINE_FEED);
        content.append(
                "Content-Disposition: form-data; name=\"" + fieldName + "\"; filename=\"" + uploadFile.getName() + "\"")
                .append(LINE_FEED);
        content.append("Content-Type: " + URLConnection.guessContentTypeFromName(uploadFile.getName()))
                .append(LINE_FEED);
        content.append("Content-Transfer-Encoding: binary").append(LINE_FEED);
        content.append(LINE_FEED);
        printOut.writeBytes(content.toString());
        printOut.flush();

        FileInputStream inputStream = new FileInputStream(uploadFile);
        byte[] buffer = new byte[4096];
        int bytesRead = -1;
        OutputStream outputstream = connection.getOutputStream();
        while ((bytesRead = inputStream.read(buffer)) != -1) {
            outputstream.write(buffer, 0, bytesRead);
        }
        outputstream.flush();
        inputStream.close();

        StringBuilder ending = new StringBuilder();
        ending.append(LINE_FEED);
        ending.append("--" + BOUNDARY + "--").append(LINE_FEED);
        printOut.writeBytes(ending.toString());
        printOut.flush();
    }

    /**
     * Uploads personal table to GACS (TAP+)
     *
     * @param tableName The name to be used by the upload table
     * @param raName    The name of R.A. column in the uploaded votable
     * @param decName   The name of Dec. column in the uploaded votable
     * @param tableFile The votable file to be uploaded
     * @return the HTTP response code, otherwise null
     * @throws Exception
     */
    public Integer uploadTable(String tableName, String raName, String decName, File tableFile)
            throws Exception {
        Integer responseCode = null;
        String httpsUrl = TAP_URL + "/Upload";
        try {
            HttpURLConnection conn = getHttpURLConnection(httpsUrl);
            if (conn instanceof HttpsURLConnection) {
                conn.setRequestMethod("POST");
                conn.setDoOutput(true);
                conn.setRequestProperty("User-Agent", "CodeJava Agent");
                conn.setRequestProperty("Content-Type", "multipart/form-data" + " ;boundary=" + BOUNDARY);
                DataOutputStream printOut = new DataOutputStream(conn.getOutputStream());
                addFormField(printOut, "TABLE_NAME", tableName);
                addFormField(printOut, "RACOL", raName);
                addFormField(printOut, "DECCOL", decName);
                addFileField(conn, printOut, "FILE", tableFile);
                printOut.close();
                conn.connect();
                responseCode = getResponseCode(conn);
                conn.disconnect();
            } else
                LOG.warn("Uploading personal table is only supported by secure HTTP.");
        } catch (KeyManagementException | NoSuchAlgorithmException | IOException e) {
            throw new Exception("[Cause: TapConnector] ", e);
        }
        return responseCode;
    }

    /**
     * Uploads personal table to GACS (TAP+)
     *
     * @param tableName The name to be used by the upload table
     * @param tableFile The votable file to be uploaded
     * @return the HTTP response code, otherwise null
     * @throws Exception
     */
    public Integer uploadTable(String tableName, File tableFile) throws Exception {
        Integer responseCode = null;
        String httpsUrl = TAP_URL + "/Upload";
        try {
            HttpURLConnection conn = getHttpURLConnection(httpsUrl);
            if (conn instanceof HttpsURLConnection) {
                conn.setRequestMethod("POST");
                conn.setDoOutput(true);
                conn.setRequestProperty("User-Agent", "CodeJava Agent");
                conn.setRequestProperty("Content-Type", "multipart/form-data" + " ;boundary=" + BOUNDARY);
                DataOutputStream printOut = new DataOutputStream(conn.getOutputStream());
                addFormField(printOut, "TABLE_NAME", tableName);
                addFileField(conn, printOut, "FILE", tableFile);
                printOut.close();
                conn.connect();
                responseCode = getResponseCode(conn);
                conn.disconnect();
            } else
                LOG.warn("Uploading personal table is only supported by secure HTTP.");
        } catch (KeyManagementException | NoSuchAlgorithmException | IOException e) {
            throw new Exception("[Cause: TapConnector] ", e);
        }
        return responseCode;
    }

    /**
     * Delete personal table at GACS (TAP+)
     *
     * @param tableName The name of the table to be deleted. Limit to the tables owned
     *                  by the same person.
     * @return the HTTP response code, otherwise null
     * @throws Exception
     */
    public Integer deleteTable(String tableName) throws Exception {
        Integer responseCode = null;
        String httpsUrl = TAP_URL + "/Upload";
        try {
            HttpURLConnection conn = getHttpURLConnection(httpsUrl);
            if (conn instanceof HttpsURLConnection) {
                conn.setRequestMethod("POST");
                conn.setDoOutput(true);
                conn.setRequestProperty("User-Agent", "CodeJava Agent");
                conn.setRequestProperty("Content-Type", "multipart/form-data" + " ;boundary=" + BOUNDARY);
                DataOutputStream printOut = new DataOutputStream(conn.getOutputStream());
                addFormField(printOut, "TABLE_NAME", tableName);
                addFormField(printOut, "DELETE", "true");
                printOut.writeBytes("--" + BOUNDARY + "--" + LINE_FEED);
                printOut.flush();
                printOut.close();
                conn.connect();
                responseCode = getResponseCode(conn);
                conn.disconnect();
            } else
                LOG.warn("Deleting personal table is only supported by secure HTTP.");
        } catch (KeyManagementException | NoSuchAlgorithmException | IOException e) {
            throw new Exception("[Cause: TapConnector] ", e);
        }
        return responseCode;
    }

//    /**
//     * Get the Shared Group Id (TAP+)
//     *
//     * @return String or null if the userGroup is not part of GACS groups
//     * @throws Exception
//     */
//    String getSharedGroupId(String userGroup) throws Exception {
//        String httpsUrl = TAP_URL + "/tap/share?action=GetGroups";
//        String groupId = null;
//        try {
//            HttpURLConnection conn = getHttpURLConnection(httpsUrl);
//            if (conn instanceof HttpsURLConnection) {
//                conn.connect();
//                SharedGroups sg;
//                JAXBContext context = JAXBContext.newInstance(SharedGroups.class);
//                Unmarshaller um = context.createUnmarshaller();
//                sg = (SharedGroups) um.unmarshal(conn.getInputStream());
//                groupId = sg.getSharedGroup().stream().filter(g -> g.getTitle().equals(userGroup)).map(g -> g.getId())
//                        .findFirst().get();
//                conn.disconnect();
//            } else {
//                LOG.warn("This method is only supported by secure HTTP.");
//            }
//        } catch (IOException | JAXBException | KeyManagementException | NoSuchAlgorithmException e) {
//            throw new Exception("[Cause: TapConnector] ", e);
//        }
//        return groupId;
//    }

//    /**
//     * Share personal table to users/groups (TAP+)
//     *
//     * @param tableName
//     * @param userGroup
//     * @throws Exception
//     */
//    public void shareTable(String tableName, String userGroup) throws Exception {
//        String sharedGroupId = getSharedGroupId(userGroup);
//        if (sharedGroupId != null) {
//            String httpsUrl = TAP_URL + "/tap/share";
//            try {
//                HttpURLConnection conn = getHttpURLConnection(httpsUrl);
//                if (conn instanceof HttpsURLConnection) {
//                    conn.setRequestMethod("POST");
//                    conn.setDoOutput(true);
//                    conn.setRequestProperty("Content-Type", "");
//                    DataOutputStream printOut = new DataOutputStream(conn.getOutputStream());
//                    String tableToShare = String.format("user_%s.%s", credentials.getUserName(), tableName);
//                    String shareAction = String.format(
//                            "CreateOrUpdateItem&resource_type=0&title=%s&description=description&items_list=%s|Group|Read",
//                            tableToShare, sharedGroupId);
//                    printOut.writeBytes(String.format("action=%s", shareAction));
//                    printOut.flush();
//                    printOut.close();
//                    conn.connect();
//                    getResponseCode(conn);
//                    conn.disconnect();
//                }
//            } catch (IOException | KeyManagementException | NoSuchAlgorithmException e) {
//                throw new Exception("[Cause: TapConnector] ", e);
//            }
//        }
//    }

    /**
     * Utilise method
     *
     * @param is   an InputStream object
     * @param file a File object to save the data
     */
    public static void saveAsFile(InputStream is, File file) {
        BufferedWriter bw;
        try {
            bw = new BufferedWriter(new FileWriter(file));
            BufferedReader bin = new BufferedReader(new InputStreamReader(is));
            bin.lines().forEach(l -> {
                try {
                    bw.write(l);
                } catch (Exception e) {
                    LOG.error("Cannot write to {}, see ", file.getName(), e);
                }
            });
            bin.close();
            bw.close();
        } catch (IOException e) {
            LOG.error("Cannot open {} to write, see ", file.getName(), e);
        }
    }
}

package ics.astro.tap.internal;

import net.xml.ivoa.uws.JobSummary;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.Unmarshaller;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Table Access Protocol interface
 */
public interface Tap {

    String table = "tables";
    String sync = "sync";
    String async = "async";
    String async_phase = "async/%d/phase";
    String async_result = "async/%d/results/result";
    String async_error = "async/%d/error";
    String ENC = "UTF-8";

    /**
     * Gets the HTTP Connection
     *
     * @param httpUrl the TAP service url
     * @return HttpURLConnection object or null
     * @throws IOException
     */
    default HttpURLConnection getHttpURLConnection(String httpUrl) throws IOException {
        URL url = new URL(httpUrl);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setUseCaches(false);
        return conn;
    }

    /**
     * Gets the HTTP response code
     *
     * @param conn the TAP service connection
     * @return reponse code
     * @throws IOException
     */
    default int getResponseCode(HttpURLConnection conn) throws IOException {
        return conn.getResponseCode();
    }

    /**
     * Gets the input stream of the connection
     * @param conn the Tap service connection
     * @return the input stream of the service
     * @throws IOException
     */
    default InputStream getInputStream(HttpURLConnection conn) throws IOException {
        return conn.getInputStream();
    }

    /**
     * Gets the available tables on the TAP service
     * @return the input stream of the tables list in xml format
     * @throws IOException
     */
    InputStream getAvailableTables() throws IOException;
    InputStream runSynchronousQuery(String query, String format) throws IOException;
    InputStream runAsynchronousJob(String query, String format) throws IOException;
    InputStream getJobList();
    InputStream getJobSummary(String jobId);

    /**
     * Gets the job identifier of an asynchronous query
     * @param conn the asynchronous query connection
     * @return
     * @throws Exception
     */
    String getJobId(HttpURLConnection conn) throws Exception;

    String updateJobPhase(String jobId, long millis) throws InterruptedException, IOException;
    String getJobPhase(String jobId) throws IOException;
    String getJobError(String jobId) throws IOException;
    Integer deleteJob(String jobId);
    InputStream getJobResult(String jobId) throws IOException;
}

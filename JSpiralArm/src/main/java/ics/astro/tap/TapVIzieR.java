package ics.astro.tap;

import ics.astro.tap.internal.AsyncQueryPhases;
import ics.astro.tap.internal.Tap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URLEncoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TapVIzieR implements Tap {

    private final String TAP_URL = TapService.VIZIER.getTAPURL();

    private static final Logger logger = LoggerFactory.getLogger(TapVIzieR.class);

    @Override
    public InputStream getAvailableTables() throws IOException {
        String httpUrl = String.format("%s%s", TAP_URL, table);
        logger.debug("{}", httpUrl);
        HttpURLConnection conn = getHttpURLConnection(httpUrl);
        conn.connect();
        int code = getResponseCode(conn);
        logger.debug("response code: {}", code);
        if (code == HttpURLConnection.HTTP_OK)
            return getInputStream(conn);
        else
            return null;
    }

    @Override
    public InputStream runSynchronousQuery(String query, String format) throws IOException {
        String httpUrl = String.format("%s%s?REQUEST=doQuery&LANG=ADQL&FORMAT=%s&QUERY=%s",
                TAP_URL, sync, format, URLEncoder.encode((query), ENC));
        logger.debug("{}", httpUrl);
        HttpURLConnection conn = getHttpURLConnection(httpUrl);
        conn.connect();
        int code = getResponseCode(conn);
        logger.debug("response code: {}", code);
        if (code == HttpURLConnection.HTTP_OK)
            return getInputStream(conn);
        else
            return null;
    }

    @Override
    public InputStream runAsynchronousJob(String query, String format) throws IOException {
        String httpUrl = String.format("%s%s?PHASE=run&REQUEST=doQuery&LANG=ADQL&FORMAT=%s&QUERY=%s",
                TAP_URL, async,format, URLEncoder.encode((query), ENC));
        logger.debug("{}", httpUrl);
        HttpURLConnection conn = getHttpURLConnection(httpUrl);
        conn.setRequestMethod("POST");
        conn.connect();
        int code = getResponseCode(conn);
        logger.debug("response code: {}", code);
        if (code == HttpURLConnection.HTTP_OK)
            return getInputStream(conn);
        else
            return null;
    }

    @Override
    public InputStream getJobList() {
        return null;
    }

    @Override
    public InputStream getJobSummary(String jobId) {
        return null;
    }

    @Override
    public String getJobId(HttpURLConnection conn) throws Exception {
        return null;
    }

    /**
     * Gets the job identifier of an asynchronous query
     * @param inputStream of the asynchronous query
     * @return
     * @throws Exception
     */
    public String getJobId(InputStream inputStream) throws Exception {
//        String template = "http://tapvizier.u-strasbg.fr:80/TAPVizieR/tap/async/(\\d*)/results/result";
        String template = "<jobId>(\\d*)</jobId>";
        Pattern pattern = Pattern.compile(template);
        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
        String line;
        String jobId = null;
        while ((line = br.readLine()) != null) {
            Matcher m = pattern.matcher(line);
            if (m.find())
                jobId = m.group(1);
        }
        br.close();
        inputStream.close();
        if (jobId != null)
            logger.info("JobId: {}", jobId);
        return jobId;
    }

    @Override
    public String updateJobPhase(String jobId, long millis) throws InterruptedException, IOException {
        String phase;
        int noSleep = 0;
        do {
            if (noSleep != 0)
                Thread.currentThread().sleep(millis);
            phase = getJobPhase(jobId);
            if ((phase.equals(AsyncQueryPhases.ERROR.name()))
                    || (phase.equals(AsyncQueryPhases.ABORTED.name()))) {
                logger.error("The query ends in {}, see {}", phase, getJobError(jobId));
                break;
            }
            noSleep++;
        } while (!phase.equals(AsyncQueryPhases.COMPLETED.name()));
        logger.info("Job {} final phase: {}", jobId, phase);
        return phase;
    }

    @Override
    public String getJobPhase(String jobId) throws IOException {
        String httpUrl = String.format("%s%s/%s/phase",TAP_URL, async, jobId);
        HttpURLConnection conn = getHttpURLConnection(httpUrl);
        conn.connect();
        int code = getResponseCode(conn);
        logger.debug("response code: {}", code);
        if (code == HttpURLConnection.HTTP_OK) {
            InputStream is = conn.getInputStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            String phase = br.lines().findFirst().get();
            br.close();
            logger.debug("Job {} current phase: {}", jobId,  phase);
            return phase;
        } else
            return null;
    }

    @Override
    public String getJobError(String jobId) throws IOException {
        String httpUrl = String.format("%s%s/%s/error",TAP_URL, async, jobId);
        HttpURLConnection conn = getHttpURLConnection(httpUrl);
        conn.connect();
        int code = getResponseCode(conn);
        logger.debug("response code: {}", code);
        if (code == HttpURLConnection.HTTP_OK) {
            InputStream is = conn.getInputStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            br.lines().forEach(System.out::println);
            br.close();
            return null;
        } else
            return null;
    }

    @Override
    public Integer deleteJob(String jobId) {
        return null;
    }

    @Override
    public InputStream getJobResult(String jobId) throws IOException {
        String httpUrl = String.format("%s%s/%s/results/result", TAP_URL, async, jobId);
        HttpURLConnection conn = getHttpURLConnection(httpUrl);
        conn.connect();
        int code = getResponseCode(conn);
        logger.debug("response code: {}", code);
        if (code == HttpURLConnection.HTTP_OK) {
            return conn.getInputStream();
        } else
            return null;
    }
}

package boomi_file_upload;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

@RestController
@RequestMapping("/api")
public class TransferController {

    private static final Logger log =
            LoggerFactory.getLogger(TransferController.class);

    private static final long MIB = 1024L * 1024L;

    private static final String CONTENT_API =
            "/api/Files/1.0/Content";

    private static final String TRANSFER_ID_HEADER =
            "X-Thru-TransferId";

    @Value("${boomi.aft.base-url}")
    private String baseUrl;

    @Value("${boomi.aft.api-key-header:ApiKey}")
    private String apiKeyHeader;

    @Value("${boomi.aft.api-key}")
    private String apiKey;

    @Value("${boomi.aft.endpoint-path.doxis}")
    private String doxisEndpointPath;

    @Value("${boomi.aft.endpoint-path.sftp}")
    private String sftpEndpointPath;

    @Value("${boomi.aft.remote-path:/}")
    private String remotePath;

    @Value("${boomi.aft.chunk-size-bytes:8388608}")
    private long configuredChunkSize;

    @Value("${boomi.aft.connect-timeout-seconds:15}")
    private long connectTimeoutSeconds;

    @Value("${boomi.aft.read-timeout-seconds:180}")
    private long readTimeoutSeconds;

    /*
     * Upload endpoint used by index.html:
     * POST /api/transfers
     *
     * Form fields:
     *   file        - selected file
     *   destination - SFTP or DOXIS
     */
    @PostMapping(
            value = "/transfers",
            consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<Map<String, Object>> upload(
            @RequestParam("file") MultipartFile file,
            @RequestParam("destination") String destination) {

        String requestReference = UUID.randomUUID().toString();

        try {
            if (file == null || file.isEmpty()) {
                return errorResponse(
                        HttpStatus.BAD_REQUEST,
                        "Please select a non-empty file.");
            }

            String uploadFilename = file.getOriginalFilename();

            if (uploadFilename == null || uploadFilename.isBlank()) {
                uploadFilename = "upload.bin";
            }

            // Avoid unsafe filename characters in multipart headers.
            uploadFilename = uploadFilename
                    .replace("\\", "_")
                    .replace("/", "_")
                    .replace("\"", "_")
                    .replace("\r", "_")
                    .replace("\n", "_");

            String selectedDestination = destination == null
                    ? ""
                    : destination.trim();

            String endpointPath;

            if ("SFTP".equalsIgnoreCase(selectedDestination)) {
                endpointPath = sftpEndpointPath;
                selectedDestination = "SFTP";
            } else if ("DOXIS".equalsIgnoreCase(selectedDestination)) {
                endpointPath = doxisEndpointPath;
                selectedDestination = "DOXIS";
            } else {
                return errorResponse(
                        HttpStatus.BAD_REQUEST,
                        "Unsupported destination: " + selectedDestination);
            }

            long fileSize = file.getSize();

            log.info(
                    "[{}] Upload started. File={}, size={} bytes ({} MiB), destination={}, endpoint={}",
                    requestReference,
                    uploadFilename,
                    fileSize,
                    String.format("%.2f", fileSize / (double) MIB),
                    selectedDestination,
                    endpointPath);

            HttpClient httpClient = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(
                            connectTimeoutSeconds))
                    .build();

            /*
             * Construct the metadata JSON.
             * No Jackson dependency is required.
             */
            String metadataJson = buildMetadataJson(
                    endpointPath,
                    uploadFilename,
                    remotePath,
                    fileSize);

            /*
             * STEP 1/4: Pre-flight.
             *
             * Java HttpClient explicitly sends a body with OPTIONS.
             */
            log.info(
                    "[{}] Step 1/4: Calling AFT pre-flight",
                    requestReference);

            HttpRequest preflightRequest = HttpRequest.newBuilder()
                    .uri(URI.create(
                            joinUrl(baseUrl, CONTENT_API)))
                    .timeout(Duration.ofSeconds(readTimeoutSeconds))
                    .header(apiKeyHeader, apiKey)
                    .header("Content-Type", "application/json")
                    .method(
                            "OPTIONS",
                            HttpRequest.BodyPublishers.ofString(
                                    metadataJson,
                                    StandardCharsets.UTF_8))
                    .build();

            HttpResponse<String> preflightResponse =
                    httpClient.send(
                            preflightRequest,
                            HttpResponse.BodyHandlers.ofString(
                                    StandardCharsets.UTF_8));

            log.info(
                    "[{}] Pre-flight HTTP status: {}",
                    requestReference,
                    preflightResponse.statusCode());

            if (!isSuccessful(preflightResponse.statusCode())) {
                throw new IOException(
                        "AFT pre-flight failed. HTTP "
                                + preflightResponse.statusCode()
                                + ": " + preflightResponse.body());
            }

            String preflightBody = preflightResponse.body();

            long maximumChunkSize = extractLong(
                    preflightBody,
                    "uploadMaxChunkSize");

            String chunkedUploadPath = extractString(
                    preflightBody,
                    "uploadChunkedFileUrl");

            log.info(
                    "[{}] AFT pre-flight successful. Maximum chunk size={} bytes ({} MiB)",
                    requestReference,
                    maximumChunkSize,
                    String.format(
                            "%.2f",
                            maximumChunkSize / (double) MIB));

            if (maximumChunkSize <= 0) {
                throw new IOException(
                        "AFT returned an invalid maximum chunk size.");
            }

            if (chunkedUploadPath == null
                    || chunkedUploadPath.isBlank()) {
                throw new IOException(
                        "AFT pre-flight did not return uploadChunkedFileUrl.");
            }

            /*
             * Use the session API confirmed in your Postman tests.
             *
             * The same session/transfer ID is reused for all chunks.
             * Commit is called once after all chunks have uploaded.
             */
            long chunkSize = Math.min(
                    configuredChunkSize,
                    maximumChunkSize);

            if (chunkSize <= 0 || chunkSize > Integer.MAX_VALUE) {
                throw new IOException(
                        "Invalid configured chunk size: " + chunkSize);
            }

            log.info(
                    "[{}] Configured chunk size={} bytes ({} MiB)",
                    requestReference,
                    chunkSize,
                    String.format("%.2f", chunkSize / (double) MIB));

            /*
             * STEP 2/4: Create the upload session.
             */
            log.info(
                    "[{}] Step 2/4: Creating AFT upload session",
                    requestReference);

            HttpRequest sessionRequest = HttpRequest.newBuilder()
                    .uri(URI.create(
                            joinUrl(baseUrl, chunkedUploadPath)))
                    .timeout(Duration.ofSeconds(readTimeoutSeconds))
                    .header(apiKeyHeader, apiKey)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(
                            metadataJson,
                            StandardCharsets.UTF_8))
                    .build();

            HttpResponse<String> sessionResponse =
                    httpClient.send(
                            sessionRequest,
                            HttpResponse.BodyHandlers.ofString(
                                    StandardCharsets.UTF_8));

            log.info(
                    "[{}] Session creation HTTP status: {}",
                    requestReference,
                    sessionResponse.statusCode());

            if (!isSuccessful(sessionResponse.statusCode())) {
                throw new IOException(
                        "AFT session creation failed. HTTP "
                                + sessionResponse.statusCode()
                                + ": " + sessionResponse.body());
            }

            String transferId = sessionResponse.headers()
                    .firstValue(TRANSFER_ID_HEADER)
                    .orElse(null);

            if (transferId == null || transferId.isBlank()) {
                throw new IOException(
                        "AFT did not return the "
                                + TRANSFER_ID_HEADER
                                + " response header.");
            }

            log.info(
                    "[{}] Upload session created. Transfer ID={}",
                    requestReference,
                    transferId);

            String sessionUrl = joinUrl(
                    baseUrl,
                    chunkedUploadPath)
                    + encodePathSegment(transferId);

            /*
             * STEP 3/4: Upload the file in chunks.
             */
            log.info(
                    "[{}] Step 3/4: Starting chunk upload",
                    requestReference);

            long bytesUploaded = 0;
            int sequenceNumber = 0;

            try (InputStream inputStream = file.getInputStream()) {

                while (bytesUploaded < fileSize) {

                    int bytesToRead = (int) Math.min(
                            chunkSize,
                            fileSize - bytesUploaded);

                    byte[] chunk = inputStream.readNBytes(bytesToRead);

                    if (chunk.length == 0) {
                        throw new IOException(
                                "Unexpected end of file while reading "
                                        + uploadFilename);
                    }

                    long chunkStart = bytesUploaded;
                    long chunkEnd = chunkStart + chunk.length - 1;

                    double progressBefore = fileSize == 0
                            ? 0
                            : bytesUploaded * 100.0 / fileSize;

                    log.info(
                            "[{}] Uploading chunk {}: bytes {}-{}; chunk size={} bytes; progress before upload={}%",
                            requestReference,
                            sequenceNumber,
                            chunkStart,
                            chunkEnd,
                            chunk.length,
                            String.format("%.1f", progressBefore));

                    String boundary = "----AFT-" + UUID.randomUUID();

                    byte[] multipartBody = buildChunkMultipart(
                            boundary,
                            sequenceNumber,
                            uploadFilename,
                            chunk);

                    HttpRequest chunkRequest = HttpRequest.newBuilder()
                            .uri(URI.create(sessionUrl))
                            .timeout(Duration.ofSeconds(readTimeoutSeconds))
                            .header(apiKeyHeader, apiKey)
                            .header(
                                    "Content-Type",
                                    "multipart/form-data; boundary=" + boundary)
                            .PUT(HttpRequest.BodyPublishers.ofByteArray(
                                    multipartBody))
                            .build();

                    HttpResponse<String> chunkResponse =
                            httpClient.send(
                                    chunkRequest,
                                    HttpResponse.BodyHandlers.ofString(
                                            StandardCharsets.UTF_8));

                    log.info(
                            "[{}] Chunk {} response HTTP status: {}",
                            requestReference,
                            sequenceNumber,
                            chunkResponse.statusCode());

                    if (!isSuccessful(chunkResponse.statusCode())) {
                        throw new IOException(
                                "Chunk " + sequenceNumber
                                        + " failed. HTTP "
                                        + chunkResponse.statusCode()
                                        + ": " + chunkResponse.body());
                    }

                    bytesUploaded += chunk.length;

                    double progress = bytesUploaded * 100.0 / fileSize;

                    log.info(
                            "[{}] Chunk {} uploaded successfully. Total uploaded={} / {} bytes ({}%)",
                            requestReference,
                            sequenceNumber,
                            bytesUploaded,
                            fileSize,
                            String.format("%.1f", progress));

                    sequenceNumber++;
                }
            }

            log.info(
                    "[{}] All {} chunks uploaded. Total bytes={}",
                    requestReference,
                    sequenceNumber,
                    bytesUploaded);

            /*
             * STEP 4/4: Commit the upload session once.
             */
            log.info(
                    "[{}] Step 4/4: Committing upload session {}",
                    requestReference,
                    transferId);

            HttpRequest commitRequest = HttpRequest.newBuilder()
                    .uri(URI.create(sessionUrl + "/Commit"))
                    .timeout(Duration.ofSeconds(readTimeoutSeconds))
                    .header(apiKeyHeader, apiKey)
                    .POST(HttpRequest.BodyPublishers.noBody())
                    .build();

            HttpResponse<String> commitResponse =
                    httpClient.send(
                            commitRequest,
                            HttpResponse.BodyHandlers.ofString(
                                    StandardCharsets.UTF_8));

            log.info(
                    "[{}] Commit HTTP status: {}; response={}",
                    requestReference,
                    commitResponse.statusCode(),
                    commitResponse.body());

            if (!isSuccessful(commitResponse.statusCode())) {
                throw new IOException(
                        "AFT commit failed. HTTP "
                                + commitResponse.statusCode()
                                + ": " + commitResponse.body());
            }

            log.info(
                    "[{}] AFT accepted upload. File={}, transferId={}, destination={}",
                    requestReference,
                    uploadFilename,
                    transferId,
                    selectedDestination);

            Map<String, Object> result = new LinkedHashMap<>();
            result.put("success", true);
            result.put("status", "ACCEPTED");
            result.put("message",
                    "Boomi AFT accepted the upload for processing. "
                            + "Final delivery has not yet been confirmed.");
            result.put("requestReference", requestReference);
            result.put("fileName", uploadFilename);
            result.put("sizeBytes", fileSize);
            result.put("destination", selectedDestination);
            result.put("transferId", transferId);
            result.put("chunksUploaded", sequenceNumber);

            return ResponseEntity.accepted().body(result);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            log.error(
                    "[{}] Upload interrupted",
                    requestReference,
                    e);

            return errorResponse(
                    HttpStatus.BAD_GATEWAY,
                    "Upload interrupted. Reference: " + requestReference);

        } catch (Exception e) {
            log.error(
                    "[{}] Upload failed: {}",
                    requestReference,
                    e.getMessage(),
                    e);

            return errorResponse(
                    HttpStatus.BAD_GATEWAY,
                    "Upload failed: " + e.getMessage()
                            + " (reference: " + requestReference + ")");
        }
    }

    /*
     * Build the JSON metadata expected by the AFT Content API.
     */
    private String buildMetadataJson(
            String endpointPath,
            String filename,
            String path,
            long fileSize) {

        return "{"
                + "\"endpointPath\":\"" + jsonEscape(endpointPath) + "\","
                + "\"attributes\":{"
                + "\"fileName\":\"" + jsonEscape(filename) + "\","
                + "\"path\":\"" + jsonEscape(path) + "\","
                + "\"size\":" + fileSize
                + "}"
                + "}";
    }

    /*
     * Multipart format observed in the working browser/Postman flow:
     *
     * Model_0 = {"SeqNumber":0}
     * Model_1 = binary chunk
     */
    private byte[] buildChunkMultipart(
            String boundary,
            int sequenceNumber,
            String filename,
            byte[] chunk) throws IOException {

        ByteArrayOutputStream output = new ByteArrayOutputStream();

        writeUtf8(
                output,
                "--" + boundary + "\r\n"
                        + "Content-Disposition: form-data; name=\"Model_0\"\r\n"
                        + "Content-Type: text/plain; charset=UTF-8\r\n"
                        + "\r\n"
                        + "{\"SeqNumber\":" + sequenceNumber + "}\r\n");

        writeUtf8(
                output,
                "--" + boundary + "\r\n"
                        + "Content-Disposition: form-data; name=\"Model_1\"; filename=\""
                        + filename + "\"\r\n"
                        + "Content-Type: application/octet-stream\r\n"
                        + "\r\n");

        output.write(chunk);

        writeUtf8(output, "\r\n--" + boundary + "--\r\n");

        return output.toByteArray();
    }

    private void writeUtf8(
            ByteArrayOutputStream output,
            String value) throws IOException {

        output.write(value.getBytes(StandardCharsets.UTF_8));
    }

    private String jsonEscape(String value) {
        if (value == null) {
            return "";
        }

        StringBuilder result = new StringBuilder();

        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);

            switch (c) {
                case '"' -> result.append("\\\"");
                case '\\' -> result.append("\\\\");
                case '\b' -> result.append("\\b");
                case '\f' -> result.append("\\f");
                case '\n' -> result.append("\\n");
                case '\r' -> result.append("\\r");
                case '\t' -> result.append("\\t");
                default -> {
                    if (c < 0x20) {
                        result.append(String.format("\\u%04x", (int) c));
                    } else {
                        result.append(c);
                    }
                }
            }
        }

        return result.toString();
    }

    private long extractLong(String json, String propertyName)
            throws IOException {

        String escapedName = Pattern.quote(propertyName);

        Pattern pattern = Pattern.compile(
                "\"" + escapedName + "\"\\s*:\\s*(\\d+)");

        Matcher matcher = pattern.matcher(json);

        if (!matcher.find()) {
            throw new IOException(
                    "AFT pre-flight response is missing numeric field: "
                            + propertyName);
        }

        try {
            return Long.parseLong(matcher.group(1));
        } catch (NumberFormatException e) {
            throw new IOException(
                    "Invalid numeric value for " + propertyName, e);
        }
    }

    private String extractString(String json, String propertyName)
            throws IOException {

        String escapedName = Pattern.quote(propertyName);

        Pattern pattern = Pattern.compile(
                "\"" + escapedName + "\"\\s*:\\s*\"([^\"]*)\"");

        Matcher matcher = pattern.matcher(json);

        if (!matcher.find()) {
            throw new IOException(
                    "AFT pre-flight response is missing field: "
                            + propertyName);
        }

        return matcher.group(1);
    }

    private boolean isSuccessful(int statusCode) {
        return statusCode >= 200 && statusCode < 300;
    }

    private String joinUrl(String base, String path) {
        String cleanBase = base.replaceAll("/+$", "");
        String cleanPath = path.replaceAll("^/+", "");

        return cleanBase + "/" + cleanPath;
    }

    private String encodePathSegment(String value) {
        // Transfer IDs returned by AFT are expected to be URL-safe.
        return value;
    }

    private ResponseEntity<Map<String, Object>> errorResponse(
            HttpStatus status,
            String message) {

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("success", false);
        result.put("status", status.value());
        result.put("message", message);

        return ResponseEntity.status(status).body(result);
    }
}



package net.momirealms.craftengine.core.pack.host.impl;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Scheduler;
import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import net.momirealms.craftengine.core.pack.host.*;
import net.momirealms.craftengine.core.plugin.CraftEngine;
import net.momirealms.craftengine.core.plugin.config.Config;
import net.momirealms.craftengine.core.plugin.config.ConfigSection;
import net.momirealms.craftengine.core.plugin.config.ConfigValue;
import net.momirealms.craftengine.core.plugin.network.NetWorkUser;
import net.momirealms.craftengine.core.util.HashUtils;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.ProxyConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("unused")
public final class S3Host implements ResourcePackHost {
    static final ResourcePackHostFactory<S3Host> FACTORY = new Factory();
    private final S3AsyncClient s3AsyncClient;
    private final S3Presigner preSigner;
    private final GetObjectPresignRequest presignRequest;
    private final HeadObjectRequest headObjectRequest;
    private final String bucket;
    private final String uploadPath;
    private final String cdnUrl;
    private final boolean disableCalculateSHA256;
    private final Bandwidth limit;
    private final Cache<UUID, Bucket> userRateLimiters;
    private final boolean multipartEnabled;
    private final long partSizeBytes;
    private final int maxConcurrency;


    private S3Host(S3AsyncClient s3AsyncClient, S3Presigner preSigner, GetObjectPresignRequest presignRequest,
                   HeadObjectRequest headObjectRequest, String bucket, String uploadPath,
                   boolean disableCalculateSHA256, String cdnUrl, Bandwidth limit,
                   boolean multipartEnabled, long partSizeBytes, int maxConcurrency) {
        this.s3AsyncClient = s3AsyncClient;
        this.preSigner = preSigner;
        this.presignRequest = presignRequest;
        this.headObjectRequest = headObjectRequest;
        this.bucket = bucket;
        this.uploadPath = uploadPath;
        this.disableCalculateSHA256 = disableCalculateSHA256;
        this.cdnUrl = cdnUrl;
        this.limit = limit;
        this.userRateLimiters = limit == null ? null : Caffeine.newBuilder()
                .scheduler(Scheduler.systemScheduler())
                .expireAfterAccess(10, TimeUnit.MINUTES)
                .build();
        this.multipartEnabled = multipartEnabled;
        this.partSizeBytes = partSizeBytes;
        this.maxConcurrency = maxConcurrency;

    }

    @Override
    public boolean canUpload() {
        return true;
    }

    @Override
    public ResourcePackHostType<S3Host> type() {
        return ResourcePackHosts.S3;
    }

    @Override
    public CompletableFuture<List<ResourcePackDownloadData>> requestResourcePackDownloadLink(NetWorkUser user) {
        if (checkRateLimit(user.uuid())) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }

        CompletableFuture<List<ResourcePackDownloadData>> future = new CompletableFuture<>();

        this.s3AsyncClient.headObject(this.headObjectRequest).whenComplete((headResponse, ex) -> {
            if (ex != null) {
                future.completeExceptionally(ex);
                return;
            }

            String sha1 = headResponse.metadata().get("sha1");
            if (sha1 == null) {
                fail(future, "Missing SHA-1 metadata in S3 object", null);
                return;
            }

            String downloadUrl = buildFinalUrl(this.preSigner.presignGetObject(this.presignRequest).httpRequest());
            UUID packUuid = UUID.nameUUIDFromBytes(sha1.getBytes(StandardCharsets.UTF_8));

            future.complete(List.of(new ResourcePackDownloadData(downloadUrl, packUuid, sha1)));
        });

        return future;
    }

    @Override
    public CompletableFuture<Void> upload(Path resourcePackPath) {
        CompletableFuture<Void> future = new CompletableFuture<>();

        CraftEngine.instance().scheduler().executeAsync(() -> {
            try {
                long fileSize = Files.size(resourcePackPath);
                String localSha1 = HashUtils.calculateLocalFileSha1(resourcePackPath);
                Map<String, String> metadata = Map.of("sha1", localSha1);
                if (this.multipartEnabled && fileSize > this.partSizeBytes) {
                    uploadMultipart(resourcePackPath, fileSize, metadata, future);
                } else {
                    uploadSimple(resourcePackPath, metadata, future);
                }
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
        });

        return future;
    }

    private void uploadSimple(Path resourcePackPath, Map<String, String> metadata, CompletableFuture<Void> future) {
        try {
            PutObjectRequest.Builder requestBuilder = PutObjectRequest.builder()
                    .bucket(this.bucket)
                    .key(this.uploadPath)
                    .metadata(metadata);

            if (!this.disableCalculateSHA256) {
                requestBuilder.checksumSHA256(calculateSHA256(resourcePackPath));
            }

            this.s3AsyncClient.putObject(requestBuilder.build(), AsyncRequestBody.fromFile(resourcePackPath))
                    .whenComplete((resp, ex) -> {
                        if (ex != null) {
                            fail(future, "Upload failed", ex.getMessage());
                        } else {
                            future.complete(null);
                        }
                    });
        } catch (Exception e) {
            future.completeExceptionally(e);
        }
    }

    private void uploadMultipart(Path resourcePackPath, long fileSize, Map<String, String> metadata, CompletableFuture<Void> future) {
        CreateMultipartUploadRequest createRequest = CreateMultipartUploadRequest.builder()
                .bucket(this.bucket)
                .key(this.uploadPath)
                .metadata(metadata)
                .build();

        this.s3AsyncClient.createMultipartUpload(createRequest).whenComplete((createResponse, createEx) -> {
            if (createEx != null) {
                fail(future, "Failed to initiate multipart upload", createEx.getMessage());
                return;
            }

            String uploadId = createResponse.uploadId();
            int totalParts = (int) ((fileSize + this.partSizeBytes - 1) / this.partSizeBytes);
            List<CompletableFuture<CompletedPart>> partFutures = new ArrayList<>(totalParts);
            Semaphore semaphore = new Semaphore(this.maxConcurrency);

            try {
                for (int partNumber = 1; partNumber <= totalParts; partNumber++) {
                    long position = (long) (partNumber - 1) * this.partSizeBytes;
                    long length = Math.min(this.partSizeBytes, fileSize - position);
                    int currentPartNumber = partNumber;

                    CompletableFuture<CompletedPart> partFuture = CompletableFuture
                            .supplyAsync(() -> readChunk(resourcePackPath, position, length))
                            .thenCompose(chunk -> uploadSinglePart(uploadId, currentPartNumber, chunk, semaphore));

                    partFutures.add(partFuture);
                }

                CompletableFuture.allOf(partFutures.toArray(new CompletableFuture[0])).whenComplete((v, ex) -> {
                    if (ex != null) {
                        abortMultipartUpload(uploadId);
                        fail(future, "Multipart upload failed", ex.getMessage());
                        return;
                    }

                    List<CompletedPart> completedParts = new ArrayList<>(partFutures.size());
                    for (CompletableFuture<CompletedPart> pf : partFutures) {
                        completedParts.add(pf.join());
                    }
                    completedParts.sort(Comparator.comparingInt(CompletedPart::partNumber));

                    CompleteMultipartUploadRequest completeRequest = CompleteMultipartUploadRequest.builder()
                            .bucket(this.bucket)
                            .key(this.uploadPath)
                            .uploadId(uploadId)
                            .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build())
                            .build();

                    this.s3AsyncClient.completeMultipartUpload(completeRequest).whenComplete((completeResponse, completeEx) -> {
                        if (completeEx != null) {
                            abortMultipartUpload(uploadId);
                            fail(future, "Failed to complete multipart upload", completeEx.getMessage());
                        } else {
                            future.complete(null);
                        }
                    });
                });
            } catch (Exception e) {
                abortMultipartUpload(uploadId);
                future.completeExceptionally(e);
            }
        });
    }

    private CompletableFuture<CompletedPart> uploadSinglePart(String uploadId, int partNumber, byte[] chunk, Semaphore semaphore) {
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            CompletableFuture<CompletedPart> failed = new CompletableFuture<>();
            failed.completeExceptionally(e);
            return failed;
        }

        UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
                .bucket(this.bucket)
                .key(this.uploadPath)
                .uploadId(uploadId)
                .partNumber(partNumber)
                .build();

        return this.s3AsyncClient.uploadPart(uploadPartRequest, AsyncRequestBody.fromBytes(chunk))
                .whenComplete((r, e) -> semaphore.release())
                .thenApply(response -> CompletedPart.builder()
                        .partNumber(partNumber)
                        .eTag(response.eTag())
                        .build());
    }

    private void abortMultipartUpload(String uploadId) {
        try {
            AbortMultipartUploadRequest abortRequest = AbortMultipartUploadRequest.builder()
                    .bucket(this.bucket)
                    .key(this.uploadPath)
                    .uploadId(uploadId)
                    .build();
            this.s3AsyncClient.abortMultipartUpload(abortRequest);
        } catch (Exception ignored) {
        }
    }

    private static byte[] readChunk(Path path, long position, long length) {
        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "r")) {
            raf.seek(position);
            byte[] buffer = new byte[(int) length];
            raf.readFully(buffer);
            return buffer;
        } catch (IOException e) {
            throw new RuntimeException("Failed to read file chunk", e);
        }
    }

    private String buildFinalUrl(SdkHttpRequest request) {
        String query = request.encodedQueryParameters().map(q -> "?" + q).orElse("");
        if (this.cdnUrl == null || this.cdnUrl.isEmpty()) {
            return request.protocol() + "://" + request.host() + request.encodedPath() + query;
        }

        String baseCdn = this.cdnUrl.endsWith("/") ? this.cdnUrl.substring(0, this.cdnUrl.length() - 1) : this.cdnUrl;
        return baseCdn + request.encodedPath() + query;
    }

    private boolean checkRateLimit(UUID player) {
        if (this.userRateLimiters == null) return false;
        Bucket bucket = this.userRateLimiters.get(player, k -> Bucket.builder().addLimit(this.limit).build());
        return bucket != null && !bucket.tryConsume(1);
    }

    private void fail(CompletableFuture<?> future, String reason, String body) {
        future.completeExceptionally(new RuntimeException("S3Host Error: " + reason + (body != null ? " | " + body : "")));
    }

    private static String calculateSHA256(Path filePath) {
        try (InputStream is = Files.newInputStream(filePath)) {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] buffer = new byte[8192];
            int len;
            while ((len = is.read(buffer)) != -1) {
                md.update(buffer, 0, len);
            }
            return Base64.getEncoder().encodeToString(md.digest());
        } catch (IOException | NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to calculate SHA256", e);
        }
    }

    private static class Factory implements ResourcePackHostFactory<S3Host> {
        private static final Region DEFAULT_REGION = Region.of("auto");
        private static final String[] USE_ENVIRONMENT_VARIABLES = new String[]{"use_environment_variables", "use-environment-variables"};
        private static final String[] PATH_STYLE = new String[]{"path_style", "path-style"};
        private static final String[] ACCESS_KEY_ID = new String[]{"access_key_id", "access-key-id"};
        private static final String[] ACCESS_KEY_SECRET = new String[]{"access_key_secret", "access-key-secret"};
        private static final String[] UPLOAD_PATH = new String[]{"upload_path", "upload-path"};
        private static final String[] DISABLE_CALCULATE_SHA256 = new String[]{"disable_calculate_sha256", "disable-calculate-sha256"};
        private static final String[] RATE_LIMIT = new String[]{"rate_map", "rate_limit", "rate-map", "rate-limit"};
        private static final String[] MAX_REQUESTS = new String[]{"max_requests", "max-requests"};
        private static final String[] RESET_INTERVAL = new String[]{"reset_interval", "reset-interval"};
        private static final String[] MULTIPART_UPLOAD = new String[]{"multipart_upload", "multipart-upload"};
        private static final String[] PART_SIZE = new String[]{"part_size", "part-size"};
        private static final String[] MAX_CONCURRENCY = new String[]{"max_concurrency", "max-concurrency"};
        private static final String[] CONNECT_TIMEOUT = new String[]{"connect_timeout", "connect-timeout"};
        private static final String[] SOCKET_TIMEOUT = new String[]{"socket_timeout", "socket-timeout", "read_timeout", "read-timeout"};
        private static final String[] API_CALL_TIMEOUT = new String[]{"api_call_timeout", "api-call-timeout"};
        private static final String[] API_CALL_ATTEMPT_TIMEOUT = new String[]{"api_call_attempt_timeout", "api-call-attempt-timeout"};

        @Override
        public S3Host create(ConfigSection section) {
            boolean useEnv = section.getBoolean(USE_ENVIRONMENT_VARIABLES);
            String endpoint = section.getNonEmptyString("endpoint");
            String protocol = section.getString("protocol", "https");
            URI endpointUri = URI.create(protocol + "://" + endpoint);
            boolean usePathStyle = section.getBoolean(PATH_STYLE);
            String bucket = section.getNonEmptyString("bucket");
            Region region = section.getValue("region", it -> Region.of(it.getAsString()), DEFAULT_REGION);
            String accessKeyId = useEnv ? getNonNullEnvironmentVariable(section, "CE_S3_ACCESS_KEY_ID") : section.getNonEmptyString(ACCESS_KEY_ID);
            String accessKeySecret = useEnv ? getNonNullEnvironmentVariable(section, "CE_S3_ACCESS_KEY_SECRET") : section.getNonEmptyString(ACCESS_KEY_SECRET);
            String uploadPath = section.getString(UPLOAD_PATH, "craftengine/resource_pack.zip");
            boolean disableCalculateSHA256 = section.getBoolean(DISABLE_CALCULATE_SHA256);
            Duration validity = Duration.ofSeconds(section.getInt("validity", 10));

            String cdnUrl = section.getValue("cdn", it -> {
                ConfigSection configSection = it.getAsSection();
                String cdnDomain = configSection.getNonEmptyString("domain");
                String cdnProtocol = configSection.getValue("protocol", ConfigValue::getAsNonEmptyString, "https");
                return cdnProtocol + "://" + cdnDomain;
            });

            AwsBasicCredentials credentials = AwsBasicCredentials.create(accessKeyId, accessKeySecret);

            S3Configuration configuration = S3Configuration.builder()
                    .pathStyleAccessEnabled(usePathStyle)
                    .build();

            NettyNioAsyncHttpClient.Builder httpClientBuilder = NettyNioAsyncHttpClient.builder();
            ClientOverrideConfiguration.Builder overrideConfigurationBuilder = ClientOverrideConfiguration.builder();

            ConfigValue timeout = section.getValue("timeout");
            if (timeout != null) {
                int connect, socket, apiCall, apiCallAttempt;
                if (timeout.is(Map.class)) {
                    ConfigSection configSection = timeout.getAsSection();
                    connect = configSection.getValue(CONNECT_TIMEOUT, it -> it.getAsInt(1), -1);
                    socket = configSection.getValue(SOCKET_TIMEOUT, it -> it.getAsInt(1), -1);
                    apiCall = configSection.getValue(API_CALL_TIMEOUT, it -> it.getAsInt(1), -1);
                    apiCallAttempt = configSection.getValue(API_CALL_ATTEMPT_TIMEOUT, it -> it.getAsInt(1), -1);
                } else {
                    connect = socket = apiCall = apiCallAttempt = timeout.getAsInt(1);
                }
                if (connect > 0) httpClientBuilder = httpClientBuilder.connectionTimeout(Duration.ofSeconds(connect));
                if (socket > 0) httpClientBuilder = httpClientBuilder.readTimeout(Duration.ofSeconds(socket)).writeTimeout(Duration.ofSeconds(socket));
                if (apiCall > 0) overrideConfigurationBuilder.apiCallTimeout(Duration.ofSeconds(apiCall));
                if (apiCallAttempt > 0) overrideConfigurationBuilder.apiCallAttemptTimeout(Duration.ofSeconds(apiCallAttempt));
            }

            S3AsyncClientBuilder s3AsyncClientBuilder = S3AsyncClient.builder()
                    .endpointOverride(endpointUri)
                    .region(region)
                    .credentialsProvider(StaticCredentialsProvider.create(credentials))
                    .serviceConfiguration(configuration)
                    .overrideConfiguration(overrideConfigurationBuilder.build());

            if (Config.enableProxy()) {
                ProxyConfiguration.Builder builder = ProxyConfiguration.builder().host(Config.proxyHost()).port(Config.proxyPort()).scheme(Config.proxyScheme());
                if (!Config.proxyUsername().isEmpty()) builder = builder.username(Config.proxyUsername());
                if (!Config.proxyPassword().isEmpty()) builder = builder.password(Config.proxyPassword());
                httpClientBuilder = httpClientBuilder.proxyConfiguration(builder.build());
            }

            s3AsyncClientBuilder = s3AsyncClientBuilder.httpClient(httpClientBuilder.build());

            S3AsyncClient s3AsyncClient = s3AsyncClientBuilder.build();
            S3Presigner preSigner = S3Presigner.builder()
                    .endpointOverride(endpointUri)
                    .region(region)
                    .credentialsProvider(StaticCredentialsProvider.create(credentials))
                    .serviceConfiguration(configuration)
                    .build();

            GetObjectPresignRequest presignRequest = GetObjectPresignRequest.builder()
                    .signatureDuration(validity)
                    .getObjectRequest(b -> b.bucket(bucket).key(uploadPath))
                    .build();

            HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                    .bucket(bucket)
                    .key(uploadPath)
                    .build();

            Bandwidth limit = section.getValue(RATE_LIMIT, it -> {
                ConfigSection configSection = it.getAsSection();
                int maxRequests = Math.max(configSection.getInt(MAX_REQUESTS, 5), 1);
                int resetInterval = Math.max(configSection.getInt(RESET_INTERVAL, 20), 1);
                return Bandwidth.builder()
                        .capacity(maxRequests)
                        .refillGreedy(maxRequests, Duration.ofSeconds(resetInterval))
                        .build();
            });

            ConfigSection multipartSection = section.getSection(MULTIPART_UPLOAD);
            boolean multipartEnabled = multipartSection != null;
            long partSizeBytes = -1, thresholdBytes = -1; int maxConcurrency = -1;
            if (multipartEnabled) {
                partSizeBytes = multipartSection.getLong(PART_SIZE, -1);
                maxConcurrency = multipartSection.getValue(MAX_CONCURRENCY, it -> it.getAsInt(1) -1);
                if (partSizeBytes <= 0 || maxConcurrency <= 0) multipartEnabled = false;
            }

            return new S3Host(
                    s3AsyncClient, preSigner, presignRequest,
                    headObjectRequest, bucket, uploadPath,
                    disableCalculateSHA256, cdnUrl, limit,
                    multipartEnabled, partSizeBytes, maxConcurrency
            );
        }
    }
}

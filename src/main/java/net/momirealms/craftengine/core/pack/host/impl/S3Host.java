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
import net.momirealms.craftengine.core.util.HashUtils;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.ProxyConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("unused")
public final class S3Host implements ResourcePackHost {
    public static final ResourcePackHostFactory<S3Host> FACTORY = new Factory();
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

    private String cachedSha1;

    private S3Host(S3AsyncClient s3AsyncClient, S3Presigner preSigner, GetObjectPresignRequest presignRequest,
                   HeadObjectRequest headObjectRequest, String bucket, String uploadPath,
                   boolean disableCalculateSHA256, String cdnUrl, Bandwidth limit) {
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
    public CompletableFuture<List<ResourcePackDownloadData>> requestResourcePackDownloadLink(UUID player) {
        if (checkRateLimit(player)) {
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

            this.cachedSha1 = sha1;
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
                String localSha1 = HashUtils.calculateLocalFileSha1(resourcePackPath);
                PutObjectRequest.Builder requestBuilder = PutObjectRequest.builder()
                        .bucket(this.bucket)
                        .key(this.uploadPath)
                        .metadata(Map.of("sha1", localSha1));

                if (!this.disableCalculateSHA256) {
                    requestBuilder.checksumSHA256(HashUtils.calculateLocalFileSha1(resourcePackPath));
                }

                this.s3AsyncClient.putObject(requestBuilder.build(), AsyncRequestBody.fromFile(resourcePackPath))
                        .whenComplete((resp, ex) -> {
                            if (ex != null) {
                                fail(future, "Upload failed", ex.getMessage());
                            } else {
                                this.cachedSha1 = localSha1;
                                future.complete(null);
                            }
                        });
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
        });

        return future;
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

            S3AsyncClientBuilder s3AsyncClientBuilder = S3AsyncClient.builder()
                    .endpointOverride(endpointUri)
                    .region(region)
                    .credentialsProvider(StaticCredentialsProvider.create(credentials))
                    .serviceConfiguration(b -> b.pathStyleAccessEnabled(usePathStyle));

            if (Config.enableProxy()) {
                ProxyConfiguration.Builder builder = ProxyConfiguration.builder().host(Config.proxyHost()).port(Config.proxyPort()).scheme(Config.proxyScheme());
                if (!Config.proxyUsername().isEmpty()) builder = builder.username(Config.proxyUsername());
                if (!Config.proxyPassword().isEmpty()) builder = builder.password(Config.proxyPassword());
                SdkAsyncHttpClient httpClient = NettyNioAsyncHttpClient.builder().proxyConfiguration(builder.build()).build();
                s3AsyncClientBuilder = s3AsyncClientBuilder.httpClient(httpClient);
            }

            S3AsyncClient s3AsyncClient = s3AsyncClientBuilder.build();

            S3Presigner preSigner = S3Presigner.builder()
                    .endpointOverride(endpointUri)
                    .region(region)
                    .credentialsProvider(StaticCredentialsProvider.create(credentials))
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

            return new S3Host(s3AsyncClient, preSigner, presignRequest, headObjectRequest, bucket, uploadPath, disableCalculateSHA256, cdnUrl, limit);
        }
    }
}

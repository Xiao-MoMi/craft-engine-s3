package net.momirealms.craftengine.core.pack.host.impl;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Scheduler;
import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import net.momirealms.craftengine.core.pack.host.*;
import net.momirealms.craftengine.core.plugin.CraftEngine;
import net.momirealms.craftengine.core.plugin.config.ConfigSection;
import net.momirealms.craftengine.core.plugin.config.ConfigValue;
import net.momirealms.craftengine.core.plugin.config.KnownResourceException;
import net.momirealms.craftengine.core.plugin.logger.Debugger;
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
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.utils.http.SdkHttpUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("unused")
final class S3Host implements ResourcePackHost {
    public static final ResourcePackHostFactory<S3Host> FACTORY = new Factory();
    private final S3AsyncClient s3AsyncClient;
    private final S3Presigner preSigner;
    private final GetObjectPresignRequest presignRequest;
    private final HeadObjectRequest headObjectRequest;
    private final String bucket;
    private final String uploadPath;
    private final boolean disableCalculateSHA256;
    private final String cdnUrl;
    private final boolean enableRateLimit;
    private final Bandwidth limit;
    private final Cache<UUID, Bucket> userRateLimiters = Caffeine.newBuilder()
            .scheduler(Scheduler.systemScheduler())
            .expireAfterAccess(10, TimeUnit.MINUTES)
            .build();

    private S3Host(
            S3AsyncClient s3AsyncClient,
            S3Presigner preSigner,
            GetObjectPresignRequest presignRequest,
            HeadObjectRequest headObjectRequest,
            String bucket,
            String uploadPath,
            boolean disableCalculateSHA256,
            String cdnUrl,
            Bandwidth limit
    ) {
        this.s3AsyncClient = s3AsyncClient;
        this.preSigner = preSigner;
        this.presignRequest = presignRequest;
        this.headObjectRequest = headObjectRequest;
        this.bucket = bucket;
        this.uploadPath = uploadPath;
        this.disableCalculateSHA256 = disableCalculateSHA256;
        this.cdnUrl = cdnUrl;
        this.limit = limit;
        this.enableRateLimit = limit != null;
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
            throw new RuntimeException("Failed to calculate SHA1", e);
        }
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
        if (this.checkRateLimit(player)) {
            Debugger.RESOURCE_PACK.debug(() -> "[S3] Rate limit exceeded for player " + player + ". Skipping request.");
            return CompletableFuture.completedFuture(Collections.emptyList());
        }
        return this.s3AsyncClient
                .headObject(this.headObjectRequest)
                .handle((headResponse, exception) -> {
                    if (exception != null) {
                        Throwable cause = exception.getCause();
                        if (cause instanceof NoSuchKeyException e) {
                            CraftEngine.instance().logger().warn("[S3] Resource pack not found in bucket '" + this.bucket + "'. Path: " + this.uploadPath, e);
                        } else {
                            CraftEngine.instance().logger().warn("[S3] Failed to retrieve resource pack metadata.", exception);
                        }
                        return Collections.emptyList();
                    }
                    String sha1 = headResponse.metadata().get("sha1");
                    if (sha1 == null) {
                        CraftEngine.instance().logger().warn("[S3] Missing SHA-1 checksum in object metadata. Path: " + this.uploadPath);
                        throw new CompletionException(new IllegalStateException("Missing SHA-1 metadata for S3 object: " + this.uploadPath));
                    }

                    return Collections.singletonList(
                            ResourcePackDownloadData.of(
                                    replaceWithCdnUrl(this.preSigner.presignGetObject(this.presignRequest).httpRequest()),
                                    UUID.nameUUIDFromBytes(sha1.getBytes(StandardCharsets.UTF_8)),
                                    sha1
                            )
                    );
                });
    }

    @Override
    public CompletableFuture<Void> upload(Path resourcePackPath) {
        PutObjectRequest.Builder build = PutObjectRequest.builder()
                .bucket(this.bucket)
                .key(this.uploadPath)
                .metadata(Map.of("sha1", HashUtils.calculateLocalFileSha1(resourcePackPath)));
        if (!this.disableCalculateSHA256) {
            build = build.checksumSHA256(calculateSHA256(resourcePackPath));
        }
        long uploadStart = System.currentTimeMillis();
        CraftEngine.instance().logger().info("[S3] Initiating resource pack upload to '" + this.uploadPath + "'");
        return this.s3AsyncClient
                .putObject(build.build(), AsyncRequestBody.fromFile(resourcePackPath))
                .handle((response, exception) -> {
                    if (exception != null) {
                        Throwable cause = exception instanceof CompletionException ?
                                exception.getCause() :
                                exception;
                        CraftEngine.instance().logger().warn("[S3] Upload failed for path '" + this.uploadPath + "'. Error: " + cause.getClass().getSimpleName() + " - " + cause.getMessage(), exception);
                    } else {
                        CraftEngine.instance().logger().info(
                                "[S3] Successfully uploaded resource pack to '" + this.uploadPath + "' in " +
                                        (System.currentTimeMillis() - uploadStart) + " ms"
                        );
                    }
                    return null;
                });
    }

    private String replaceWithCdnUrl(SdkHttpRequest sdkHttpRequest) {
        String encodedQueryString = sdkHttpRequest.encodedQueryParameters().map(value -> "?" + value).orElse("");
        String portString = SdkHttpUtils.isUsingStandardPort(sdkHttpRequest.protocol(), sdkHttpRequest.port()) ? "" : ":" + sdkHttpRequest.port();
        if (this.cdnUrl == null || this.cdnUrl.isEmpty()) {
            return sdkHttpRequest.protocol() + "://" + sdkHttpRequest.host() + portString + sdkHttpRequest.encodedPath() + encodedQueryString;
        }
        return this.cdnUrl + portString + sdkHttpRequest.encodedPath() + encodedQueryString;
    }

    private boolean checkRateLimit(UUID user) {
        if (!this.enableRateLimit) return false;
        Bucket rateLimiter = this.userRateLimiters.get(user, k -> Bucket.builder().addLimit(this.limit).build());
        if (rateLimiter == null) { // 怎么可能null?
            rateLimiter = Bucket.builder().addLimit(this.limit).build();
            this.userRateLimiters.put(user, rateLimiter);
        }
        return !rateLimiter.tryConsume(1);
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
            String protocol = section.getString("https", "protocol");
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

            ConfigSection proxySetting = section.getSection("proxy");
            if (proxySetting != null) {
                String host = proxySetting.getNonEmptyString("host");
                int port = proxySetting.getNonNullInt("port");
                if (port <= 0) {
                    throw new KnownResourceException("number.greater_than", section.assemblePath("port"), "port", "0");
                } else if (port > 65535) {
                    throw new KnownResourceException("number.less_than", section.assemblePath("port"), "port", "65536");
                }
                String scheme = proxySetting.getNonEmptyString("scheme");
                String username = proxySetting.getString("username");
                String password = proxySetting.getString("password");
                ProxyConfiguration.Builder builder = ProxyConfiguration.builder().host(host).port(port).scheme(scheme);
                if (username != null) builder = builder.username(username);
                if (password != null) builder = builder.password(password);
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

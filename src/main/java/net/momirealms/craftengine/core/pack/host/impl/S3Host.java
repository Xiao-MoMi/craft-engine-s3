package net.momirealms.craftengine.core.pack.host.impl;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Scheduler;
import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import net.momirealms.craftengine.core.pack.host.ResourcePackDownloadData;
import net.momirealms.craftengine.core.pack.host.ResourcePackHost;
import net.momirealms.craftengine.core.pack.host.ResourcePackHostFactory;
import net.momirealms.craftengine.core.pack.host.ResourcePackHosts;
import net.momirealms.craftengine.core.plugin.CraftEngine;
import net.momirealms.craftengine.core.plugin.locale.LocalizedException;
import net.momirealms.craftengine.core.plugin.logger.Debugger;
import net.momirealms.craftengine.core.util.HashUtils;
import net.momirealms.craftengine.core.util.Key;
import net.momirealms.craftengine.core.util.MiscUtils;
import net.momirealms.craftengine.core.util.ResourceConfigUtils;
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
public class S3Host implements ResourcePackHost {
    public static final Factory FACTORY = new Factory();
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
            .maximumSize(256)
            .scheduler(Scheduler.systemScheduler())
            .expireAfterAccess(10, TimeUnit.MINUTES)
            .build();

    public S3Host(
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

    @Override
    public boolean canUpload() {
        return true;
    }

    @Override
    public Key type() {
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

    public static class Factory implements ResourcePackHostFactory {

        @Override
        public ResourcePackHost create(Map<String, Object> arguments) {
            boolean useEnv = ResourceConfigUtils.getAsBoolean(arguments.getOrDefault("use-environment-variables", false), "use-environment-variables");
            String endpoint = ResourceConfigUtils.requireNonEmptyStringOrThrow(arguments.get("endpoint"), "warning.config.host.s3.missing_endpoint");
            String protocol = ResourceConfigUtils.requireNonEmptyStringOrThrow(arguments.getOrDefault("protocol", "https"), "warning.config.host.s3.missing_protocol");
            URI endpointUri = URI.create(protocol + "://" + endpoint);
            boolean usePathStyle = ResourceConfigUtils.getAsBoolean(arguments.getOrDefault("path-style", false), "path-style");
            String bucket = ResourceConfigUtils.requireNonEmptyStringOrThrow(arguments.get("bucket"), "warning.config.host.s3.missing_bucket");
            Region region = Region.of(ResourceConfigUtils.requireNonEmptyStringOrThrow(arguments.getOrDefault("region", "auto"), "warning.config.host.s3.missing_region"));
            String accessKeyId = ResourceConfigUtils.requireNonEmptyStringOrThrow(useEnv ? System.getenv("CE_S3_ACCESS_KEY_ID") : arguments.get("access-key-id"), "warning.config.host.s3.missing_access_key");
            String accessKeySecret = ResourceConfigUtils.requireNonEmptyStringOrThrow(useEnv ? System.getenv("CE_S3_ACCESS_KEY_SECRET") : arguments.get("access-key-secret"), "warning.config.host.s3.missing_secret");
            String uploadPath = ResourceConfigUtils.requireNonEmptyStringOrThrow(arguments.getOrDefault("upload-path", "craftengine/resource_pack.zip"), "warning.config.host.s3.missing_upload_path");
            boolean disableCalculateSHA256 = ResourceConfigUtils.getAsBoolean(arguments.getOrDefault("disable-calculate-sha256", false), "disable-calculate-sha256");
            Duration validity = Duration.ofSeconds(ResourceConfigUtils.getAsInt(arguments.getOrDefault("validity", 10), "validity"));

            Map<String, Object> cdn = MiscUtils.castToMap(arguments.get("cdn"), true);
            String cdnUrl = null;
            if (cdn != null) {
                String cdnDomain = ResourceConfigUtils.requireNonEmptyStringOrThrow(cdn.get("domain"), "warning.config.host.s3.missing_cdn_domain");
                String cdnProtocol = ResourceConfigUtils.requireNonEmptyStringOrThrow(cdn.getOrDefault("protocol", "https"), "warning.config.host.s3.missing_cdn_protocol");
                cdnUrl = cdnProtocol + "://" + cdnDomain;
            }

            AwsBasicCredentials credentials = AwsBasicCredentials.create(accessKeyId, accessKeySecret);

            S3AsyncClientBuilder s3AsyncClientBuilder = S3AsyncClient.builder()
                    .endpointOverride(endpointUri)
                    .region(region)
                    .credentialsProvider(StaticCredentialsProvider.create(credentials))
                    .serviceConfiguration(b -> b.pathStyleAccessEnabled(usePathStyle));

            Map<String, Object> proxySetting = MiscUtils.castToMap(arguments.get("proxy"), true);
            if (proxySetting != null) {
                String host = ResourceConfigUtils.requireNonEmptyStringOrThrow(proxySetting.get("host"), "warning.config.host.proxy.missing_host");
                int port = ResourceConfigUtils.getAsInt(proxySetting.get("port"), "port");
                if (port <= 0 || port > 65535) {
                    throw new LocalizedException("warning.config.host.proxy.missing_port");
                }
                String scheme = ResourceConfigUtils.requireNonEmptyStringOrThrow(proxySetting.get("scheme"), "warning.config.host.proxy.missing_scheme");
                String username = Optional.ofNullable(proxySetting.get("username")).map(String::valueOf).orElse(null);
                String password = Optional.ofNullable(proxySetting.get("password")).map(String::valueOf).orElse(null);
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

            Map<String, Object> rateMap = MiscUtils.castToMap(ResourceConfigUtils.get(arguments, "rate-map", "rate-limit"), true);
            Bandwidth limit = null;
            if (rateMap != null) {
                int maxRequests = Math.max(ResourceConfigUtils.getAsInt(rateMap.getOrDefault("max-requests", 5), "max-requests"), 1);
                int resetInterval = Math.max(ResourceConfigUtils.getAsInt(rateMap.getOrDefault("reset-interval", 20), "reset-interval"), 1);
                limit = Bandwidth.builder()
                        .capacity(maxRequests)
                        .refillGreedy(maxRequests, Duration.ofSeconds(resetInterval))
                        .initialTokens(maxRequests / 2) // 修正首次可以直接突破限制请求 maxRequests * 2 次
                        .build();
            }

            return new S3Host(s3AsyncClient, preSigner, presignRequest, headObjectRequest, bucket, uploadPath, disableCalculateSHA256, cdnUrl, limit);
        }
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
}

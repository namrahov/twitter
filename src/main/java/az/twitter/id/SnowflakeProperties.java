package az.twitter.id;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * @param workerId unique per running instance (0..1023); two instances with the same id can produce duplicate ids
 */
@Validated
@ConfigurationProperties("twitter.snowflake")
public record SnowflakeProperties(
        @Min(0) @Max(SnowflakeIdGenerator.MAX_WORKER_ID) long workerId
) {
}

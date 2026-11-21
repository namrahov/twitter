package az.twitter.dto.request;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record EditPostRequest(
        @NotBlank @Size(max = 280)
        String content
) {
}

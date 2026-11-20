package az.twitter.user;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

public record CreateUserRequest(
        @NotNull @Pattern(regexp = "^[A-Za-z0-9_]{1,50}$", message = "1-50 letters, digits or underscore")
        String username,
        @NotBlank @Size(max = 100)
        String displayName
) {
}

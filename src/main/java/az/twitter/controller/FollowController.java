package az.twitter.controller;

import az.twitter.dto.response.CursorPage;
import az.twitter.dto.response.UserResponse;
import az.twitter.security.CurrentUser;
import az.twitter.service.FollowService;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/users/{id}")
public class FollowController {

    private final FollowService followService;

    public FollowController(FollowService followService) {
        this.followService = followService;
    }

    /** PUT is idempotent: repeating it (client retry, double click) is harmless. */
    @PutMapping("/follow")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void follow(@CurrentUser long currentUserId, @PathVariable long id) {
        followService.follow(currentUserId, id);
    }

    @DeleteMapping("/follow")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void unfollow(@CurrentUser long currentUserId, @PathVariable long id) {
        followService.unfollow(currentUserId, id);
    }

    @GetMapping("/followers")
    public CursorPage<UserResponse> followers(@PathVariable long id,
                                              @RequestParam(defaultValue = "0") long cursor,
                                              @RequestParam(defaultValue = "20") @Min(1) @Max(100) int limit) {
        return followService.followers(id, cursor, limit);
    }

    @GetMapping("/following")
    public CursorPage<UserResponse> following(@PathVariable long id,
                                              @RequestParam(defaultValue = "0") long cursor,
                                              @RequestParam(defaultValue = "20") @Min(1) @Max(100) int limit) {
        return followService.following(id, cursor, limit);
    }
}

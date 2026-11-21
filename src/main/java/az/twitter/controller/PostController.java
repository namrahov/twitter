package az.twitter.controller;

import az.twitter.dto.request.CreatePostRequest;
import az.twitter.dto.request.EditPostRequest;
import az.twitter.dto.response.CursorPage;
import az.twitter.dto.response.PostResponse;
import az.twitter.security.CurrentUser;
import az.twitter.service.PostService;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class PostController {

    private final PostService postService;

    public PostController(PostService postService) {
        this.postService = postService;
    }

    @PostMapping("/posts")
    @ResponseStatus(HttpStatus.CREATED)
    public PostResponse create(@CurrentUser long currentUserId, @Valid @RequestBody CreatePostRequest request) {
        return PostResponse.from(postService.create(currentUserId, request.content()));
    }

    @GetMapping("/posts/{id}")
    public PostResponse get(@PathVariable long id) {
        return PostResponse.from(postService.get(id));
    }

    /** Author only (403 otherwise). */
    @PutMapping("/posts/{id}")
    public PostResponse edit(@CurrentUser long currentUserId, @PathVariable long id,
                             @Valid @RequestBody EditPostRequest request) {
        return PostResponse.from(postService.edit(currentUserId, id, request.content()));
    }

    /** Author only (403 otherwise). */
    @DeleteMapping("/posts/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void delete(@CurrentUser long currentUserId, @PathVariable long id) {
        postService.delete(currentUserId, id);
    }

    /** Profile timeline. {@code cursor} is the last post id of the previous page. */
    @GetMapping("/users/{id}/posts")
    public CursorPage<PostResponse> byUser(@PathVariable long id,
                                           @RequestParam(defaultValue = "" + Long.MAX_VALUE) long cursor,
                                           @RequestParam(defaultValue = "20") @Min(1) @Max(100) int limit) {
        return postService.byUser(id, cursor, limit);
    }
}

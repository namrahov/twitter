package az.twitter.controller;

import az.twitter.dto.response.CursorPage;
import az.twitter.dto.response.PostResponse;
import az.twitter.security.CurrentUser;
import az.twitter.service.TimelineService;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TimelineController {

    private final TimelineService timelineService;

    public TimelineController(TimelineService timelineService) {
        this.timelineService = timelineService;
    }

    /** Home timeline: own posts and posts of followed users, newest first. */
    @GetMapping("/timeline")
    public CursorPage<PostResponse> home(@CurrentUser long currentUserId,
                                         @RequestParam(defaultValue = "" + Long.MAX_VALUE) long cursor,
                                         @RequestParam(defaultValue = "20") @Min(1) @Max(100) int limit) {
        return timelineService.home(currentUserId, cursor, limit);
    }
}

package real.talk.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.*;
import real.talk.model.dto.lesson.*;
import real.talk.model.entity.Lesson;
import real.talk.model.entity.User;
import real.talk.service.lesson.LessonService;
import real.talk.service.user.UserService;

import java.util.List;
import java.util.UUID;

import static real.talk.util.filters.LessonFilterNormalizer.*;

@RestController
@RequestMapping("/api/v1/lessons")
@RequiredArgsConstructor
@Slf4j
class LessonsController {

    private final UserService userService;
    private final LessonService lessonService;

    @PostMapping("/create-lesson")
    public ResponseEntity<LessonCreateResponse> createLesson(@RequestBody LessonCreateRequest lessonRequest) {

        User user = userService.saveUser(lessonRequest);
        Lesson lesson = lessonService.createLesson(user, lessonRequest);
        LessonCreateResponse createLessonResponse = LessonCreateResponse.builder()
                .lessonId(lesson.getId())
                .build();
        return ResponseEntity.ok(createLessonResponse);
    }

    @GetMapping("/is-lesson-ready/{lessonId}")
    public ResponseEntity<Boolean> lessonStatus(@PathVariable UUID lessonId) {
        return ResponseEntity.ok(lessonService.isLessonReady(lessonId));
    }

    @GetMapping("/public-lessons")
    public ResponseEntity<List<LessonLiteResponse>> getPublicLessons(
            @RequestParam(required = false) String language,
            @RequestParam(name = "language_level", required = false) String languageLevel,
            @RequestParam(name = "lesson_topic", required = false) String lessonTopic,
            @RequestParam(name = "grammar_contains", required = false) String grammarContains,
            @RequestParam(required = false) Integer page,
            @RequestParam(required = false) Integer size,
            @RequestParam(defaultValue = "createdAt,desc") String sort,
            @RequestParam(required = false) String email) {
        var filter = LessonFilter.builder()
                .language(blankToNull(language))
                .languageLevel(blankToNull(languageLevel))
                .lessonTopic(blankToNull(lessonTopic))
                .grammarContains(blankToNull(grammarContains))
                .page(page)
                .size(size)
                .sort(blankToNull(sort))
                .email(blankToNull(email))
                .build();
        var normalized = normalize(filter);
        if (!normalized.equals(filter)) {
            log.info("Normalized public-lessons params: from={} to={}", filter, normalized);
        }

        // Identify usage
        User currentUser = null;
        try {
            org.springframework.security.core.Authentication auth = org.springframework.security.core.context.SecurityContextHolder
                    .getContext().getAuthentication();
            if (auth != null && auth.isAuthenticated() && !auth.getPrincipal().equals("anonymousUser")) {
                if (auth.getPrincipal() instanceof org.springframework.security.oauth2.core.user.OAuth2User oauthUser) {
                    String email1 = oauthUser.getAttribute("email");
                    currentUser = userService.getUserByEmail(email1).orElse(null);
                } else if (auth
                        .getPrincipal() instanceof org.springframework.security.core.userdetails.UserDetails userDetails) {
                    currentUser = userService.getUserByEmail(userDetails.getUsername()).orElse(null);
                } else if (auth.getPrincipal() instanceof User) {
                    currentUser = (User) auth.getPrincipal();
                }
            }
        } catch (Exception e) {
            log.warn("Failed to resolve user in LessonsController", e);
        }

        var resultPage = lessonService.getPublicLessonsLite(normalized, currentUser);
        return ResponseEntity.ok(resultPage.getContent());
    }

    private static String blankToNull(String s) {
        return (s == null || s.isBlank()) ? null : s;
    }

    @GetMapping("/{lessonId}")
    public ResponseEntity<LessonFullResponse> getLessonById(@PathVariable UUID lessonId) {
        return ResponseEntity.ok(lessonService.getLessonFullById(lessonId));
    }

    @DeleteMapping("/{lessonId}")
    public ResponseEntity<Void> deleteLesson(@AuthenticationPrincipal User user, @PathVariable UUID lessonId) {
        if (user == null) {
            return ResponseEntity.status(401).build();
        }
        lessonService.deletePendingLesson(user.getUserId(), lessonId);
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/{lessonId}/share")
    public ResponseEntity<Void> shareLesson(@PathVariable UUID lessonId, @RequestParam UUID studentId) {
        lessonService.shareLesson(lessonId, studentId);
        return ResponseEntity.ok().build();
    }

    @DeleteMapping("/{lessonId}/unshare")
    public ResponseEntity<Void> unshareLesson(@PathVariable UUID lessonId, @RequestParam UUID studentId) {
        lessonService.unshareLesson(lessonId, studentId);
        return ResponseEntity.noContent().build();
    }

    @GetMapping("/{lessonId}/shared-users")
    public ResponseEntity<List<real.talk.model.dto.student.StudentResponse>> getSharedUsers(
            @PathVariable UUID lessonId) {
        return ResponseEntity.ok(lessonService.getSharedUsers(lessonId));
    }

    @GetMapping("/shared")
    public ResponseEntity<List<LessonLiteResponse>> getSharedLessons() {
        // Resolve user
        User currentUser = null;
        try {
            org.springframework.security.core.Authentication auth = org.springframework.security.core.context.SecurityContextHolder
                    .getContext().getAuthentication();
            if (auth != null && auth.isAuthenticated() && !auth.getPrincipal().equals("anonymousUser")) {
                if (auth.getPrincipal() instanceof User) {
                    currentUser = (User) auth.getPrincipal();
                } else if (auth
                        .getPrincipal() instanceof org.springframework.security.core.userdetails.UserDetails userDetails) {
                    currentUser = userService.getUserByEmail(userDetails.getUsername()).orElseThrow();
                }
            }
        } catch (Exception e) {
            log.warn("Failed to resolve user in LessonsController", e);
        }

        if (currentUser == null) {
            return ResponseEntity.status(401).build();
        }

        return ResponseEntity.ok(lessonService.getSharedLessons(currentUser));
    }

}

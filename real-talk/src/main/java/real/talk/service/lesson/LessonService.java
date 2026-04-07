package real.talk.service.lesson;

import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import real.talk.model.dto.lesson.*;
import real.talk.model.entity.Lesson;
import real.talk.model.entity.User;
import real.talk.model.entity.enums.LessonAccess;
import real.talk.model.entity.enums.LessonStatus;
import real.talk.model.entity.enums.UserRole;
import real.talk.repository.lesson.LessonRepository;
import real.talk.repository.user.UserRepository;
import real.talk.service.access.AccessControlService;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class LessonService {

    private static final String DEFAULT_PRESET = "claudia";

    private final LessonRepository lessonRepository;
    private final UserRepository userRepository;
    private final AccessControlService accessControlService;

    public Lesson createLesson(User user, LessonCreateRequest lessonRequest) {
        // Access Check
        checkBuilderAccess(user);
        validateYoutubeLink(lessonRequest.getYoutubeLink());
        validateSegment(lessonRequest.getSegmentStartMin(), lessonRequest.getSegmentEndMin());

        Lesson lesson = new Lesson();
        lesson.setId(UUID.randomUUID());
        lesson.setUser(user);
        lesson.setLanguage(lessonRequest.getLanguage());
        lesson.setLanguageLevel(lessonRequest.getLanguageLevel());
        lesson.setGrammarTopics(normalizeGrammarTopics(lessonRequest.getGrammarTopics()));
        lesson.setPreset(resolvePreset(lessonRequest.getPreset()));
        lesson.setYoutubeUrl(lessonRequest.getYoutubeLink().trim());
        lesson.setSegmentStartMin(lessonRequest.getSegmentStartMin());
        lesson.setSegmentEndMin(lessonRequest.getSegmentEndMin());
        lesson.setStatus(LessonStatus.PENDING);
        lesson.setAccess(LessonAccess.PUBLIC);
        lesson.setCreatedAt(Instant.now());
        lessonRepository.save(lesson);

        return lesson;
    }

    private String resolvePreset(String preset) {
        return (preset == null || preset.isBlank()) ? DEFAULT_PRESET : preset.trim();
    }

    private void validateYoutubeLink(String youtubeLink) {
        if (youtubeLink == null || youtubeLink.isBlank()) {
            throw new IllegalArgumentException("youtubeLink is required");
        }
    }

    private List<String> normalizeGrammarTopics(List<String> grammarTopics) {
        if (grammarTopics == null || grammarTopics.isEmpty()) {
            return List.of();
        }
        return grammarTopics.stream()
                .filter(topic -> topic != null && !topic.isBlank())
                .map(String::trim)
                .limit(3)
                .toList();
    }

    private void validateSegment(Double segmentStartMin, Double segmentEndMin) {
        boolean hasStart = segmentStartMin != null;
        boolean hasEnd = segmentEndMin != null;
        if (hasStart != hasEnd) {
            throw new IllegalArgumentException("segmentStartMin and segmentEndMin must be provided together");
        }
        if (!hasStart) {
            return;
        }
        if (segmentStartMin < 0 || segmentEndMin <= 0) {
            throw new IllegalArgumentException("Segment bounds must be positive");
        }
        if (segmentEndMin <= segmentStartMin) {
            throw new IllegalArgumentException("segmentEndMin must be greater than segmentStartMin");
        }
    }

    private void checkBuilderAccess(User user) {
        // Delegate to centralized guard
        // We assume 'false' for scenarios for now unless specific endpoint requires it
        // Or if createLesson uses scenarios (it does set 'scenario' sometimes?)
        // The Lesson entity doesn't show 'scenario' field in the snippet, but prompt
        // mentions it.
        // For MVP/Current state, let's enforce basic builder access.
        // If scenarios are separate param, we would check it.
        // Prompt says: "Lesson scenario selection in Lesson Builder" -> Plus only.
        // But here we are just creating a lesson. Let's assume basic access check
        // first.
        accessControlService.checkBuilderAccess(user, false);
    }

    public List<Lesson> getPendingLessons() {
        return lessonRepository.findByStatus(LessonStatus.PENDING);
    }

    public List<Lesson> getProcessingLessons() {
        return lessonRepository.findByStatus(LessonStatus.PROCESSING);
    }

    public Optional<Lesson> getLessonWithGladiaDone() {
        return lessonRepository.findProcessingLessonWithGladiaDone();
    }

    public List<Lesson> getBatchProcessingLessonsWithGladiaDone(int limit) {
        return lessonRepository.findBatchProcessingLessonsWithGladiaDone(limit);
    }

    public List<Lesson> getLessonsWithLlmDone() {
        return lessonRepository.findProcessingLessonsWithLlmDone();
    }

    public Boolean isLessonReady(UUID lessonId) {
        return lessonRepository.existsByIdAndStatusEquals(lessonId, LessonStatus.READY);
    }

    /**
     * Полный урок по id: возвращаем мета + весь JSON data (exercises, glossary и
     * т.д.).
     * Используется для детальной страницы / перехода по ссылке.
     */
    public LessonFullResponse getLessonFullById(UUID lessonId) {
        Lesson l = lessonRepository.findById(lessonId)
                .orElseThrow(() -> new IllegalArgumentException("Lesson not found: " + lessonId));

        // youtubeUrl лежит в колонке, а основной контент — в JSON 'data'
        GeneratedPreset data = l.getData();

        return new LessonFullResponse(
                l.getId(),
                l.getYoutubeUrl(),
                l.getLessonTopic(),
                l.getTags(), // JSON Tags (lesson_theme, language, lexical_fields, ...)
                l.getGrammarTopics(), // список тем грамматики сверху (если нужен на странице)
                l.getCreatedAt(),
                data // ВЕСЬ контент урока (exercises, glossary, transcript, и т.д.)
        );
    }

    /**
     * Лёгкий список публичных готовых уроков (без JSON 'data'), постранично.
     * Используется на странице OpenLibrary.
     */
    public Page<LessonLiteResponse> getPublicLessonsLite(LessonFilter f, User viewer) {
        Pageable pageable = buildPageable(f.getPage(), f.getSize(), f.getSort());

        // Check Access Level
        boolean fullAccess = accessControlService.canAccessPublicLibrary(viewer);

        // If filtering by specific email (author), handle it
        String email = f.getEmail();
        if (email != null) {
            return lessonRepository.findLiteByStatusAndAccessAndEmail(
                    List.of(LessonStatus.READY,
                            LessonStatus.PENDING,
                            LessonStatus.PROCESSING),
                    LessonAccess.PUBLIC,
                    email,
                    pageable);
        }

        if (fullAccess) {
            // Smart/Plus: Show All Public Lessons (Community + System)
            return lessonRepository.findLiteByStatusAndAccess(
                    LessonStatus.READY,
                    LessonAccess.PUBLIC,
                    pageable);
        } else {
            // Unauth / Free / Student: Show Only System Lessons (Admin)
            // "Open Library" = System content
            return lessonRepository.findLiteByStatusAndAccessAndUserRole(
                    LessonStatus.READY,
                    LessonAccess.PUBLIC,
                    UserRole.ADMIN,
                    pageable);
        }
    }

    public Page<LessonLiteResponse> getMyLessonsLite(LessonFilter f) {
        Pageable pageable = buildPageable(f.getPage(), f.getSize(), f.getSort());
        Page<LessonLiteResponse> page = lessonRepository.findLiteByStatusAndAccess(
                LessonStatus.READY,
                LessonAccess.PUBLIC,
                pageable);
        return page;
    }

    public List<GeneratedPreset> getPublicReadyLessons() {
        return lessonRepository.findByStatusAndAccess(LessonStatus.READY, LessonAccess.PUBLIC)
                .stream().map(Lesson::getData).toList();
    }

    // Перегрузка с фильтрами — теперь постранично + сортировка (белый список) */
    public Page<GeneratedPreset> getPublicReadyLessons(LessonFilter f) {
        Pageable pageable = buildPageable(f.getPage(), f.getSize(), f.getSort()); // только page/size
        // разберём sort=language,-lesson_topic → s1,s2 токены
        String[] sortTokens = toSortTokens(f.getSort());
        Page<Lesson> page = lessonRepository.findPublicReadyFilteredPaged(
                LessonStatus.READY.name(), LessonAccess.PUBLIC.name(),
                f.getLanguage(), f.getLanguageLevel(), f.getLessonTopic(), f.getGrammarContains(),
                sortTokens[0], sortTokens[1],
                pageable);
        return page.map(lesson -> {
            GeneratedPreset dto = lesson.getData();
            return dto;
        });
    }

    // ===== helpers =====
    private Pageable buildPageable(Integer page, Integer size, String sortParam) {
        int p = (page == null || page < 0) ? 0 : page;
        int s = (size == null || size <= 0) ? 10 : Math.min(size, 50);
        Sort.Direction direction = sortParam.startsWith("-") ? Sort.Direction.DESC : Sort.Direction.ASC;
        String sortField = sortParam.startsWith("-") || sortParam.startsWith("+")
                ? sortParam.substring(1)
                : sortParam;

        return PageRequest.of(p, s, Sort.by(direction, sortField));
    }

    /**
     * + * Преобразуем sort-параметр в 2 безопасных токена для SQL:
     * + * input: "language,-lesson_topic" → ["language_asc","lesson_topic_desc"]
     * + * Белый список: language | lesson_topic | createdAt
     * + * Любой мусор → игнор → дефолт "created_at_desc".
     * +
     */
    private String[] toSortTokens(String sortParam) {
        String def = "created_at_desc";
        String s1 = def, s2 = def;
        if (sortParam == null || sortParam.isBlank())
            return new String[] { s1, s2 };

        ArrayList<String> tokens = new ArrayList<>();
        for (String raw : sortParam.split(",")) {
            String t = raw.trim();
            if (t.isEmpty())
                continue;
            boolean desc = t.startsWith("-");
            String key = t.replaceFirst("^[+-]", "");
            String dbKey = switch (key) {
                case "language" -> "language";
                case "lesson_topic" -> "lesson_topic";
                case "createdAt" -> "created_at";
                default -> null; // не из белого списка — пропускаем
            };
            if (dbKey != null) {
                tokens.add(dbKey + (desc ? "_desc" : "_asc"));
            }
        }
        if (!tokens.isEmpty())
            s1 = tokens.get(0);
        if (tokens.size() > 1)
            s2 = tokens.get(1);
        return new String[] { s1, s2 };
    }

    public Lesson saveLesson(Lesson lesson) {
        return lessonRepository.save(lesson);
    }

    public void shareLesson(UUID lessonId, UUID studentId) {
        Lesson lesson = lessonRepository.findById(lessonId)
                .orElseThrow(() -> new IllegalArgumentException("Lesson not found"));

        User student = userRepository.findById(studentId)
                .orElseThrow(() -> new IllegalArgumentException("Student not found"));

        lesson.getSharedUsers().add(student);
        lessonRepository.save(lesson);
    }

    public void unshareLesson(UUID lessonId, UUID studentId) {
        Lesson lesson = lessonRepository.findById(lessonId)
                .orElseThrow(() -> new IllegalArgumentException("Lesson not found"));

        User student = userRepository.findById(studentId)
                .orElseThrow(() -> new IllegalArgumentException("Student not found"));

        lesson.getSharedUsers().remove(student);
        lessonRepository.save(lesson);
    }

    public List<real.talk.model.dto.student.StudentResponse> getSharedUsers(UUID lessonId) {
        Lesson lesson = lessonRepository.findById(lessonId)
                .orElseThrow(() -> new IllegalArgumentException("Lesson not found"));
        return lesson.getSharedUsers().stream()
                .map(u -> new real.talk.model.dto.student.StudentResponse(u.getUserId(), u.getName(), u.getEmail(),
                        null))
                .toList();
    }

    public List<LessonLiteResponse> getSharedLessons(User user) {
        return lessonRepository.findSharedLessons(user.getUserId());
    }

    @Transactional
    public void deletePendingLesson(UUID userId, UUID lessonId) {
        Lesson lesson = lessonRepository.findById(lessonId)
                .orElseThrow(() -> new AccessControlService.AccessDeniedException("Lesson not found"));

        if (!lesson.getUser().getUserId().equals(userId)) {
            throw new AccessControlService.AccessDeniedException("You can delete only your own lessons");
        }

        if (lesson.getStatus() != LessonStatus.PENDING) {
            throw new AccessControlService.AccessDeniedException("Only PENDING lessons can be deleted");
        }

        lessonRepository.delete(lesson);
    }
}

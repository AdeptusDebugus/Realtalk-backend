package real.talk.service.gladia;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import real.talk.model.dto.gladia.PreRecorderResponse;
import real.talk.model.dto.gladia.TranscriptionResultResponse;
import real.talk.model.entity.GladiaData;
import real.talk.model.entity.Lesson;
import real.talk.model.entity.enums.DataStatus;
import real.talk.model.entity.enums.LessonStatus;
import real.talk.service.lesson.LessonService;

import java.time.Instant;
import java.util.List;

import static java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor;

@Service
@RequiredArgsConstructor
@Slf4j
public class GladiaTaskScheduler {

    private static final int MAX_ATTEMPTS = 3;

    private final TranscriptionService transcriptionService;
    private final GladiaService gladiaService;
    private final LessonService lessonService;

    @Scheduled(cron = "${gladia.create-request.cron}")
    public void processGladiaRequest() {
        List<Lesson> pendingLessons = lessonService.getPendingLessons()
                .stream()
                .filter( it -> it.getCreatedAt().isBefore(Instant.now().minusSeconds(60)))
                .toList();

        if (pendingLessons == null || pendingLessons.isEmpty())
            return;

        log.info("Starting concurrent gladia requests for {} lessons", pendingLessons.size());
        try (var executor = newVirtualThreadPerTaskExecutor()) {
            pendingLessons.forEach(lesson -> executor.submit(() -> processLessonRequest(lesson)));
        }
        log.info("Finished submitting gladia requests to executor");
    }

    private void processLessonRequest(Lesson lesson) {
        int nextAttempt = (lesson.getGladiaAttempts() == null ? 0 : lesson.getGladiaAttempts()) + 1;
        lesson.setGladiaAttempts(nextAttempt);
        try {
            log.info("Processing lesson {} (Gladia attempt {}/{})", lesson.getId(), nextAttempt, MAX_ATTEMPTS);
            if (lesson.getYoutubeUrl() == null || lesson.getYoutubeUrl().isBlank()) {
                lesson.setStatus(LessonStatus.ERROR);
                lessonService.saveLesson(lesson);
                log.error("Lesson {} has empty youtubeUrl. Marked as ERROR", lesson.getId());
                return;
            }
            lesson.setStatus(LessonStatus.PROCESSING);
            PreRecorderResponse preRecorderResponse = transcriptionService.transcribeAudio(lesson.getYoutubeUrl());
            GladiaData gladiaData = new GladiaData();
            gladiaData.setLesson(lesson);
            gladiaData.setStatus(DataStatus.CREATED);
            gladiaData.setGladiaRequestId(preRecorderResponse.getId());
            gladiaData.setCreatedAt(Instant.now());
            gladiaService.saveGladiaData(gladiaData);
            lessonService.saveLesson(lesson);
            log.info("Finished processing lesson {}", lesson.getId());
        } catch (Exception e) {
            if (nextAttempt >= MAX_ATTEMPTS) {
                lesson.setStatus(LessonStatus.ERROR);
                log.error("Lesson {} reached max Gladia attempts ({}). Marked as ERROR", lesson.getId(),
                        MAX_ATTEMPTS, e);
            } else {
                lesson.setStatus(LessonStatus.PENDING);
                log.warn("Lesson {} Gladia attempt {}/{} failed. Will retry", lesson.getId(), nextAttempt,
                        MAX_ATTEMPTS, e);
            }
            lessonService.saveLesson(lesson);
        }
    }

    @Scheduled(cron = "${gladia.get-response.cron}")
    public void processGladiaResponse() {
        List<GladiaData> gladiaDataByStatus = gladiaService.getGladiaDataByStatusCreated();

        if (gladiaDataByStatus == null || gladiaDataByStatus.isEmpty())
            return;

        log.info("Starting concurrent gladia responses check for {} tasks", gladiaDataByStatus.size());
        try (var executor = newVirtualThreadPerTaskExecutor()) {
            gladiaDataByStatus.forEach(gladiaData -> executor.submit(() -> processGladiaDataResponse(gladiaData)));
        }
        log.info("Finished submitting gladia responses to executor");
    }

    private void processGladiaDataResponse(GladiaData gladiaData) {
        try {
            log.info("Processing gladiaRequest {}", gladiaData.getGladiaRequestId());
            TranscriptionResultResponse transcriptionResult = transcriptionService
                    .getTranscriptionResult(gladiaData.getGladiaRequestId());
            if (transcriptionResult.getStatus().equals("done")) {
                gladiaData.setStatus(DataStatus.DONE);
                gladiaData.setData(transcriptionResult);
                gladiaService.saveGladiaData(gladiaData);
                log.info("Finished processing gladiaRequest {}", gladiaData.getGladiaRequestId());
            }
        } catch (Exception e) {
            Lesson lesson = gladiaData.getLesson();
            int nextAttempt = (lesson.getGladiaAttempts() == null ? 0 : lesson.getGladiaAttempts()) + 1;
            lesson.setGladiaAttempts(nextAttempt);
            if (nextAttempt >= MAX_ATTEMPTS) {
                lesson.setStatus(LessonStatus.ERROR);
                gladiaData.setStatus(DataStatus.ERROR);
                lessonService.saveLesson(lesson);
                gladiaService.saveGladiaData(gladiaData);
                log.error("Lesson {} reached max Gladia attempts ({}) while checking response. Marked as ERROR",
                        lesson.getId(), MAX_ATTEMPTS, e);
            } else {
                lessonService.saveLesson(lesson);
                log.warn("Error checking response for {} (Gladia attempt {}/{})",
                        gladiaData.getGladiaRequestId(), nextAttempt, MAX_ATTEMPTS, e);
            }
        }
    }
}

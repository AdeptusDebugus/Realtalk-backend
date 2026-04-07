package real.talk.service.llm;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import real.talk.model.dto.lesson.GeneratedPreset;
import real.talk.model.entity.GladiaData;
import real.talk.model.entity.Lesson;
import real.talk.model.entity.LlmData;
import real.talk.model.entity.enums.DataStatus;
import real.talk.model.entity.enums.LessonStatus;
import real.talk.service.gladia.GladiaService;
import real.talk.service.lesson.LessonService;

import java.time.Instant;
import java.util.List;

import static java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor;

@Service
@RequiredArgsConstructor
@Slf4j
public class LlmTaskScheduler {

    private static final int MAX_ATTEMPTS = 3;

    private final LessonService lessonService;
    private final GladiaService gladiaService;
    private final GptLessonService gptLessonService;
    private final LlmDataService llmDataService;

    @Scheduled(cron = "${llm.generate-lesson.cron}")
    public void processPendingLessons() {
        List<Lesson> processingLessons = lessonService.getBatchProcessingLessonsWithGladiaDone(10);

        if (processingLessons == null || processingLessons.isEmpty()) {
            return;
        }

        log.info("Starting concurrent processing for {} lessons", processingLessons.size());

        try (var executor = newVirtualThreadPerTaskExecutor()) {
            processingLessons.forEach(lesson -> executor.submit(() -> processLesson(lesson)));
        }

        log.info("Finished submitting batches to executor");
    }

    private void processLesson(Lesson lesson) {
        int nextAttempt = (lesson.getLlmAttempts() == null ? 0 : lesson.getLlmAttempts()) + 1;
        lesson.setLlmAttempts(nextAttempt);
        lessonService.saveLesson(lesson);

        try {
            log.info("Processing lesson id={} (LLM attempt {}/{})", lesson.getId(), nextAttempt, MAX_ATTEMPTS);
            GladiaData data = gladiaService.getGladiaDataByLessonIdAndStatusDone(lesson.getId())
                    .orElseThrow(() -> new RuntimeException("Gladia data not generated yet for lesson " + lesson.getId()));

            GeneratedPreset generatedLesson = gptLessonService.createLesson(lesson, data);

            LlmData llmData = new LlmData();
            llmData.setLesson(lesson);
            llmData.setStatus(DataStatus.DONE);
            llmData.setData(generatedLesson);
            llmData.setCreatedAt(Instant.now());
            llmDataService.save(llmData);
        } catch (Exception e) {
            if (nextAttempt >= MAX_ATTEMPTS) {
                lesson.setStatus(LessonStatus.ERROR);
                lessonService.saveLesson(lesson);
                log.error("Lesson {} reached max LLM attempts ({}). Marked as ERROR", lesson.getId(), MAX_ATTEMPTS, e);
            } else {
                log.warn("Lesson {} LLM attempt {}/{} failed. Will retry", lesson.getId(), nextAttempt, MAX_ATTEMPTS, e);
            }
        }
    }
}

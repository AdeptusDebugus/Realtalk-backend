package real.talk.service.lesson;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import real.talk.model.dto.lesson.GeneratedPreset;
import real.talk.model.dto.lesson.Tags;
import real.talk.model.entity.GladiaData;
import real.talk.model.entity.Lesson;
import real.talk.model.entity.LlmData;
import real.talk.model.entity.User;
import real.talk.model.entity.enums.LessonStatus;
import real.talk.service.llm.LlmDataService;
import real.talk.service.gladia.GladiaService;
import real.talk.service.user.UserService;

import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class LessonTaskScheduler {

    private final LlmDataService llmDataService;
    private final LessonService lessonService;
    private final GladiaService gladiaService;
    private final UserService userService;

    @Scheduled(cron = "${lesson.ready-lesson.cron}")
    public void processReadyLessons() {
        List<Lesson> lessonsWithLlmDone = lessonService.getLessonsWithLlmDone();
        if (lessonsWithLlmDone == null || lessonsWithLlmDone.isEmpty()) {
            log.info("Lessons with Llm Done not found");
            return;
        }

        log.info("Starting ready lessons check for {} tasks", lessonsWithLlmDone.size());
        lessonsWithLlmDone.forEach(this::processReadyLesson);
        log.info("Finished processing ready lessons");
    }

    private void processReadyLesson(Lesson lesson) {
        try {
            log.info("▶️ Обработка урока id={} (статус={})", lesson.getId(), lesson.getStatus());
            GladiaData gladia = gladiaService.getGladiaDataByLessonIdAndStatusDone(lesson.getId())
                    .orElseThrow(() -> {
                        log.error("❌ GladiaData не найден для lessonId={}", lesson.getId());
                        return new RuntimeException("GladiaData not found");
                    });
            LlmData llm = llmDataService.getLlmDataByLessonIdAndStatusDone(lesson.getId())
                    .orElseThrow(() -> {
                        log.error("❌ LlmData не найден для lessonId={}", lesson.getId());
                        return new RuntimeException("LlmData not found");
                    });

            GeneratedPreset lessonData = llm.getData();


            Tags tags = Tags.builder()
                    .lexical_fields(List.of(lessonData.getTag()))
                    .language(lesson.getLanguage())
                    .language_level(lesson.getLanguageLevel())
                    .preset(lesson.getPreset())
                    .build();

            lesson.setTags(tags);
            lesson.setData(lessonData);
            lesson.setStatus(LessonStatus.READY);
            lessonService.saveLesson(lesson);
            log.info("✅ Урок id={} обновлен и сохранен со статусом READY", lesson.getId());

            User user = userService.getUserById(lesson.getUser().getUserId());
            double audioDurationSeconds = gladia.getData() != null
                    && gladia.getData().getFile() != null
                    && gladia.getData().getFile().getAudioDuration() != null
                            ? gladia.getData().getFile().getAudioDuration()
                            : 0.0;
            double billableDurationSeconds = resolveBillableDurationSeconds(lesson, audioDurationSeconds);
            double currentDuration = user.getDuration() == null ? 0.0 : user.getDuration();
            int currentLessonCount = user.getLessonCount() == null ? 0 : user.getLessonCount();
            user.setDuration(currentDuration + billableDurationSeconds);
            user.setLessonCount(currentLessonCount + 1);

            // Subtract minutes from quota
            if (user.getLessonBuilderMinutes() != null) {
                int lessonMinutes = (int) Math.ceil(billableDurationSeconds / 60.0);
                user.setLessonBuilderMinutes(Math.max(0, user.getLessonBuilderMinutes() - lessonMinutes));
            }

            userService.saveUser(user);

            log.info("User [{}] updated: duration -> {}, lessons -> {}, minutes remaining -> {}",
                    user.getUserId(),
                    user.getDuration(),
                    user.getLessonCount(),
                    user.getLessonBuilderMinutes());
        } catch (Exception e) {
            log.error("Ошибка при обработке готового урока id={}", lesson.getId(), e);
        }
    }

    private double resolveBillableDurationSeconds(Lesson lesson, double audioDurationSeconds) {
        Double segmentStartMin = lesson.getSegmentStartMin();
        Double segmentEndMin = lesson.getSegmentEndMin();
        if (segmentStartMin == null || segmentEndMin == null) {
            return audioDurationSeconds;
        }
        double segmentStartSec = segmentStartMin * 60.0;
        double segmentEndSec = segmentEndMin * 60.0;

        double normalizedStart = Math.max(0.0, Math.min(segmentStartSec, audioDurationSeconds));
        double normalizedEnd = Math.max(0.0, Math.min(segmentEndSec, audioDurationSeconds));
        if (normalizedEnd <= normalizedStart) {
            return 0.0;
        }
        return normalizedEnd - normalizedStart;
    }
}

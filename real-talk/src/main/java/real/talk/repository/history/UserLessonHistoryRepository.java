package real.talk.repository.history;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;
import real.talk.model.dto.history.LessonHistoryResponse;
import real.talk.model.entity.Lesson;
import real.talk.model.entity.UserLessonHistory;
import real.talk.model.entity.enums.LessonAccess;
import real.talk.model.entity.enums.LessonStatus;

import java.time.Instant;
import java.util.UUID;

public interface UserLessonHistoryRepository extends JpaRepository<UserLessonHistory, Long> {
    long countUserLessonHistoriesByUserId(UUID userId);

    void deleteAllByLesson(Lesson lesson);

    @Query(
            """
    select new real.talk.model.dto.history.LessonHistoryResponse(
           l.id,
           l.youtubeUrl,
           l.lessonTopic,
           l.tags,
           l.createdAt,
           h.lastOpenedAt
       )
       from Lesson l
    left join UserLessonHistory h
        on h.lesson = l and h.userId = :userId
    where l.status = :status
      and l.access = :access
    order by h.lastOpenedAt desc nulls last
"""
    )
    Page<LessonHistoryResponse> getLessonsWithHistory(
            @Param("status") LessonStatus status,
            @Param("access") LessonAccess access,
            @Param("userId") UUID userId,
            Pageable pageable
    );

    @Modifying
    @Query("DELETE FROM UserLessonHistory h WHERE h.lastOpenedAt < :cutoff")
    int deleteOlderThan(@Param("cutoff") Instant cutoff);

    @Modifying
    @Transactional
    @Query(
            value = """
                    INSERT INTO user_lesson_history (user_id, lesson_id, last_opened_at, open_count)
                    VALUES (:userId, :lessonId, :now, 1)
                    ON CONFLICT (user_id, lesson_id)
                    DO UPDATE SET 
                        last_opened_at = EXCLUDED.last_opened_at,
                        open_count = user_lesson_history.open_count + 1
                    """,
            nativeQuery = true
    )
    void upsertLessonHistory(@Param("userId") UUID userId,
                             @Param("lessonId") UUID lessonId,
                             @Param("now") Instant now);
}

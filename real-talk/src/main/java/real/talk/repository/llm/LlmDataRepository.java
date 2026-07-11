package real.talk.repository.llm;

import org.springframework.data.jpa.repository.JpaRepository;
import real.talk.model.entity.Lesson;
import real.talk.model.entity.LlmData;
import real.talk.model.entity.enums.DataStatus;

import java.util.Optional;
import java.util.UUID;

public interface LlmDataRepository extends JpaRepository<LlmData, UUID> {

    Optional<LlmData> findByLessonId(UUID uuid);

    Optional<LlmData> findByLessonIdAndStatus(UUID uuid,  DataStatus status);

    void deleteByLesson(Lesson lesson);
}

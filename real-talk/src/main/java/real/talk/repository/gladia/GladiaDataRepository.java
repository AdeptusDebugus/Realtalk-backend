package real.talk.repository.gladia;

import org.springframework.data.jpa.repository.JpaRepository;
import real.talk.model.entity.Lesson;
import real.talk.model.entity.GladiaData;
import real.talk.model.entity.enums.DataStatus;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface GladiaDataRepository extends JpaRepository<GladiaData, UUID> {


    List<GladiaData> findGladiaDataByGladiaRequestId(UUID requestId);
    List<GladiaData> findGladiaDataByStatus(DataStatus status);
    Optional<GladiaData> findGladiaDataByLessonIdAndStatus(UUID lessonId, DataStatus status);
    void deleteByLesson(Lesson lesson);
}

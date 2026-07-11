package real.talk.repository.folder;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import real.talk.model.dto.folder.FolderLessonResponse;
import real.talk.model.entity.Lesson;
import real.talk.model.entity.Folder;
import real.talk.model.entity.FolderLesson;

import java.util.List;
import java.util.Optional;

public interface FolderLessonRepository extends JpaRepository<FolderLesson, Long> {
    List<FolderLesson> findAllByFolderId(Long folderId);
    Optional<FolderLesson> findByFolderAndLesson(Folder folder, Lesson lesson);
    void deleteAllByFolder(Folder folder);
    void deleteAllByLesson(Lesson lesson);
    int countByFolder(Folder folder);

    @Query("""
        select new real.talk.model.dto.folder.FolderLessonResponse(
            l.id,
            l.youtubeUrl,
            l.lessonTopic,
            l.tags,
            l.createdAt
        )
        from FolderLesson fl
        join fl.lesson l
        where fl.folder = :folder
        order by l.createdAt desc
    """)
    List<FolderLessonResponse> findLessonsByFolder(Folder folder);
}

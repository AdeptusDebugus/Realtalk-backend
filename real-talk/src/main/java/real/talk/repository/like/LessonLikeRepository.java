package real.talk.repository.like;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import real.talk.model.entity.Lesson;
import real.talk.model.entity.User;
import real.talk.model.entity.UserLessonLike;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface LessonLikeRepository extends JpaRepository<UserLessonLike, UUID> {
    Optional<UserLessonLike> findByUserAndLesson(User user, Lesson lesson);

    List<UserLessonLike> findAllByUser(User user);

    void deleteByUserAndLesson(User user, Lesson lesson);

    void deleteAllByLesson(Lesson lesson);

    boolean existsByUserAndLesson(User user, Lesson lesson);

    @Query("SELECT ll FROM UserLessonLike ll " +
            "JOIN FETCH ll.lesson l " +
            "WHERE ll.user = :user")
    List<UserLessonLike> findAllWithLessonByUser(User user);
    long countByUser(User user);
}

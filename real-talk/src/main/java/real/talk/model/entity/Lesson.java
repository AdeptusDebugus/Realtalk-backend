package real.talk.model.entity;

import jakarta.persistence.*;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import real.talk.model.dto.lesson.GeneratedPreset;

import real.talk.model.dto.lesson.Tags;
import real.talk.model.entity.enums.LessonAccess;
import real.talk.model.entity.enums.LessonStatus;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Getter
@Setter
@Entity
@Table(name = "lessons")
public class Lesson {
    @Id
    @Column(name = "lesson_id", nullable = false)
    private UUID id;

    @NotNull
    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "user_id", nullable = false)
    private User user;

    @Column(name = "language")
    private String language;

    @Size(max = 10)
    @Column(name = "language_level", length = 10)
    private String languageLevel;

    @Size(max = 255)
    @Column(name = "youtube_url")
    private String youtubeUrl;

    @Column(name = "segment_start_min")
    private Double segmentStartMin;

    @Column(name = "segment_end_min")
    private Double segmentEndMin;

    @Column(name = "tags")
    @JdbcTypeCode(SqlTypes.JSON)
    private Tags tags;

    @Column(name = "lesson_topic")
    private String lessonTopic;

    @Column(name = "preset", length = 100)
    private String preset;

    @Column(name = "grammar_topics")
    private List<String> grammarTopics;

    @NotNull
    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20)
    private LessonStatus status;

    @NotNull
    @Enumerated(EnumType.STRING)
    @Column(name = "access", nullable = false, length = 20)
    private LessonAccess access;

    @Column(name = "data")
    @JdbcTypeCode(SqlTypes.JSON)
    private GeneratedPreset data;

    @Column(name = "created_at")
    private Instant createdAt;

    @Column(name = "gladia_attempts", nullable = false)
    private Integer gladiaAttempts = 0;

    @Column(name = "llm_attempts", nullable = false)
    private Integer llmAttempts = 0;

    @ManyToMany(fetch = FetchType.LAZY)
    @JoinTable(name = "lesson_shared_users", joinColumns = @JoinColumn(name = "lesson_id"), inverseJoinColumns = @JoinColumn(name = "user_id"))
    private java.util.Set<User> sharedUsers = new java.util.HashSet<>();
}

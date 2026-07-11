package real.talk.model.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import real.talk.model.entity.enums.SubscriptionPlan;
import real.talk.model.entity.enums.SubscriptionStatus;

import java.time.Instant;
import java.util.UUID;

@Getter
@Setter
@Entity
@Table(name = "subscriptions")
public class Subscription {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "id", nullable = false)
    private UUID id;

    @OneToOne
    @JoinColumn(name = "user_id", referencedColumnName = "user_id")
    private User user;

    @Column(name = "paddle_subscription_id", unique = true)
    private String paddleSubscriptionId;

    @Column(name = "paddle_customer_id")
    private String paddleCustomerId;

    @Enumerated(EnumType.STRING)
    @Column(name = "status")
    private SubscriptionStatus status;

    @Enumerated(EnumType.STRING)
    @Column(name = "plan")
    private SubscriptionPlan plan;

    @Column(name = "next_billed_at")
    private Instant nextBilledAt;

    @Column(name = "trial_minutes_unlock_at")
    private Instant trialMinutesUnlockAt;

    @Column(name = "deferred_lesson_builder_minutes")
    private Integer deferredLessonBuilderMinutes = 0;

    @Column(name = "created_at")
    private Instant createdAt;

    @Column(name = "updated_at")
    private Instant updatedAt;
}

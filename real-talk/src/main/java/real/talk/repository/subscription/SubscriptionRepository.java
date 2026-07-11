package real.talk.repository.subscription;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import real.talk.model.entity.Subscription;
import real.talk.model.entity.enums.SubscriptionStatus;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface SubscriptionRepository extends JpaRepository<Subscription, UUID> {
    Optional<Subscription> findByPaddleSubscriptionId(String paddleSubscriptionId);

    List<Subscription> findByUserUserId(UUID userId);

    @Query("""
            select s from Subscription s
            where s.status in :statuses
              and s.nextBilledAt is not null
              and s.nextBilledAt <= :now
              and coalesce(s.user.lessonBuilderMinutes, 0) > 0
            """)
    List<Subscription> findExpiredSubscriptionsWithRemainingMinutes(
            @Param("statuses") Collection<SubscriptionStatus> statuses,
            @Param("now") Instant now);

    @Query("""
            select s from Subscription s
            where coalesce(s.deferredLessonBuilderMinutes, 0) > 0
              and s.trialMinutesUnlockAt is not null
              and s.trialMinutesUnlockAt <= :now
              and s.status in :statuses
            """)
    List<Subscription> findSubscriptionsWithUnlockedTrialMinutes(
            @Param("statuses") Collection<SubscriptionStatus> statuses,
            @Param("now") Instant now);
}

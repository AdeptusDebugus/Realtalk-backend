package real.talk.service.paddle;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import real.talk.model.entity.Subscription;
import real.talk.model.entity.User;
import real.talk.model.entity.enums.SubscriptionPlan;
import real.talk.model.entity.enums.SubscriptionStatus;
import real.talk.repository.subscription.SubscriptionRepository;
import real.talk.service.user.UserService;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class SubscriptionExpirationScheduler {

    private final SubscriptionRepository subscriptionRepository;
    private final UserService userService;

    @Scheduled(cron = "${paddle.subscription-expiration.cron:0 0 * * * *}")
    @Transactional
    public void resetLessonBuilderMinutesForExpiredCanceledSubscriptions() {
        Instant now = Instant.now();
        releaseUnlockedTrialMinutes(now);

        List<Subscription> expiredSubscriptions = subscriptionRepository.findExpiredSubscriptionsWithRemainingMinutes(
                List.of(SubscriptionStatus.canceled, SubscriptionStatus.deleted),
                now);

        for (Subscription subscription : expiredSubscriptions) {
            User user = subscription.getUser();
            if (user == null) {
                log.warn("Expired canceled subscription {} has no user", subscription.getPaddleSubscriptionId());
                continue;
            }

            if (hasAnotherCurrentBuilderSubscription(user.getUserId(), subscription.getId(), now)) {
                log.info("Keeping lesson builder minutes for user {} because another builder subscription is current",
                        user.getEmail());
                continue;
            }

            user.setLessonBuilderMinutes(0);
            userService.saveUser(user);
            log.info("Reset lesson builder minutes for user {} after subscription {} ended",
                    user.getEmail(), subscription.getPaddleSubscriptionId());
        }
    }

    private void releaseUnlockedTrialMinutes(Instant now) {
        List<Subscription> subscriptions = subscriptionRepository.findSubscriptionsWithUnlockedTrialMinutes(
                List.of(SubscriptionStatus.active, SubscriptionStatus.trialing),
                now);

        for (Subscription subscription : subscriptions) {
            User user = subscription.getUser();
            if (user == null) {
                log.warn("Subscription {} has unlocked trial minutes but no user",
                        subscription.getPaddleSubscriptionId());
                continue;
            }

            int deferredMinutes = subscription.getDeferredLessonBuilderMinutes() != null
                    ? subscription.getDeferredLessonBuilderMinutes()
                    : 0;
            if (deferredMinutes <= 0) {
                continue;
            }

            int currentMinutes = user.getLessonBuilderMinutes() != null ? user.getLessonBuilderMinutes() : 0;
            user.setLessonBuilderMinutes(currentMinutes + deferredMinutes);
            subscription.setDeferredLessonBuilderMinutes(0);
            subscription.setUpdatedAt(now);
            userService.saveUser(user);
            subscriptionRepository.save(subscription);

            log.info("Released {} deferred trial minutes for user {}", deferredMinutes, user.getEmail());
        }
    }

    private boolean hasAnotherCurrentBuilderSubscription(UUID userId, UUID expiringSubscriptionId, Instant now) {
        return subscriptionRepository.findByUserUserId(userId)
                .stream()
                .filter(subscription -> !subscription.getId().equals(expiringSubscriptionId))
                .filter(this::isBuilderPlan)
                .anyMatch(subscription -> isActiveOrTrialing(subscription) || isCanceledButStillCurrent(subscription, now));
    }

    private boolean isBuilderPlan(Subscription subscription) {
        return subscription.getPlan() == SubscriptionPlan.SMART
                || subscription.getPlan() == SubscriptionPlan.PLUS
                || subscription.getPlan() == SubscriptionPlan.PRO;
    }

    private boolean isActiveOrTrialing(Subscription subscription) {
        return subscription.getStatus() == SubscriptionStatus.active
                || subscription.getStatus() == SubscriptionStatus.trialing;
    }

    private boolean isCanceledButStillCurrent(Subscription subscription, Instant now) {
        return subscription.getStatus() == SubscriptionStatus.canceled
                && subscription.getNextBilledAt() != null
                && subscription.getNextBilledAt().isAfter(now);
    }
}

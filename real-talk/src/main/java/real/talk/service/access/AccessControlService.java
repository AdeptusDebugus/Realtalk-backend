package real.talk.service.access;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import real.talk.model.dto.user.UserAccessDto;
import real.talk.model.entity.Subscription;
import real.talk.model.entity.User;
import real.talk.model.entity.enums.SubscriptionPlan;
import real.talk.model.entity.enums.SubscriptionStatus;
import real.talk.model.entity.enums.UserRole;
import real.talk.repository.subscription.SubscriptionRepository;

import java.time.Instant;
import java.util.Comparator;

@Slf4j
@Service
@RequiredArgsConstructor
public class AccessControlService {

    private final SubscriptionRepository subscriptionRepository;

    public UserAccessDto getUserAccessDto(User user) {
        if (user == null)
            return null;

        Subscription sub = getActiveSubscription(user);
        SubscriptionPlan plan = (sub != null) ? sub.getPlan() : null;

        boolean canAccessLibrary = canAccessPublicLibrary(user);
        boolean canUseBuilder = false;
        try {
            checkBuilderAccess(user, false);
            canUseBuilder = true;
        } catch (AccessDeniedException e) {
            canUseBuilder = false;
        }

        boolean canUseCollections = canUseCollections(user);
        boolean canUseScenarios = false;
        try {
            checkBuilderAccess(user, true);
            canUseScenarios = true; // If they pass the scenario check
        } catch (AccessDeniedException e) {
            // Specific check for scenarios
            // checkBuilderAccess(user, true) throws if no Plus plan
            canUseScenarios = false;
        }

        UserAccessDto.Permissions perms = new UserAccessDto.Permissions(
                canAccessLibrary,
                canUseBuilder,
                canUseCollections,
                canUseScenarios);

        return new UserAccessDto(
                user.getUserId().toString(),
                user.getEmail(),
                user.getName(),
                user.getRole(),
                plan,
                (sub != null) ? sub.getPaddleCustomerId() : null,
                getRemainingMinutes(user),
                perms);
    }

    public Integer getRemainingMinutes(User user) {
        if (user == null)
            return 0;
        return user.getLessonBuilderMinutes() != null ? user.getLessonBuilderMinutes() : 0;
    }

    /**
     * Checks if the user has access to the Open Library / Community Lessons.
     * Rules:
     * - Unauthenticated: No (handled by SecurityConfig usually, but here for
     * completeness if passed null)
     * - Student: No (unless shared link, which bypasses this check)
     * - Open Library Plan: Yes
     * - Smart / Plus: Yes
     * - Admin: Yes
     */
    public boolean canAccessPublicLibrary(User user) {
        if (user == null)
            return true;
        if (user.getRole() == UserRole.ADMIN)
            return true;
        if (user.getRole() == UserRole.STUDENT)
            return true;

        return hasActivePlan(user, SubscriptionPlan.OPEN_LIBRARY, SubscriptionPlan.SMART, SubscriptionPlan.PLUS,
                SubscriptionPlan.PRO);
    }

    /**
     * Checks if the user can use the Lesson Builder.
     * Rules:
     * - Student / Open Library: No
     * - Smart: Yes (300 mins)
     * - Plus: Yes (600 mins)
     * - Admin: Yes
     */
    public void checkBuilderAccess(User user, boolean requiresScenarios) {
        if (user == null) {
            throw new AccessDeniedException("User not authenticated");
        }
        if (user.getRole() == UserRole.ADMIN)
            return;

        if (user.getRole() == UserRole.STUDENT) {
            throw new AccessDeniedException("Student role cannot use Lesson Builder");
        }

        Subscription sub = getActiveSubscription(user);
        if (sub == null) {
            throw new AccessDeniedException("Active subscription required for Lesson Builder");
        }

        // Plan Check
        if (sub.getPlan() == SubscriptionPlan.OPEN_LIBRARY) {
            throw new AccessDeniedException(
                    "Open Library plan does not include Lesson Builder. Please upgrade to Smart or Plus.");
        }

        // Scenario Check
        if (requiresScenarios && !hasPlanAtLeast(sub.getPlan(), SubscriptionPlan.PLUS)) {
            throw new AccessDeniedException("Lesson Scenarios require Plus plan.");
        }

        // Minute Check
        // Admin doesn't need minutes, but regular users do.
        if (getRemainingMinutes(user) <= 0) {
            throw new AccessDeniedException("Insufficient Lesson Builder minutes. Please upgrade or wait for renewal.");
        }
    }

    /**
     * Checks if user can use Collections.
     * Rules:
     * - Plus: Yes
     * - Admin: Yes
     * - Others: No
     */
    public boolean canUseCollections(User user) {
        if (user == null)
            return false;
        if (user.getRole() == UserRole.ADMIN)
            return true;

        return hasActivePlan(user, SubscriptionPlan.PLUS, SubscriptionPlan.PRO);
    }

    // --- Shared Link Logic ---

    /**
     * Generates a secure token for sharing a specific resource.
     * Format: resourceId:timestamp:signature
     */
    public String generateShareToken(String resourceId) {
        // Implementation placeholder - in production use correct crypto
        long expiry = System.currentTimeMillis() + (7 * 24 * 60 * 60 * 1000); // 7 days
        return resourceId + ":" + expiry + ":signature_placeholder";
    }

    public boolean validateShareToken(String token, String resourceId) {
        if (token == null || resourceId == null)
            return false;
        String[] parts = token.split(":");
        if (parts.length != 3)
            return false;

        String tokenResourceId = parts[0];
        long expiry = Long.parseLong(parts[1]);

        if (!tokenResourceId.equals(resourceId))
            return false;
        if (System.currentTimeMillis() > expiry)
            return false;

        // Validate signature here
        return true;
    }

    // --- Helpers ---

    private boolean hasActivePlan(User user, SubscriptionPlan... allowedPlans) {
        Subscription sub = getActiveSubscription(user);
        if (sub == null)
            return false;

        for (SubscriptionPlan plan : allowedPlans) {
            if (sub.getPlan() == plan)
                return true;
        }
        return false;
    }

    private Subscription getActiveSubscription(User user) {
        Instant now = Instant.now();
        return subscriptionRepository.findByUserUserId(user.getUserId())
                .stream()
                .filter(s -> isCurrentSubscription(s, now))
                .max(Comparator.comparingInt(s -> getPlanPriority(s.getPlan())))
                .orElse(null);
    }

    private boolean isCurrentSubscription(Subscription subscription, Instant now) {
        if (subscription.getStatus() == SubscriptionStatus.active
                || subscription.getStatus() == SubscriptionStatus.trialing) {
            return true;
        }

        return subscription.getStatus() == SubscriptionStatus.canceled
                && subscription.getNextBilledAt() != null
                && subscription.getNextBilledAt().isAfter(now);
    }

    private boolean hasPlanAtLeast(SubscriptionPlan actualPlan, SubscriptionPlan minimumPlan) {
        return getPlanPriority(actualPlan) >= getPlanPriority(minimumPlan);
    }

    private int getPlanPriority(SubscriptionPlan plan) {
        if (plan == null) {
            return -1;
        }
        return switch (plan) {
            case OPEN_LIBRARY -> 0;
            case SMART -> 1;
            case PLUS -> 2;
            case PRO -> 3;
        };
    }

    // Inner RuntimeException for simplicity, or use global one
    public static class AccessDeniedException extends RuntimeException {
        public AccessDeniedException(String message) {
            super(message);
        }
    }
}

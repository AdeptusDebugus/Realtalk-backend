package real.talk.service.paddle;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import real.talk.model.entity.Subscription;
import real.talk.model.entity.User;
import real.talk.model.entity.enums.SubscriptionPlan;
import real.talk.model.entity.enums.SubscriptionStatus;
import real.talk.repository.subscription.SubscriptionRepository;
import real.talk.service.user.UserService;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class PaddleService {

    private final SubscriptionRepository subscriptionRepository;
    private final UserService userService;
    private final ObjectMapper objectMapper;
    private final RestTemplate restTemplate;

    @Value("${paddle.webhook.secret}")
    private String webhookSecret;

    @Value("${paddle.api.key}")
    private String paddleApiKey;

    @Value("${paddle.api.url}")
    private String paddleApiUrl;

    // Plans map (Paddle Price ID -> Internal Plan)
    // You should preferably put these in properties or DB, but hardcoded for now is
    // fine for strict requirements
    // TODO: update with real PRICE IDs
    @Value("${paddle.price.smart:}")
    private String smartPriceId;

    @Value("${paddle.price.open-library:}")
    private String openLibraryPriceId;

    @Value("${paddle.price.plus:}")
    private String plusPriceId;

    @Value("${paddle.price.100-mins-start:}")
    private String mins100StartPriceId;

    @Value("${paddle.price.100-mins-plus:}")
    private String mins100PlusPriceId;

    private static final int TRIAL_AVAILABLE_MINUTES = 60;
    private static final long TRIAL_DEFERRED_MINUTES_DELAY_DAYS = 7;

    private SubscriptionPlan determinePlan(JsonNode data) {
        // Parse items to find priceId
        JsonNode items = data.path("items");
        if (items.isArray() && items.size() > 0) {
            for (JsonNode item : items) {
                String priceId = item.path("price").path("id").asText();
                if (priceId.equals(smartPriceId))
                    return SubscriptionPlan.SMART;
                if (priceId.equals(plusPriceId))
                    return SubscriptionPlan.PLUS;
                if (priceId.equals(openLibraryPriceId))
                    return SubscriptionPlan.OPEN_LIBRARY;
            }
        }
        return SubscriptionPlan.OPEN_LIBRARY; // default
    }

    private void updateUserLimits(User user, Subscription subscription) {
        SubscriptionPlan plan = subscription.getPlan();
        SubscriptionStatus status = subscription.getStatus();
        if (status != SubscriptionStatus.active && status != SubscriptionStatus.trialing) {
            return;
        }

        int planMinutes = getPlanLessonBuilderMinutes(plan);
        if (status == SubscriptionStatus.trialing && planMinutes > TRIAL_AVAILABLE_MINUTES) {
            Instant unlockAt = subscription.getTrialMinutesUnlockAt();
            if (unlockAt == null) {
                unlockAt = getTrialMinutesUnlockAt(subscription);
                subscription.setTrialMinutesUnlockAt(unlockAt);
                subscription.setDeferredLessonBuilderMinutes(planMinutes - TRIAL_AVAILABLE_MINUTES);
            }

            if (Instant.now().isBefore(unlockAt)) {
                ensureMinimumMinutes(user, TRIAL_AVAILABLE_MINUTES);
                return;
            }

            releaseDeferredTrialMinutes(user, subscription);
            return;
        }

        if (hasDeferredTrialMinutes(subscription) && isTrialMinutesUnlocked(subscription)) {
            releaseDeferredTrialMinutes(user, subscription);
            return;
        }

        if (hasDeferredTrialMinutes(subscription)) {
            ensureMinimumMinutes(user, TRIAL_AVAILABLE_MINUTES);
            return;
        }

        if (planMinutes > 0) {
            ensureMinimumMinutes(user, planMinutes);
        } else if (plan == SubscriptionPlan.OPEN_LIBRARY) {
            // Ensure no minutes are given (or reset to 0/standard free limit?)
            // For now, do nothing or set to 0?
            // If user downgrades from Smart to Open Library, they might need to lose
            // minutes.
            // But existing logic is "No paid roles".
            // Let's safe-guard against getting free minutes.
            // user.setLessonBuilderMinutes(0); // Optional: force reset on downgrade
        }
    }

    private boolean hasDeferredTrialMinutes(Subscription subscription) {
        return subscription.getDeferredLessonBuilderMinutes() != null
                && subscription.getDeferredLessonBuilderMinutes() > 0;
    }

    private boolean isTrialMinutesUnlocked(Subscription subscription) {
        return subscription.getTrialMinutesUnlockAt() != null
                && !Instant.now().isBefore(subscription.getTrialMinutesUnlockAt());
    }

    private int getPlanLessonBuilderMinutes(SubscriptionPlan plan) {
        if (plan == SubscriptionPlan.SMART) {
            return 300;
        }
        if (plan == SubscriptionPlan.PLUS) {
            return 600;
        }
        return 0;
    }

    private Instant getTrialMinutesUnlockAt(Subscription subscription) {
        Instant trialStartedAt = subscription.getCreatedAt() != null ? subscription.getCreatedAt() : Instant.now();
        return trialStartedAt.plus(TRIAL_DEFERRED_MINUTES_DELAY_DAYS, ChronoUnit.DAYS);
    }

    private void releaseDeferredTrialMinutes(User user, Subscription subscription) {
        int deferredMinutes = subscription.getDeferredLessonBuilderMinutes() != null
                ? subscription.getDeferredLessonBuilderMinutes()
                : 0;
        if (deferredMinutes <= 0) {
            return;
        }

        int currentMinutes = user.getLessonBuilderMinutes() != null ? user.getLessonBuilderMinutes() : 0;
        user.setLessonBuilderMinutes(currentMinutes + deferredMinutes);
        subscription.setDeferredLessonBuilderMinutes(0);
    }

    private void ensureMinimumMinutes(User user, int minutes) {
        if (user.getLessonBuilderMinutes() == null || user.getLessonBuilderMinutes() < minutes) {
            user.setLessonBuilderMinutes(minutes);
        }
    }

    @Transactional
    public boolean processWebhook(String signature, String payload) {
        if (!verifySignature(signature, payload)) {
            log.warn("Invalid Paddle webhook signature");
            return false;
        }

        try {
            JsonNode root = objectMapper.readTree(payload);
            String eventType = root.path("event_type").asText();
            JsonNode data = root.path("data");

            log.info("Processing Paddle event: {}", eventType);

            switch (eventType) {
                case "subscription.created":
                case "subscription.updated":
                    handleSubscriptionUpdate(data);
                    break;
                case "subscription.canceled":
                    handleSubscriptionCanceled(data);
                    break;
                case "transaction.completed":
                    handleTransactionCompleted(data);
                    break;
                default:
                    log.debug("Ignored event type: {}", eventType);
            }
            return true;
        } catch (Exception e) {
            log.error("Failed to parse webhook payload", e);
            throw new RuntimeException(e);
        }
    }

    private void handleSubscriptionUpdate(JsonNode data) {
        String paddleSubId = data.path("id").asText();
        String customerId = data.path("customer_id").asText();
        String statusStr = data.path("status").asText();

        // Find user
        // Try to find by existing subscription linking
        Optional<Subscription> existingSub = subscriptionRepository.findByPaddleSubscriptionId(paddleSubId);
        User user = null;

        if (existingSub.isPresent()) {
            user = existingSub.get().getUser();
        } else {
            // New subscription. Ensure we have a way to link to User.
            // Look for 'custom_data' -> 'email' (Preferred) or 'userId' (Legacy)
            JsonNode customData = data.path("custom_data");
            log.info("Inspecting Paddle Custom Data: {}", customData.toString());

            if (customData.has("email")) {
                String email = customData.get("email").asText();
                user = userService.getUserByEmail(email).orElse(null);
                if (user == null) {
                    log.warn("User not found by email: {}", email);
                }
            } else if (customData.has("userId")) {
                String userIdStr = customData.get("userId").asText();
                try {
                    java.util.UUID userId = java.util.UUID.fromString(userIdStr);
                    user = userService.getUserById(userId);
                } catch (Exception e) {
                    log.warn("Invalid userId UUID: {}", userIdStr);
                }
            } else {
                log.warn("No 'email' or 'userId' in custom_data");
            }
        }

        if (user == null) {
            log.error("Could not link subscription {} to any user", paddleSubId);
            return;
        }

        // Determine Plan
        SubscriptionPlan plan = determinePlan(data);
        SubscriptionStatus status = SubscriptionStatus.valueOf(statusStr);

        Subscription subscription = existingSub.orElse(new Subscription());
        subscription.setUser(user);
        subscription.setPaddleSubscriptionId(paddleSubId);
        subscription.setPaddleCustomerId(customerId);
        subscription.setStatus(status);
        subscription.setPlan(plan);
        Instant subscriptionPeriodEnd = getSubscriptionPeriodEnd(data);
        if (subscriptionPeriodEnd != null) {
            subscription.setNextBilledAt(subscriptionPeriodEnd);
        }
        subscription.setUpdatedAt(Instant.now());
        if (subscription.getCreatedAt() == null) {
            subscription.setCreatedAt(Instant.now());
        }

        // Update User fields
        user.setPaddleCustomerId(customerId);
        updateUserLimits(user, subscription);
        subscriptionRepository.save(subscription);
        userService.saveUser(user);
    }

    private void handleTransactionCompleted(JsonNode data) {
        log.info("Handling transaction.completed: {}", data.path("id").asText());

        // Parse items to see if it's one of our "Add minutes" products.
        // Paddle transaction items contain price.id, while details.line_items contain price_id.
        JsonNode items = data.path("items");
        boolean isAddMinutes = false;
        int totalMinutesToAdd = 0;
        if (items.isArray()) {
            for (JsonNode item : items) {
                String priceId = item.path("price").path("id").asText();
                if (priceId == null || priceId.isEmpty()) {
                    priceId = item.path("price_id").asText();
                }
                int quantity = item.path("quantity").asInt(1);
                if (priceId.equals(mins100StartPriceId) || priceId.equals(mins100PlusPriceId)) {
                    isAddMinutes = true;
                    totalMinutesToAdd += 100 * quantity;
                }
            }
        }

        // Fallback in case event producer only sends details.line_items.
        if (!isAddMinutes) {
            JsonNode lineItems = data.path("details").path("line_items");
            if (lineItems.isArray()) {
                for (JsonNode lineItem : lineItems) {
                    String priceId = lineItem.path("price_id").asText();
                    int quantity = lineItem.path("quantity").asInt(1);
                    if (priceId.equals(mins100StartPriceId) || priceId.equals(mins100PlusPriceId)) {
                        isAddMinutes = true;
                        totalMinutesToAdd += 100 * quantity;
                    }
                }
            }
        }

        if (!isAddMinutes) {
            log.info("Transaction does not contain 'Add minutes' product. Ignoring.");
            return;
        }

        if (totalMinutesToAdd <= 0) {
            totalMinutesToAdd = 100;
        }

        // Find user by custom_data.email or customer_id.
        JsonNode customData = data.path("custom_data");
        User user = null;
        if (customData.has("email")) {
            String email = customData.get("email").asText();
            user = userService.getUserByEmail(email).orElse(null);
        }

        if (user == null) {
            String customerId = data.path("customer_id").asText();
            if (customerId != null && !customerId.isEmpty()) {
                user = userService.getUserByPaddleCustomerId(customerId).orElse(null);
            } else {
                log.warn("No email in custom_data and no customer_id for transaction {}", data.path("id").asText());
            }
        }

        if (user == null) {
            log.error("Could not link transaction {} to any user. custom_data: {}, customer_id: {}",
                    data.path("id").asText(), customData.toString(), data.path("customer_id").asText());
            return;
        }

        // Increment minutes
        int currentMinutes = user.getLessonBuilderMinutes() != null ? user.getLessonBuilderMinutes() : 0;
        user.setLessonBuilderMinutes(currentMinutes + totalMinutesToAdd);
        userService.saveUser(user);
        log.info("Added {} minutes to user {}. New total: {}",
                totalMinutesToAdd, user.getEmail(), user.getLessonBuilderMinutes());
    }

    private Instant getSubscriptionPeriodEnd(JsonNode data) {
        Instant nextBilledAt = parseInstant(data.path("next_billed_at").asText());
        if (nextBilledAt != null) {
            return nextBilledAt;
        }

        return parseInstant(data.path("current_billing_period").path("ends_at").asText());
    }

    private Instant parseInstant(String value) {
        if (value == null || value.isEmpty() || value.equals("null")) {
            return null;
        }
        return Instant.parse(value);
    }

    private void handleSubscriptionCanceled(JsonNode data) {
        String paddleSubId = data.path("id").asText();
        Optional<Subscription> subOpt = subscriptionRepository.findByPaddleSubscriptionId(paddleSubId);
        if (subOpt.isPresent()) {
            Subscription sub = subOpt.get();
            sub.setStatus(SubscriptionStatus.canceled);
            sub.setUpdatedAt(Instant.now());
            subscriptionRepository.save(sub);

            // Revoke access logic if immediate, but usually cancellation is "at end of
            // period"
            // Paddle "subscription.canceled" usually means it IS canceled now or scheduled?
            // Actually check 'status' field in payload. If it's 'deleted' or 'canceled'.
            // The handler above handles "updated" so maybe we just rely on that unless
            // specifically canceled event has different structure.
            // But we already map status. so re-using handleSubscriptionUpdate might be
            // safer if data structure is same.
            // The 'data' for canceled event is usually the subscription object too.
            handleSubscriptionUpdate(data);
        }
    }

    @Transactional(readOnly = true)
    public Subscription getSubscription(User user) {
        return subscriptionRepository.findByUserUserId(user.getUserId())
                .stream()
                .filter(s -> s.getStatus() == SubscriptionStatus.active)
                .findFirst()
                .orElse(null);
    }

    public String getPortalSession(User user) {
        if (user.getPaddleCustomerId() == null) {
            log.warn("User {} does not have a Paddle Customer ID", user.getEmail());
            return null;
        }

        String apiKey = paddleApiKey != null ? paddleApiKey.trim() : null;
        if (apiKey == null || apiKey.isEmpty() || apiKey.startsWith("${")) {
            log.error("Paddle API Key is NOT configured or remains an unresolved placeholder: {}", apiKey);
            return null;
        }

        try {
            String url = paddleApiUrl + "/customers/" + user.getPaddleCustomerId() + "/portal-sessions";

            HttpHeaders headers = new HttpHeaders();
            headers.set("Authorization", "Bearer " + apiKey);
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));

            HttpEntity<String> entity = new HttpEntity<>("{}", headers);

            log.info("Requesting Paddle Portal session for customer: {} (URL: {})", user.getPaddleCustomerId(), url);
            ResponseEntity<JsonNode> response = restTemplate.postForEntity(url, entity, JsonNode.class);

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                String portalUrl = response.getBody().path("data").path("urls").path("general").path("overview")
                        .asText();
                log.info("Successfully generated Paddle Portal URL");
                return portalUrl;
            } else {
                log.error("Failed to generate Paddle Portal session. Status: {}. Response: {}",
                        response.getStatusCode(), response.getBody());
                return null;
            }
        } catch (org.springframework.web.client.HttpClientErrorException e) {
            log.error("Paddle API error: {} - {}", e.getStatusCode(), e.getResponseBodyAsString());
            return null;
        } catch (Exception e) {
            log.error("Error generating Paddle Portal session", e);
            return null;
        }
    }

    private boolean verifySignature(String signature, String payload) {
        try {
            // Paddle signature format: ts=...,h1=...
            String[] parts = signature.split(";");
            String ts = null;
            String h1 = null;
            for (String part : parts) {
                if (part.startsWith("ts="))
                    ts = part.substring(3);
                if (part.startsWith("h1="))
                    h1 = part.substring(3);
            }

            if (ts == null || h1 == null)
                return false;

            String signedPayload = ts + ":" + payload;
            SecretKeySpec secretKeySpec = new SecretKeySpec(webhookSecret.getBytes(StandardCharsets.UTF_8),
                    "HmacSHA256");
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(secretKeySpec);
            byte[] hashBytes = mac.doFinal(signedPayload.getBytes(StandardCharsets.UTF_8));

            // Hex encode
            StringBuilder hexString = new StringBuilder();
            for (byte b : hashBytes) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1)
                    hexString.append('0');
                hexString.append(hex);
            }

            return hexString.toString().equals(h1);
        } catch (Exception e) {
            log.error("Signature verification error", e);
            return false;
        }
    }
}

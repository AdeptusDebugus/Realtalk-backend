package real.talk.service.gladia;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import real.talk.model.dto.gladia.PreRecorderRequest;
import real.talk.model.dto.gladia.PreRecorderResponse;
import real.talk.model.dto.gladia.TranscriptionResultResponse;

import java.util.UUID;

import static real.talk.util.constants.Headers.GLADIA_KEY_HEADER;
import static real.talk.util.constants.URLs.GLADIA_PRE_RECORDER_URL;

@Service
@Slf4j
@RequiredArgsConstructor
public class TranscriptionService {

    @Value("${gladia.api.key}")
    private String apiKey;

    private final RestTemplate restTemplate;

    public PreRecorderResponse transcribeAudio(String audioUrl) {
        PreRecorderRequest request = PreRecorderRequest.builder()
                .audioUrl(audioUrl)
                .build();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set(GLADIA_KEY_HEADER, apiKey);

        HttpEntity<PreRecorderRequest> entity = new HttpEntity<>(request, headers);

        ResponseEntity<PreRecorderResponse> response = restTemplate.exchange(
                GLADIA_PRE_RECORDER_URL,
                HttpMethod.POST,
                entity,
                PreRecorderResponse.class
        );

        if (response.getStatusCode().is2xxSuccessful()) {
            return response.getBody();
        } else if (response.getStatusCode().is4xxClientError()) {
            throw new RuntimeException("Client Error: " + response.getBody());
        } else {
            throw new RuntimeException("Server Error: " + response.getStatusCode());
        }
    }

    public TranscriptionResultResponse getTranscriptionResult(UUID transcriptionId) {
        HttpHeaders headers = new HttpHeaders();
        headers.set(GLADIA_KEY_HEADER, apiKey);

        HttpEntity<Void> entity = new HttpEntity<>(headers);

        ResponseEntity<TranscriptionResultResponse> response = restTemplate.exchange(
                GLADIA_PRE_RECORDER_URL + "/" + transcriptionId,
                HttpMethod.GET,
                entity,
                TranscriptionResultResponse.class
        );

        if (response.getStatusCode().is2xxSuccessful()) {
            return response.getBody();
        } else if (response.getStatusCode().is4xxClientError()) {
            throw new RuntimeException("Client Error: " + response.getStatusCode());
        } else {
            throw new RuntimeException("Server Error: " + response.getStatusCode());
        }
    }
}

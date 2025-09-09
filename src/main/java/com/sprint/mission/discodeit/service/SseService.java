package com.sprint.mission.discodeit.service;

import com.sprint.mission.discodeit.dto.data.SseMessage;
import com.sprint.mission.discodeit.repository.SseEmitterRepository;
import com.sprint.mission.discodeit.repository.SseMessageRepository;
import java.util.Collection;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@Slf4j
@Service
@RequiredArgsConstructor
public class SseService {

  private final SseEmitterRepository sseEmitterRepository;
  private final SseMessageRepository sseMessageRepository;

  public SseEmitter connect(UUID receiverId, UUID lastEventId) {
    log.debug("SSE 연결 생성: receiverId={}, lastEventId={}", receiverId, lastEventId);

    SseEmitter sseEmitter = new SseEmitter(0L); // 타임아웃 없음
    
    // 연결 완료 시 저장소에서 제거
    sseEmitter.onCompletion(() -> {
      log.debug("SSE 연결 완료: receiverId={}", receiverId);
      sseEmitterRepository.remove(receiverId, sseEmitter);
    });
    
    // 타임아웃 시 저장소에서 제거
    sseEmitter.onTimeout(() -> {
      log.debug("SSE 연결 타임아웃: receiverId={}", receiverId);
      sseEmitterRepository.remove(receiverId, sseEmitter);
    });
    
    // 에러 시 저장소에서 제거
    sseEmitter.onError((throwable) -> {
      log.warn("SSE 연결 에러: receiverId={}, error={}", receiverId, throwable.getMessage());
      sseEmitterRepository.remove(receiverId, sseEmitter);
    });

    // 저장소에 추가
    sseEmitterRepository.add(receiverId, sseEmitter);

    // 이벤트 유실 복원
    if (lastEventId != null) {
      sendMissedEvents(receiverId, lastEventId);
    }

    // 초기 ping 전송
    ping(sseEmitter);

    return sseEmitter;
  }

  public void send(Collection<UUID> receiverIds, String eventName, Object data) {
    log.debug("SSE 메시지 전송: receiverIds={}, eventName={}", receiverIds, eventName);

    UUID eventId = UUID.randomUUID();
    SseMessage message = new SseMessage(eventId, eventName, data);
    
    // 메시지 저장
    sseMessageRepository.save(message);

    // 각 수신자에게 전송
    for (UUID receiverId : receiverIds) {
      sseEmitterRepository.getSseEmitters(receiverId).forEach(sseEmitter -> {
        try {
          sseEmitter.send(SseEmitter.event()
              .id(eventId.toString())
              .name(eventName)
              .data(data));
        } catch (Exception e) {
          log.warn("SSE 메시지 전송 실패: receiverId={}, eventId={}, error={}", 
              receiverId, eventId, e.getMessage());
          sseEmitterRepository.remove(receiverId, sseEmitter);
        }
      });
    }
  }

  public void broadcast(String eventName, Object data) {
    log.debug("SSE 브로드캐스트: eventName={}", eventName);

    UUID eventId = UUID.randomUUID();
    SseMessage message = new SseMessage(eventId, eventName, data);
    
    // 메시지 저장
    sseMessageRepository.save(message);

    // 모든 연결된 사용자에게 전송
    sseEmitterRepository.getAllSseEmitters().forEach((receiverId, sseEmitters) -> {
      sseEmitters.forEach(sseEmitter -> {
        try {
          sseEmitter.send(SseEmitter.event()
              .id(eventId.toString())
              .name(eventName)
              .data(data));
        } catch (Exception e) {
          log.warn("SSE 브로드캐스트 전송 실패: receiverId={}, eventId={}, error={}", 
              receiverId, eventId, e.getMessage());
          sseEmitterRepository.remove(receiverId, sseEmitter);
        }
      });
    });
  }

  @Scheduled(fixedDelay = 1000 * 60 * 30) // 30분마다 실행
  public void cleanUp() {
    log.debug("SSE 연결 정리 시작");

    sseEmitterRepository.getAllSseEmitters().forEach((receiverId, sseEmitters) -> {
      sseEmitters.removeIf(sseEmitter -> !ping(sseEmitter));
    });

    log.debug("SSE 연결 정리 완료");
  }

  private boolean ping(SseEmitter sseEmitter) {
    try {
      sseEmitter.send(SseEmitter.event()
          .name("ping")
          .data("ping"));
      return true;
    } catch (Exception e) {
      log.debug("SSE ping 실패: error={}", e.getMessage());
      return false;
    }
  }

  private void sendMissedEvents(UUID receiverId, UUID lastEventId) {
    log.debug("이벤트 유실 복원: receiverId={}, lastEventId={}", receiverId, lastEventId);

    sseMessageRepository.getMessagesAfter(lastEventId).forEach(message -> {
      sseEmitterRepository.getSseEmitters(receiverId).forEach(sseEmitter -> {
        try {
          sseEmitter.send(SseEmitter.event()
              .id(message.id().toString())
              .name(message.name())
              .data(message.data()));
        } catch (Exception e) {
          log.warn("이벤트 유실 복원 전송 실패: receiverId={}, eventId={}, error={}", 
              receiverId, message.id(), e.getMessage());
        }
      });
    });
  }
}

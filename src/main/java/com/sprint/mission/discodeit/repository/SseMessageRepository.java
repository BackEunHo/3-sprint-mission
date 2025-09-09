package com.sprint.mission.discodeit.repository;

import com.sprint.mission.discodeit.dto.data.SseMessage;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.Collectors;
import org.springframework.stereotype.Repository;

@Repository
public class SseMessageRepository {

  private final ConcurrentLinkedDeque<UUID> eventIdQueue = new ConcurrentLinkedDeque<>();
  private final Map<UUID, SseMessage> messages = new ConcurrentHashMap<>();
  private static final int MAX_MESSAGES = 1000; // 최대 저장 메시지 수

  public void save(SseMessage message) {
    // 큐에 이벤트 ID 추가
    eventIdQueue.offer(message.id());
    
    // 메시지 저장
    messages.put(message.id(), message);
    
    // 최대 메시지 수 초과 시 오래된 메시지 제거
    while (eventIdQueue.size() > MAX_MESSAGES) {
      UUID oldestEventId = eventIdQueue.poll();
      if (oldestEventId != null) {
        messages.remove(oldestEventId);
      }
    }
  }

  public List<SseMessage> getMessagesAfter(UUID lastEventId) {
    return eventIdQueue.stream()
        .filter(eventId -> eventId.compareTo(lastEventId) > 0)
        .map(messages::get)
        .filter(message -> message != null)
        .collect(Collectors.toList());
  }

  public SseMessage findById(UUID eventId) {
    return messages.get(eventId);
  }

  public List<SseMessage> getAllMessages() {
    return eventIdQueue.stream()
        .map(messages::get)
        .filter(message -> message != null)
        .collect(Collectors.toList());
  }

  public int size() {
    return messages.size();
  }
}

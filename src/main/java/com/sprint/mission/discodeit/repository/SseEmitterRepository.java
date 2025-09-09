package com.sprint.mission.discodeit.repository;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Repository;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@Repository
public class SseEmitterRepository {

  private final Map<UUID, List<SseEmitter>> data = new ConcurrentHashMap<>();

  public void add(UUID userId, SseEmitter sseEmitter) {
    data.computeIfAbsent(userId, k -> new java.util.concurrent.CopyOnWriteArrayList<>())
        .add(sseEmitter);
  }

  public void remove(UUID userId, SseEmitter sseEmitter) {
    List<SseEmitter> sseEmitters = data.get(userId);
    if (sseEmitters != null) {
      sseEmitters.remove(sseEmitter);
      if (sseEmitters.isEmpty()) {
        data.remove(userId);
      }
    }
  }

  public List<SseEmitter> getSseEmitters(UUID userId) {
    return data.getOrDefault(userId, List.of());
  }

  public Map<UUID, List<SseEmitter>> getAllSseEmitters() {
    return new ConcurrentHashMap<>(data);
  }

  public boolean hasConnection(UUID userId) {
    List<SseEmitter> sseEmitters = data.get(userId);
    return sseEmitters != null && !sseEmitters.isEmpty();
  }
}

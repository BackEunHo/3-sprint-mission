package com.sprint.mission.discodeit.controller;

import com.sprint.mission.discodeit.security.jwt.JwtTokenProvider;
import com.sprint.mission.discodeit.service.SseService;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@Slf4j
@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class SseController {

  private final SseService sseService;
  private final JwtTokenProvider jwtTokenProvider;

  @GetMapping(value = "/sse", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
  public ResponseEntity<SseEmitter> connect(
      Authentication authentication,
      @RequestHeader(value = "Authorization") String authorization,
      @RequestHeader(value = "Last-Event-ID", required = false) String lastEventId) {
    
    try {
      // Authorization 헤더에서 JWT 토큰 추출
      String token = authorization.replace("Bearer ", "");
      
      // JWT 토큰에서 userId 추출
      UUID userId = jwtTokenProvider.getUserId(token);
      UUID lastEventIdUuid = lastEventId != null ? UUID.fromString(lastEventId) : null;
      
      log.debug("SSE 연결 요청: userId={}, lastEventId={}", userId, lastEventId);
      
      SseEmitter sseEmitter = sseService.connect(userId, lastEventIdUuid);
      
      log.debug("SSE 연결 생성 완료: userId={}", userId);
      
      return ResponseEntity.ok(sseEmitter);
    } catch (Exception e) {
      log.error("SSE 연결 실패: error={}", e.getMessage(), e);
      return ResponseEntity.status(500).build();
    }
  }
}

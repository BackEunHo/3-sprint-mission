package com.sprint.mission.discodeit.controller;

import com.sprint.mission.discodeit.dto.data.MessageDto;
import com.sprint.mission.discodeit.dto.request.MessageCreateRequest;
import com.sprint.mission.discodeit.service.MessageService;
import java.util.Collections;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Controller;

@Slf4j
@Controller
@RequiredArgsConstructor
public class MessageWebSocketController {
    
  private final MessageService messageService;

  @MessageMapping("/messages")
  public MessageDto handleMessage(@Payload MessageCreateRequest request) {
    log.debug("웹소켓 메시지 수신: request={}", request);

    MessageDto messageDto = messageService.create(request, Collections.emptyList());
    
    log.debug("웹소켓 메시지 처리 완료: messageId={}, channelId={}", 
        messageDto.id(), messageDto.channelId());
    
    return messageDto;
  }
}

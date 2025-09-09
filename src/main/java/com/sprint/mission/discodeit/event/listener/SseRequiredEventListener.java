package com.sprint.mission.discodeit.event.listener;

import com.sprint.mission.discodeit.dto.data.ChannelDto;
import com.sprint.mission.discodeit.dto.data.MessageDto;
import com.sprint.mission.discodeit.dto.data.NotificationDto;
import com.sprint.mission.discodeit.entity.ChannelType;
import com.sprint.mission.discodeit.entity.Role;
import com.sprint.mission.discodeit.event.message.BinaryContentUpdatedEvent;
import com.sprint.mission.discodeit.event.message.ChannelCreatedEvent;
import com.sprint.mission.discodeit.event.message.ChannelUpdatedEvent;
import com.sprint.mission.discodeit.event.message.ChannelDeletedEvent;
import com.sprint.mission.discodeit.event.message.MessageCreatedEvent;
import com.sprint.mission.discodeit.event.message.RoleUpdatedEvent;
import com.sprint.mission.discodeit.event.message.S3UploadFailedEvent;
import com.sprint.mission.discodeit.event.message.UserCreatedEvent;
import com.sprint.mission.discodeit.event.message.UserUpdatedEvent;
import com.sprint.mission.discodeit.event.message.UserDeletedEvent;
import com.sprint.mission.discodeit.repository.ReadStatusRepository;
import com.sprint.mission.discodeit.repository.UserRepository;
import com.sprint.mission.discodeit.service.BinaryContentService;
import com.sprint.mission.discodeit.service.ChannelService;
import com.sprint.mission.discodeit.service.NotificationService;
import com.sprint.mission.discodeit.service.SseService;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionalEventListener;

@Slf4j
@Component
@RequiredArgsConstructor
public class SseRequiredEventListener {

  private final NotificationService notificationService;
  private final ReadStatusRepository readStatusRepository;
  private final ChannelService channelService;
  private final UserRepository userRepository;
  private final SseService sseService;
  private final BinaryContentService binaryContentService;

  @Value("${discodeit.admin.username}")
  private String adminUsername;

  @Async("eventTaskExecutor")
  @TransactionalEventListener(phase = org.springframework.transaction.event.TransactionPhase.AFTER_COMMIT)
  public void on(MessageCreatedEvent event) {
    MessageDto message = event.getData();
    UUID channelId = message.channelId();
    ChannelDto channel = channelService.find(channelId);

    Set<UUID> receiverIds = readStatusRepository.findAllByChannelIdAndNotificationEnabledTrue(
            channelId)
        .stream().map(readStatus -> readStatus.getUser().getId())
        .filter(receiverId -> !receiverId.equals(message.author().id()))
        .collect(Collectors.toSet());
    
    String title = message.author().username()
        .concat(
            channel.type().equals(ChannelType.PUBLIC) ?
                String.format(" (#%s)", channel.name()) : ""
        );
    String content = message.content();

    // 알림 생성
    notificationService.create(receiverIds, title, content);
    
    // SSE 이벤트 전송을 위해 알림 정보를 다시 조회
    // 실제로는 알림 생성 후 바로 SSE 이벤트를 전송하는 것이 더 효율적이지만,
    // 현재 구조에서는 알림 생성 후 조회하는 방식으로 구현
    receiverIds.forEach(receiverId -> {
      // 최신 알림을 조회하여 SSE로 전송
      var notifications = notificationService.findAllByReceiverId(receiverId);
      if (!notifications.isEmpty()) {
        NotificationDto latestNotification = notifications.get(0); // 가장 최신 알림
        sseService.send(Set.of(receiverId), "notifications.created", latestNotification);
        log.debug("SSE 알림 이벤트 전송: receiverId={}, notificationId={}", 
            receiverId, latestNotification.id());
      }
    });
  }

  @Async("eventTaskExecutor")
  @TransactionalEventListener(phase = org.springframework.transaction.event.TransactionPhase.AFTER_COMMIT)
  public void on(RoleUpdatedEvent event) {
    UUID userId = event.getUserId();
    Role from = event.getFrom();
    Role to = event.getTo();

    String title = "권한이 변경되었습니다.";
    String content = String.format("%s -> %s", from.name(), to.name());

    // 알림 생성
    notificationService.create(Set.of(userId), title, content);
    
    // SSE 이벤트 전송
    var notifications = notificationService.findAllByReceiverId(userId);
    if (!notifications.isEmpty()) {
      NotificationDto latestNotification = notifications.get(0);
      sseService.send(Set.of(userId), "notifications.created", latestNotification);
      log.debug("SSE 권한 변경 알림 이벤트 전송: userId={}, notificationId={}", 
          userId, latestNotification.id());
    }
  }

  @Async("eventTaskExecutor")
  @EventListener
  public void on(S3UploadFailedEvent event) {
    String requestId = event.getRequestId();
    UUID binaryContentId = event.getBinaryContentId();
    Throwable e = event.getE();

    String title = "S3 파일 업로드 실패";

    StringBuffer sb = new StringBuffer();
    sb.append("RequestId: ").append(requestId).append("\n");
    sb.append("BinaryContentId: ").append(binaryContentId).append("\n");
    sb.append("Error: ").append(e.getMessage()).append("\n");
    String content = sb.toString();

    Set<UUID> receiverIds = userRepository.findByUsername(adminUsername)
        .map(user -> Set.of(user.getId()))
        .orElse(Set.of());

    // 알림 생성
    notificationService.create(receiverIds, title, content);
    
    // SSE 이벤트 전송
    receiverIds.forEach(receiverId -> {
      var notifications = notificationService.findAllByReceiverId(receiverId);
      if (!notifications.isEmpty()) {
        NotificationDto latestNotification = notifications.get(0);
        sseService.send(Set.of(receiverId), "notifications.created", latestNotification);
        log.debug("SSE S3 업로드 실패 알림 이벤트 전송: receiverId={}, notificationId={}", 
            receiverId, latestNotification.id());
      }
    });
  }

  @Async("eventTaskExecutor")
  @TransactionalEventListener(phase = org.springframework.transaction.event.TransactionPhase.AFTER_COMMIT)
  public void on(BinaryContentUpdatedEvent event) {
    log.debug("BinaryContent 상태 변경 이벤트 처리: fromStatus={}, toStatus={}", 
        event.getFromStatus(), event.getToStatus());

    // 상태가 변경된 BinaryContent 정보를 조회
    var binaryContentDto = binaryContentService.find(event.getTo().getId());
    
    // 파일 업로드 상태 변경을 모든 연결된 사용자에게 브로드캐스트
    sseService.broadcast("binaryContents.updated", binaryContentDto);
    
    log.debug("SSE 파일 업로드 상태 변경 이벤트 전송: binaryContentId={}, status={}", 
        binaryContentDto.id(), binaryContentDto.status());
  }

  @Async("eventTaskExecutor")
  @TransactionalEventListener(phase = org.springframework.transaction.event.TransactionPhase.AFTER_COMMIT)
  public void on(ChannelCreatedEvent event) {
    log.debug("채널 생성 이벤트 처리: channelId={}", event.getData().id());
    
    // 채널 생성을 모든 연결된 사용자에게 브로드캐스트
    sseService.broadcast("channels.created", event.getData());
    
    log.debug("SSE 채널 생성 이벤트 전송: channelId={}", event.getData().id());
  }

  @Async("eventTaskExecutor")
  @TransactionalEventListener(phase = org.springframework.transaction.event.TransactionPhase.AFTER_COMMIT)
  public void on(ChannelUpdatedEvent event) {
    log.debug("채널 수정 이벤트 처리: channelId={}", event.getTo().id());
    
    // 채널 수정을 모든 연결된 사용자에게 브로드캐스트
    sseService.broadcast("channels.updated", event.getTo());
    
    log.debug("SSE 채널 수정 이벤트 전송: channelId={}", event.getTo().id());
  }

  @Async("eventTaskExecutor")
  @TransactionalEventListener(phase = org.springframework.transaction.event.TransactionPhase.AFTER_COMMIT)
  public void on(ChannelDeletedEvent event) {
    log.debug("채널 삭제 이벤트 처리: channelId={}", event.getData().id());
    
    // 채널 삭제를 모든 연결된 사용자에게 브로드캐스트
    sseService.broadcast("channels.deleted", event.getData());
    
    log.debug("SSE 채널 삭제 이벤트 전송: channelId={}", event.getData().id());
  }

  @Async("eventTaskExecutor")
  @TransactionalEventListener(phase = org.springframework.transaction.event.TransactionPhase.AFTER_COMMIT)
  public void on(UserCreatedEvent event) {
    log.debug("사용자 생성 이벤트 처리: userId={}", event.getData().id());
    
    // 사용자 생성을 모든 연결된 사용자에게 브로드캐스트
    sseService.broadcast("users.created", event.getData());
    
    log.debug("SSE 사용자 생성 이벤트 전송: userId={}", event.getData().id());
  }

  @Async("eventTaskExecutor")
  @TransactionalEventListener(phase = org.springframework.transaction.event.TransactionPhase.AFTER_COMMIT)
  public void on(UserUpdatedEvent event) {
    log.debug("사용자 수정 이벤트 처리: userId={}", event.getTo().id());
    
    // 사용자 수정을 모든 연결된 사용자에게 브로드캐스트
    sseService.broadcast("users.updated", event.getTo());
    
    log.debug("SSE 사용자 수정 이벤트 전송: userId={}", event.getTo().id());
  }

  @Async("eventTaskExecutor")
  @TransactionalEventListener(phase = org.springframework.transaction.event.TransactionPhase.AFTER_COMMIT)
  public void on(UserDeletedEvent event) {
    log.debug("사용자 삭제 이벤트 처리: userId={}", event.getData().id());
    
    // 사용자 삭제를 모든 연결된 사용자에게 브로드캐스트
    sseService.broadcast("users.deleted", event.getData());
    
    log.debug("SSE 사용자 삭제 이벤트 전송: userId={}", event.getData().id());
  }
}

package com.sprint.mission.discodeit.event.message;

import com.sprint.mission.discodeit.entity.BinaryContent;
import com.sprint.mission.discodeit.entity.BinaryContentStatus;
import java.time.Instant;
import lombok.Getter;

@Getter
public class BinaryContentUpdatedEvent extends UpdatedEvent<BinaryContent> {

  private final BinaryContentStatus fromStatus;
  private final BinaryContentStatus toStatus;

  public BinaryContentUpdatedEvent(BinaryContent from, BinaryContent to, Instant updatedAt) {
    super(from, to, updatedAt);
    this.fromStatus = from.getStatus();
    this.toStatus = to.getStatus();
  }
}

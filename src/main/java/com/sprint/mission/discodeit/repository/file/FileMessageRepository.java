package com.sprint.mission.discodeit.repository.file;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.sprint.mission.discodeit.entity.Message;
import com.sprint.mission.discodeit.repository.MessageRepository;

public class FileMessageRepository implements MessageRepository {

    private final Path dataDirectory;

    public FileMessageRepository() {
        this.dataDirectory = Paths.get(System.getProperty("user.dir"), "data", "messages");
        if (!Files.exists(dataDirectory)) {
            try {
                Files.createDirectories(dataDirectory);
            } catch (IOException e) {
                throw new RuntimeException("메시지 데이터 디렉토리 생성 실패", e);
            }
        }
    }

    private Path getMessagePath(UUID messageId) {
        return dataDirectory.resolve(messageId.toString() + ".ser");
    }

    private void saveMessage(Message message) {
        Path messagePath = getMessagePath(message.getMessageId());
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(messagePath.toFile()))) {
            oos.writeObject(message);
        } catch (IOException e) {
            throw new RuntimeException("메시지 저장 실패: " + message.getMessageId(), e);
        }
    }

    private Message loadMessage(Path path) {
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(path.toFile()))) {
            return (Message) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("메시지 로드 실패: " + path, e);
        }
    }

    @Override
    public Message save(Message message) {
        saveMessage(message);
        return message;
    }

    @Override
    public Message findById(UUID messageId) {
        Path messagePath = getMessagePath(messageId);
        if (Files.exists(messagePath)) {
            return loadMessage(messagePath);
        }
        return null;
    }

    @Override
    public List<Message> findAll() {
        try (Stream<Path> pathStream = Files.list(dataDirectory)) {
            return pathStream
                    .filter(path -> path.toString().endsWith(".ser"))
                    .map(this::loadMessage)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException("메시지 목록 로드 실패", e);
        }
    }

    @Override
    public List<Message> findByChannelId(UUID channelId) {
        return findAll().stream()
                .filter(m -> m.getChannelId().equals(channelId))
                .collect(Collectors.toList());
    }

    @Override
    public List<Message> findByAuthorId(UUID authorId) {
        return findAll().stream()
                .filter(m -> m.getAuthorId().equals(authorId))
                .collect(Collectors.toList());
    }

    @Override
    public void deleteById(UUID messageId) {
        try {
            Files.deleteIfExists(getMessagePath(messageId));
        } catch (IOException e) {
            throw new RuntimeException("메시지 삭제 실패: " + messageId, e);
        }
    }
}

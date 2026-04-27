package real.talk.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.*;
import real.talk.model.dto.folder.*;
import real.talk.service.folder.FolderLessonService;
import real.talk.service.folder.FolderService;

import java.util.List;
import java.util.UUID;

@RestController
@RequiredArgsConstructor
@RequestMapping("api/v1/folders")
class FolderController {
    private final FolderService folderService;
    private final FolderLessonService folderLessonService;

    // Получить все папки пользователя
    @GetMapping("")
    public ResponseEntity<List<FolderResponse>> getFolders(@AuthenticationPrincipal real.talk.model.entity.User user) {
        return ResponseEntity.ok(folderService.getUserFolders(user.getUserId()));
    }

    // Создать папку
    @PostMapping("")
    public ResponseEntity<FolderCreateResponse> createFolder(@AuthenticationPrincipal real.talk.model.entity.User user,
            @RequestBody FolderCreateRequest folderCreateRequest) {
        return ResponseEntity.ok(folderService.createFolder(user.getUserId(), folderCreateRequest.getName()));
    }

    // Переименовать папку
    @PutMapping("/{folderId}")
    public ResponseEntity<FolderRenameResponse> renameFolder(
            @PathVariable Long folderId,
            @RequestBody FolderRenameRequest folderRenameRequest) {
        return ResponseEntity.ok(folderService.renameFolder(folderId, folderRenameRequest.getName()));
    }

    // Удалить папку
    @DeleteMapping("/{folderId}")
    public ResponseEntity<Void> deleteFolder(@PathVariable Long folderId) {
        folderService.deleteFolder(folderId);
        return ResponseEntity.noContent().build();
    }

    // Добавить урок в папку
    @PostMapping("/{folderId}/lessons")
    public ResponseEntity<Void> addLesson(@AuthenticationPrincipal real.talk.model.entity.User user,
            @PathVariable Long folderId,
            @RequestBody AddLessonToFolderRequest addLessonToFolderRequest) {
        folderLessonService.addLessonToFolder(user.getUserId(), folderId, addLessonToFolderRequest.lessonId());
        return ResponseEntity.ok().build();
    }

    // Удалить урок из папки
    @DeleteMapping("/{folderId}/lessons/{lessonId}")
    public ResponseEntity<Void> removeLesson(@AuthenticationPrincipal real.talk.model.entity.User user,
            @PathVariable Long folderId,
            @PathVariable UUID lessonId) {
        folderLessonService.removeLessonFromFolder(user.getUserId(), folderId, lessonId);
        return ResponseEntity.noContent().build();
    }

    // Получить все уроки в папке
    @GetMapping("/{folderId}/lessons")
    public ResponseEntity<List<FolderLessonResponse>> getLessons(
            @AuthenticationPrincipal real.talk.model.entity.User user,
            @PathVariable Long folderId) {
        return ResponseEntity.ok(folderLessonService.getLessonsInFolder(folderId, user.getUserId()));
    }
}

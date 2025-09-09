package com.sprint.mission.discodeit.controller;

import com.sprint.mission.discodeit.controller.api.AuthApi;
import com.sprint.mission.discodeit.dto.data.JwtDto;
import com.sprint.mission.discodeit.dto.data.JwtInformation;
import com.sprint.mission.discodeit.dto.data.UserDto;
import com.sprint.mission.discodeit.dto.request.RoleUpdateRequest;
import com.sprint.mission.discodeit.security.jwt.JwtTokenProvider;
import com.sprint.mission.discodeit.service.AuthService;
import com.sprint.mission.discodeit.service.UserService;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.web.csrf.CsrfToken;
import org.springframework.web.bind.annotation.CookieValue;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RequiredArgsConstructor
@RestController
@RequestMapping("/api/auth")
public class AuthController implements AuthApi {

  private final AuthService authService;
  private final UserService userService;
  private final JwtTokenProvider jwtTokenProvider;

  @GetMapping("csrf-token")
  public ResponseEntity<Void> getCsrfToken(CsrfToken csrfToken) {
    log.debug("CSRF 토큰 요청");
    log.trace("CSRF 토큰: {}", csrfToken.getToken());
    return ResponseEntity
        .status(HttpStatus.NO_CONTENT)
        .build();
  }

  @PostMapping("refresh")
  public ResponseEntity<JwtDto> refresh(@CookieValue(value = "REFRESH_TOKEN", required = false) String refreshToken,
      HttpServletResponse response) {
    log.info("토큰 리프레시 요청");
    
    // refresh 토큰이 없는 경우 401 Unauthorized 반환
    if (refreshToken == null || refreshToken.isEmpty()) {
      log.warn("Refresh 토큰이 없습니다");
      return ResponseEntity
          .status(HttpStatus.UNAUTHORIZED)
          .build();
    }
    
    try {
      JwtInformation jwtInformation = authService.refreshToken(refreshToken);
      Cookie refreshCookie = jwtTokenProvider.genereateRefreshTokenCookie(
          jwtInformation.getRefreshToken());
      response.addCookie(refreshCookie);

      JwtDto body = new JwtDto(
          jwtInformation.getUserDto(),
          jwtInformation.getAccessToken()
      );
      return ResponseEntity
          .status(HttpStatus.OK)
          .body(body);
    } catch (Exception e) {
      log.error("토큰 리프레시 실패: {}", e.getMessage());
      return ResponseEntity
          .status(HttpStatus.UNAUTHORIZED)
          .build();
    }
  }

  @PutMapping("role")
  public ResponseEntity<UserDto> updateRole(@RequestBody RoleUpdateRequest request) {
    log.info("권한 수정 요청");
    UserDto userDto = authService.updateRole(request);

    return ResponseEntity
        .status(HttpStatus.OK)
        .body(userDto);
  }
}

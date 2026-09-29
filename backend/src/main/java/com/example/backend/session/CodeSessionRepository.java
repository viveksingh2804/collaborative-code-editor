package com.example.backend.session;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface CodeSessionRepository extends JpaRepository<CodeSession, Long> {

    Optional<CodeSession> findBySessionCode(String sessionCode);
}
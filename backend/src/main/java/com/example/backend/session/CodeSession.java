package com.example.backend.session;

import jakarta.persistence.*;

@Entity
@Table(name = "code_sessions")
public class CodeSession {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(unique = true, nullable = false)
    private String sessionCode;

    @Lob
    @Column(columnDefinition = "LONGTEXT")
    private String code;

    public CodeSession() {
    }

    public CodeSession(String sessionCode, String code) {
        this.sessionCode = sessionCode;
        this.code = code;
    }

    public Long getId() {
        return id;
    }

    public String getSessionCode() {
        return sessionCode;
    }

    public void setSessionCode(String sessionCode) {
        this.sessionCode = sessionCode;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }
}
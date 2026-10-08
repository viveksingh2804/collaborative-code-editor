# ⚡ CollabCode — Collaborative Code Editor

A real-time collaborative code editor that allows multiple users to write and edit code together in the same session.

The project is built using React, Monaco Editor, Spring Boot WebSockets, and MySQL, with operation-based synchronization and undo/redo support.

---

## 🚀 Features

- ✍️ Real-time collaborative code editing
- 👥 Multiple users in the same session
- ⚡ Character-level operation synchronization
- 🔄 Operation-based synchronization
- 🔢 Version tracking for concurrent edits
- ↩️ Undo support
- ↪️ Redo support
- 💾 MySQL-based code persistence
- 🔗 Shareable session URLs
- 🟢 Real-time connected-user count
- 💻 Monaco Editor with Java syntax highlighting
- 🌐 WebSocket-based communication

---

## 🛠️ Tech Stack

### Frontend

- React
- Vite
- Monaco Editor
- JavaScript
- CSS

### Backend

- Java
- Spring Boot
- Spring WebSocket
- Maven
- Jackson

### Database

- MySQL
- Spring Data JPA
- Hibernate

---

## 🏗️ Architecture

```text
                ┌─────────────────────┐
                │     React Client    │
                │                     │
                │   Monaco Editor     │
                └──────────┬──────────┘
                           │
                      WebSocket
                           │
                           ▼
                ┌─────────────────────┐
                │   Spring Boot       │
                │   WebSocket Server  │
                │                     │
                │ Operation Handling  │
                │ Version Tracking    │
                │ Undo / Redo         │
                │ Session Management  │
                └──────────┬──────────┘
                           │
                         JPA
                           │
                           ▼
                ┌─────────────────────┐
                │       MySQL         │
                │                     │
                │   Code Sessions     │
                └─────────────────────┘

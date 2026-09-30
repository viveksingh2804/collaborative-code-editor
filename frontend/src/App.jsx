import { useEffect, useRef, useState } from "react";
import Editor from "@monaco-editor/react";
import "./App.css";

function App() {
  const socket = useRef(null);
  const editorRef = useRef(null);

  // Prevent remote operations from being sent back
  const applyingRemoteChange = useRef(false);

  // Keep latest version without React state delay
  const versionRef = useRef(0);

  const params = new URLSearchParams(window.location.search);
  const sessionId = params.get("session");

  const [code, setCode] = useState("");
  const [connected, setConnected] = useState(false);
  const [userCount, setUserCount] = useState(0);
  const [version, setVersion] = useState(0);
  const [joinCode, setJoinCode] = useState("");

  // =========================
  // CREATE SESSION
  // =========================

  const generateSessionId = () => {
    return Math.random()
      .toString(36)
      .substring(2, 8)
      .toUpperCase();
  };

  const createSession = () => {
    const newSession = generateSessionId();
    window.location.href = `/?session=${newSession}`;
  };

  // =========================
  // JOIN SESSION
  // =========================

  const joinSession = () => {
    const id = joinCode.trim().toUpperCase();

    if (!id) {
      alert("Please enter a session ID");
      return;
    }

    window.location.href = `/?session=${id}`;
  };

  // =========================
  // WEBSOCKET
  // =========================

  useEffect(() => {
    if (!sessionId) {
      return;
    }

    const ws = new WebSocket("ws://localhost:8080/ws");

    socket.current = ws;

    ws.onopen = () => {
      console.log("WebSocket connected");

      setConnected(true);

      ws.send(
        JSON.stringify({
          type: "join",
          sessionId: sessionId,
        })
      );
    };

    ws.onmessage = (event) => {
      const data = JSON.parse(event.data);

      // =========================
      // INITIAL / FULL CODE
      // =========================

      if (data.type === "code") {
        applyingRemoteChange.current = true;

        setCode(data.code);

        if (data.version !== undefined) {
          versionRef.current = data.version;
          setVersion(data.version);
        }

        return;
      }

      // =========================
      // REMOTE OPERATION
      // =========================

      if (data.type === "operation") {
        const editor = editorRef.current;

        if (!editor) {
          return;
        }

        const model = editor.getModel();

        if (!model) {
          return;
        }

        applyingRemoteChange.current = true;

        const position = data.position;
        const deleteCount = data.deleteCount;
        const text = data.text || "";

        const startPosition =
          model.getPositionAt(position);

        const endPosition =
          model.getPositionAt(
            position + deleteCount
          );

        editor.executeEdits(
          "remote-operation",
          [
            {
              range: {
                startLineNumber:
                  startPosition.lineNumber,

                startColumn:
                  startPosition.column,

                endLineNumber:
                  endPosition.lineNumber,

                endColumn:
                  endPosition.column,
              },

              text: text,
            },
          ]
        );

        setCode(editor.getValue());

        if (data.version !== undefined) {
          versionRef.current = data.version;
          setVersion(data.version);
        }

        applyingRemoteChange.current = false;

        return;
      }

      // =========================
      // USER COUNT
      // =========================

      if (data.type === "users") {
        setUserCount(data.count);
        return;
      }

      // =========================
      // VERSION ACK
      // =========================

      if (data.type === "ack") {
        versionRef.current = data.version;
        setVersion(data.version);
        return;
      }
    };

    ws.onclose = () => {
      console.log("WebSocket disconnected");
      setConnected(false);
    };

    ws.onerror = (error) => {
      console.error("WebSocket error:", error);
    };

    return () => {
      ws.close();
    };
  }, [sessionId]);

  // =========================
  // EDITOR CHANGE
  // =========================

  const handleEditorChange = (
    value,
    changeEvent
  ) => {
    const newCode = value || "";

    setCode(newCode);

    // Remote operation should NOT be sent back
    if (applyingRemoteChange.current) {
      return;
    }

    const changes =
      changeEvent?.changes || [];

    if (
      socket.current &&
      socket.current.readyState ===
        WebSocket.OPEN &&
      changes.length > 0
    ) {
      socket.current.send(
        JSON.stringify({
          type: "operation",

          sessionId: sessionId,

          baseVersion:
            versionRef.current,

          changes: changes.map(
            (change) => ({
              position:
                change.rangeOffset,

              deleteCount:
                change.rangeLength,

              text:
                change.text,
            })
          ),
        })
      );
    }
  };

  // =========================
  // COPY LINK
  // =========================

  const copySessionLink = async () => {
    try {
      await navigator.clipboard.writeText(
        window.location.href
      );

      alert("Session link copied!");
    } catch (error) {
      console.error(error);
    }
  };

  // =========================
  // LANDING PAGE
  // =========================

  if (!sessionId) {
    return (
      <div className="landing">

        <div className="landing-card">

          <div className="logo">
            ⚡ CollabCode
          </div>

          <h1>
            Collaborative Code Editor
          </h1>

          <p>
            Code together in real-time
            with your friends and teammates.
          </p>

          <button
            className="primary-btn"
            onClick={createSession}
          >
            🚀 Create New Session
          </button>

          <div className="divider">
            <span>OR</span>
          </div>

          <input
            type="text"
            placeholder="Enter Session ID"
            value={joinCode}
            onChange={(e) =>
              setJoinCode(e.target.value)
            }
            onKeyDown={(e) => {
              if (e.key === "Enter") {
                joinSession();
              }
            }}
          />

          <button
            className="secondary-btn"
            onClick={joinSession}
          >
            Join Session
          </button>

        </div>

      </div>
    );
  }

  // =========================
  // EDITOR PAGE
  // =========================

  return (
    <div className="app">

      <header className="header">

        <div className="logo">
          ⚡ CollabCode
        </div>

        <div className="session">
          Session:
          <span>{sessionId}</span>
        </div>

        <div className="users">
          {connected
            ? `🟢 ${userCount} ${
                userCount === 1
                  ? "User"
                  : "Users"
              }`
            : "🔴 Offline"}
        </div>

        <button
          className="share-btn"
          onClick={copySessionLink}
        >
          🔗 Share
        </button>

      </header>

      <main className="editor-container">

        <Editor
          height="100%"
          language="java"
          value={code}

          onMount={(editor) => {
            editorRef.current = editor;
          }}

          onChange={handleEditorChange}

          theme="vs-dark"

          options={{
            fontSize: 16,

            minimap: {
              enabled: false,
            },

            automaticLayout: true,
          }}
        />

      </main>

    </div>
  );
}

export default App;
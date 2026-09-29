import { useEffect, useRef, useState } from "react";
import Editor from "@monaco-editor/react";
import "./App.css";

function App() {
  const socket = useRef(null);

  const params = new URLSearchParams(window.location.search);
  const sessionId = params.get("session");

  const [code, setCode] = useState("");
  const [connected, setConnected] = useState(false);
const [joinCode, setJoinCode] = useState("");
const [userCount, setUserCount] = useState(0);
  // Generate random session ID
  const generateSessionId = () => {
    return Math.random().toString(36).substring(2, 8).toUpperCase();
  };

  // Create new session
  const createSession = () => {
    const newSession = generateSessionId();
    window.location.href = `/?session=${newSession}`;
  };

  // Join existing session
  const joinSession = () => {
    const id = joinCode.trim().toUpperCase();

    if (!id) {
      alert("Please enter a session ID");
      return;
    }

    window.location.href = `/?session=${id}`;
  };

  useEffect(() => {
    if (!sessionId) return;

    const ws = new WebSocket("ws://localhost:8080/ws");
    socket.current = ws;

    ws.onopen = () => {
      console.log("WebSocket connected");
      setConnected(true);

      ws.send(
        JSON.stringify({
          type: "join",
          sessionId: sessionId
        })
      );
    };

   ws.onmessage = (event) => {
  const data = JSON.parse(event.data);

  if (data.type === "code") {
    setCode(data.code);
  }

  if (data.type === "users") {
    setUserCount(data.count);
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

  const handleEditorChange = (value, changeEvent) => {
  const newCode = value || "";

  setCode(newCode);

  const changes = changeEvent?.changes || [];

  if (
    socket.current &&
    socket.current.readyState === WebSocket.OPEN &&
    changes.length > 0
  ) {
    socket.current.send(
      JSON.stringify({
        type: "operation",
        sessionId: sessionId,
        changes: changes.map((change) => ({
          position: change.rangeOffset,
          deleteCount: change.rangeLength,
          text: change.text
        }))
      })
    );
  }
};

  const copySessionLink = async () => {
    await navigator.clipboard.writeText(window.location.href);
    alert("Session link copied!");
  };

  // Landing page
  if (!sessionId) {
    return (
      <div className="landing">
        <div className="landing-card">
          <div className="logo">⚡ CollabCode</div>

          <h1>Collaborative Code Editor</h1>

          <p>
            Code together in real-time with your friends and teammates.
          </p>

          <button className="primary-btn" onClick={createSession}>
            🚀 Create New Session
          </button>

          <div className="divider">
            <span>OR</span>
          </div>

          <input
            type="text"
            placeholder="Enter Session ID"
            value={joinCode}
            onChange={(e) => setJoinCode(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter") {
                joinSession();
              }
            }}
          />

          <button className="secondary-btn" onClick={joinSession}>
            Join Session
          </button>
        </div>
      </div>
    );
  }

  // Editor page
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
    ? `🟢 ${userCount} ${userCount === 1 ? "User" : "Users"}`
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
  onChange={handleEditorChange}
  theme="vs-dark"
  options={{
    fontSize: 16,
    minimap: {
      enabled: false
    },
    automaticLayout: true
  }}
/>

      </main>

    </div>
  );
}

export default App;
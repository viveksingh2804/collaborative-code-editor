import { useEffect, useRef, useState } from "react";
import Editor from "@monaco-editor/react";
import "./App.css";

function App() {

  const socket = useRef(null);
  const editorRef = useRef(null);
  const remoteChange = useRef(false);

  const params = new URLSearchParams(window.location.search);
  const sessionId = params.get("session") || "ABC123";

  const [code, setCode] = useState("");
  const [connected, setConnected] = useState(false);
  const [userCount, setUserCount] = useState(0);

  useEffect(() => {

    const ws = new WebSocket(
      "ws://localhost:8080/ws"
    );

    socket.current = ws;

    ws.onopen = () => {

      console.log("WEBSOCKET CONNECTED");

      setConnected(true);

      ws.send(
        JSON.stringify({
          type: "join",
          sessionId: sessionId
        })
      );
    };

    ws.onmessage = (event) => {

      console.log(
        "SERVER MESSAGE:",
        event.data
      );

      try {

        const data =
          JSON.parse(event.data);

        // ==========================
        // CODE FROM SERVER
        // ==========================

        if (data.type === "code") {

          remoteChange.current = true;

          setCode(data.code);

          if (editorRef.current) {

            const model =
              editorRef.current.getModel();

            if (
              model &&
              model.getValue() !== data.code
            ) {

              model.setValue(data.code);
            }
          }

          setTimeout(() => {
            remoteChange.current = false;
          }, 100);

          return;
        }

        // ==========================
        // USER COUNT
        // ==========================

        if (data.type === "users") {

          setUserCount(data.count);

          return;
        }

      } catch (error) {

        console.error(
          "MESSAGE ERROR:",
          error
        );
      }
    };

    ws.onerror = (error) => {

      console.error(
        "WEBSOCKET ERROR:",
        error
      );
    };

    ws.onclose = () => {

      console.log(
        "WEBSOCKET DISCONNECTED"
      );

      setConnected(false);
    };

    return () => {
      ws.close();
    };

  }, [sessionId]);

  // ==========================
  // MONACO READY
  // ==========================

  const handleEditorMount = (editor) => {

    editorRef.current = editor;
  };

  // ==========================
  // LOCAL TYPING
  // ==========================

  const handleEditorChange = (value) => {

    const newCode = value || "";

    setCode(newCode);

    // Remote update ko server par
    // dobara mat bhejo
    if (remoteChange.current) {
      return;
    }

    if (
      socket.current &&
      socket.current.readyState ===
        WebSocket.OPEN
    ) {

      socket.current.send(
        JSON.stringify({
          type: "code",
          sessionId: sessionId,
          code: newCode
        })
      );

      console.log(
        "SENT CODE:",
        newCode
      );
    }
  };

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
            ? "🟢"
            : "🔴"}

          {" "}
          {userCount} Users

        </div>

        <button
          className="share-button"
          onClick={() => {

            navigator.clipboard.writeText(
              window.location.href
            );

            alert(
              "Session link copied!"
            );
          }}
        >
          🔗 Share
        </button>

      </header>

      <main className="editor-container">

        <Editor
          height="100%"
          language="java"
          theme="vs-dark"
          value={code}
          onMount={handleEditorMount}
          onChange={handleEditorChange}
          options={{
            fontSize: 16,
            minimap: {
              enabled: false
            },
            automaticLayout: true,
            scrollBeyondLastLine: false
          }}
        />

      </main>

    </div>
  );
}

export default App;

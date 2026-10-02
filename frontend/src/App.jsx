import { useEffect, useRef, useState } from "react";
import Editor from "@monaco-editor/react";
import "./App.css";

function App() {

  const socket = useRef(null);
  const editorRef = useRef(null);

  // Monaco listener
  const changeListenerRef = useRef(null);

  // Remote change ko identify karne ke liye
  const remoteChange = useRef(false);

  // Server document version
  const versionRef = useRef(0);

  const params =
    new URLSearchParams(window.location.search);

  const sessionId =
    params.get("session") || "ABC123";

  const [code, setCode] = useState("");
  const [connected, setConnected] = useState(false);
  const [userCount, setUserCount] = useState(0);

  // ==========================
  // WEBSOCKET
  // ==========================

  useEffect(() => {

    const ws =
      new WebSocket(
        "ws://localhost:8080/ws"
      );

    socket.current = ws;

    ws.onopen = () => {

      console.log(
        "WEBSOCKET CONNECTED"
      );

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
        // INITIAL CODE
        // ==========================

        if (data.type === "code") {

          remoteChange.current = true;

          if (
            data.version !== undefined
          ) {
            versionRef.current =
              data.version;
          }

          setCode(data.code);

          const editor =
            editorRef.current;

          if (editor) {

            const model =
              editor.getModel();

            if (
              model &&
              model.getValue() !== data.code
            ) {

              model.setValue(
                data.code
              );
            }
          }

          // Monaco event fire hone ke baad
          // remote flag reset hoga
          setTimeout(() => {

            remoteChange.current =
              false;

          }, 0);

          return;
        }

        // ==========================
        // VERSION
        // ==========================

        if (data.type === "version") {

          versionRef.current =
            data.version;

          console.log(
            "CURRENT VERSION:",
            versionRef.current
          );

          return;
        }

        // ==========================
        // REMOTE OPERATION
        // ==========================

        if (
          data.type === "operation"
        ) {

          const editor =
            editorRef.current;

          if (!editor) {
            return;
          }

          const model =
            editor.getModel();

          if (!model) {
            return;
          }

          remoteChange.current = true;

          const position =
            Math.max(
              0,
              Math.min(
                data.position,
                model.getValueLength()
              )
            );

          const deleteCount =
            Math.max(
              0,
              Math.min(
                data.deleteCount,
                model.getValueLength()
                  - position
              )
            );

          const insertText =
            data.text || "";

          // Monaco ka native edit
          model.pushEditOperations(
            [],
            [
              {
                range: getRangeFromOffset(
  model,
  position,
  deleteCount
),
                text: insertText
              }
            ],
            () => null
          );

          if (
            data.version !== undefined
          ) {
            versionRef.current =
              data.version;
          }

          setCode(
            model.getValue()
          );

          setTimeout(() => {

            remoteChange.current =
              false;

          }, 0);

          return;
        }

        // ==========================
        // USER COUNT
        // ==========================

        if (data.type === "users") {

          setUserCount(
            data.count
          );

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

      if (
        changeListenerRef.current
      ) {

        changeListenerRef.current.dispose();

        changeListenerRef.current =
          null;
      }

      ws.close();
    };

  }, [sessionId]);

  // ==========================
  // EDITOR MOUNT
  // ==========================

  const handleEditorMount =
    (editor) => {

      editorRef.current =
        editor;

      // IMPORTANT:
      // React onChange ki jagah
      // Monaco native change event

      changeListenerRef.current =
        editor.onDidChangeModelContent(
          (event) => {

            // Remote operation hai
            // to server ko wapas mat bhejo
            if (
              remoteChange.current
            ) {
              return;
            }

            if (
              !socket.current ||
              socket.current.readyState !==
                WebSocket.OPEN
            ) {
              return;
            }

            if (
              !event.changes ||
              event.changes.length === 0
            ) {
              return;
            }

            // Monaco normally ek user action
            // ko ek ya multiple changes mein
            // bhej sakta hai.
            //
            // Changes ko reverse order mein
            // process karenge taaki positions
            // disturb na hon.

            const changes =
              [...event.changes]
                .sort(
                  (a, b) =>
                    b.rangeOffset -
                    a.rangeOffset
                );

            changes.forEach(
              (change) => {

                const operation = {

                  type: "operation",

                  sessionId:
                    sessionId,

                  position:
                    change.rangeOffset,

                  deleteCount:
                    change.rangeLength,

                  text:
                    change.text,

                  baseVersion:
                    versionRef.current
                };

                socket.current.send(
                  JSON.stringify(
                    operation
                  )
                );

                console.log(
                  "SENT OPERATION:",
                  operation
                );

                // Local document operation
                // server queue mein process hoga.
                versionRef.current++;
              }
            );

            setCode(
              editor.getValue()
            );
          }
        );
    };

  // ==========================
  // SHARE
  // ==========================

  const shareSession = () => {

    navigator.clipboard.writeText(
      window.location.href
    );

    alert(
      "Session link copied!"
    );
  };

  // ==========================
  // UI
  // ==========================

  return (

    <div className="app">

      <header className="header">

        <div className="logo">
          ⚡ CollabCode
        </div>

        <div className="session">

          Session:
          <span>
            {sessionId}
          </span>

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
          onClick={shareSession}
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
          options={{
            fontSize: 16,

            minimap: {
              enabled: false
            },

            automaticLayout: true,

            scrollBeyondLastLine:
              false
          }}
        />

      </main>

    </div>
  );
}

// =================================
// Convert character offset → Monaco Range
// =================================

function getRangeFromOffset(
  model,
  offset,
  deleteCount
) {

  const start =
    model.getPositionAt(offset);

  const end =
    model.getPositionAt(
      offset + deleteCount
    );

  return {
    startLineNumber:
      start.lineNumber,

    startColumn:
      start.column,

    endLineNumber:
      end.lineNumber,

    endColumn:
      end.column
  };
}

export default App;
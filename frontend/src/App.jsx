import { useEffect, useRef, useState } from "react";
import Editor from "@monaco-editor/react";
import "./App.css";

function App() {

  const socket = useRef(null);
  const editorRef = useRef(null);
  const monacoRef = useRef(null);

  const applyingRemote = useRef(false);
  const versionRef = useRef(0);

  const params =
    new URLSearchParams(window.location.search);

  const sessionId =
    params.get("session") || "ABC123";

  const [code, setCode] = useState("");
  const [connected, setConnected] = useState(false);
  const [userCount, setUserCount] = useState(0);
  const [output, setOutput] = useState("");
  const [running, setRunning] = useState(false);  
  // ==========================================
  // WEBSOCKET
  // ==========================================

  useEffect(() => {

const ws = new WebSocket(
  "wss://scholarship-troubleshooting-instrumentation-expertise.trycloudflare.com/ws"
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

        // ==================================
        // CODE
        // ==================================

        if (data.type === "code") {

          applyingRemote.current = true;

          setCode(data.code);

          if (editorRef.current) {

            const model =
              editorRef.current.getModel();

            if (
              model &&
              model.getValue() !== data.code
            ) {

              model.setValue(
                data.code
              );
            }
          }

          if (
            typeof data.version === "number"
          ) {

            versionRef.current =
              data.version;
          }

          setTimeout(() => {

            applyingRemote.current =
              false;

          }, 50);

          return;
        }

        // ==================================
        // VERSION
        // ==================================

        if (data.type === "version") {

          versionRef.current =
              data.version;

          console.log(
            "VERSION:",
            data.version
          );

          return;
        }

        // ==================================
        // USER COUNT
        // ==================================

        if (data.type === "users") {

          setUserCount(
            data.count
          );

          return;
        }

        // ==================================
        // REMOTE OPERATION
        // ==================================

        if (
          data.type === "operation"
        ) {

          applyRemoteOperation(
            data
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

      ws.close();

    };

  }, [sessionId]);

  // ==========================================
  // APPLY REMOTE OPERATION
  // ==========================================

  const applyRemoteOperation = (operation) => {

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

    applyingRemote.current = true;

    const position =
      Math.max(
        0,
        Math.min(
          operation.position,
          model.getValueLength()
        )
      );

    const deleteCount =
      Math.max(
        0,
        Math.min(
          operation.deleteCount || 0,
          model.getValueLength() - position
        )
      );

    const range =
      model.getPositionAt(position);

    const end =
      model.getPositionAt(
        position + deleteCount
      );

    editor.executeEdits(
      "remote-operation",
      [
        {
          range: {
            startLineNumber:
              range.lineNumber,

            startColumn:
              range.column,

            endLineNumber:
              end.lineNumber,

            endColumn:
              end.column
          },

          text:
            operation.text || ""
        }
      ]
    );

    setCode(
      model.getValue()
    );

    if (
      typeof operation.version === "number"
    ) {

      versionRef.current =
        operation.version;
    }

    setTimeout(() => {

      applyingRemote.current =
        false;

    }, 50);
  };

  // ==========================================
  // EDITOR MOUNT
  // ==========================================

  const handleEditorMount =
      (editor, monaco) => {

    editorRef.current =
      editor;

    monacoRef.current =
      monaco;

    // ======================================
    // CTRL + Z
    // ======================================

    editor.addCommand(
      monaco.KeyMod.CtrlCmd |
      monaco.KeyCode.KeyZ,
      () => {

        sendHistoryAction(
          "undo"
        );
      }
    );

    // ======================================
    // CTRL + Y
    // ======================================

    editor.addCommand(
      monaco.KeyMod.CtrlCmd |
      monaco.KeyCode.KeyY,
      () => {

        sendHistoryAction(
          "redo"
        );
      }
    );

    // ======================================
    // MAC CMD + SHIFT + Z
    // ======================================

    editor.addCommand(
      monaco.KeyMod.CtrlCmd |
      monaco.KeyMod.Shift |
      monaco.KeyCode.KeyZ,
      () => {

        sendHistoryAction(
          "redo"
        );
      }
    );

    console.log(
      "MONACO READY"
    );
  };

  // ==========================================
  // SEND UNDO / REDO
  // ==========================================

  const sendHistoryAction =
      (action) => {

    if (
      !socket.current ||
      socket.current.readyState !==
        WebSocket.OPEN
    ) {

      return;
    }

    socket.current.send(
      JSON.stringify({
        type: action
      })
    );

    console.log(
      "SENT:",
      action
    );
  };

  // ==========================================
  // LOCAL EDIT
  // ==========================================

  // ==========================================
// LOCAL EDIT
// ==========================================

const handleEditorChange =
    (value, event) => {

  const newCode =
    value || "";

  setCode(newCode);

  // Remote operation ko server par
  // dobara mat bhejo.
  if (applyingRemote.current) {
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
    !event ||
    !event.changes ||
    event.changes.length === 0
  ) {
    return;
  }

  const changes =
    [...event.changes]
      .sort(
        (a, b) =>
          b.rangeOffset -
          a.rangeOffset
      );

  for (const change of changes) {

    // IMPORTANT:
    // Har local operation ko current
    // local version milega.
    const baseVersion =
      versionRef.current;

    const operation = {

      type: "operation",

      sessionId:
        sessionId,

      position:
        change.rangeOffset,

      deleteCount:
        change.rangeLength,

      text:
        change.text || "",

      baseVersion:
        baseVersion
    };

    socket.current.send(
      JSON.stringify(operation)
    );

    // IMPORTANT:
    // Apne operation ko immediately
    // next local operation ke liye
    // next version maan rahe hain.
    versionRef.current =
      baseVersion + 1;

    console.log(
      "SENT OPERATION:",
      operation
    );
  }
};

const runCode = async () => {

  setRunning(true);
  setOutput("Running...");

  try {

   const response = await fetch(
  "https://scholarship-troubleshooting-instrumentation-expertise.trycloudflare.com/api/execute",
  {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      code: code
    })
  }
);

    const data = await response.json();

    if (data.success) {

      setOutput(
        data.output || "Program executed successfully."
      );

    } else {

      setOutput(
        data.error || "Execution failed."
      );
    }

  } catch (error) {

    setOutput(
      "Could not connect to execution server."
    );

  } finally {

    setRunning(false);
  }
};
  // ==========================================
  // RENDER
  // ==========================================

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
          <div className="toolbar">

  <button
    className="run-button"
    onClick={runCode}
    disabled={running}
  >
    {running ? "⏳ Running..." : "▶ Run Code"}
  </button>

</div>
      <main className="editor-container">

        <Editor
          height="100%"
          language="java"
          theme="vs-dark"
          value={code}
          onMount={
            handleEditorMount
          }
          onChange={
            handleEditorChange
          }
          options={{

            fontSize: 16,

            minimap: {
              enabled: false
            },

            automaticLayout: true,

            scrollBeyondLastLine:
              false,

            // Monaco ka internal
            // undo/redo disabled.
            undoRedo:
              true
          }}
        />

      </main>

      <div className="output-panel">
        <div className="output-header">Output</div>
        <pre>{output || "Run your code to see the output here."}</pre>
      </div>

    </div>
  );
}

export default App;

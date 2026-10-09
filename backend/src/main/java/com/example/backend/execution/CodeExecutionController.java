package com.example.backend.execution;

import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@RestController
@RequestMapping("/api")
@CrossOrigin(origins = "*")
public class CodeExecutionController {

    @PostMapping("/execute")
    public ExecutionResponse execute(
            @RequestBody ExecutionRequest request) {

        Path tempDir = null;

        try {
            String code = request.code();

            if (code == null || code.trim().isEmpty()) {
                return new ExecutionResponse(
                        false,
                        "",
                        "Code cannot be empty."
                );
            }

            String className = extractClassName(code);

            if (className == null) {
                return new ExecutionResponse(
                        false,
                        "",
                        "Could not find a Java class."
                );
            }

            tempDir = Files.createTempDirectory("collab-code-");

            Path sourceFile =
                    tempDir.resolve(className + ".java");

            Files.writeString(sourceFile, code);

            // ==========================
            // COMPILE
            // ==========================

            Process compile =
                    new ProcessBuilder(
                            "javac",
                            sourceFile.getFileName().toString()
                    )
                    .directory(tempDir.toFile())
                    .redirectErrorStream(true)
                    .start();

            boolean compileFinished =
                    compile.waitFor(
                            15,
                            TimeUnit.SECONDS
                    );

            if (!compileFinished) {
                compile.destroyForcibly();

                return new ExecutionResponse(
                        false,
                        "",
                        "Compilation timed out."
                );
            }

            String compileOutput =
                    readOutput(compile.getInputStream());

            if (compile.exitValue() != 0) {
                return new ExecutionResponse(
                        false,
                        "",
                        compileOutput
                );
            }

            // ==========================
            // RUN
            // ==========================

            Process run =
                    new ProcessBuilder(
                            "java",
                            "-cp",
                            ".",
                            className
                    )
                    .directory(tempDir.toFile())
                    .redirectErrorStream(true)
                    .start();

            boolean runFinished =
                    run.waitFor(
                            5,
                            TimeUnit.SECONDS
                    );

            if (!runFinished) {
                run.destroyForcibly();

                return new ExecutionResponse(
                        false,
                        "",
                        "Program execution timed out."
                );
            }

            String output =
                    readOutput(run.getInputStream());

            if (run.exitValue() != 0) {
                return new ExecutionResponse(
                        false,
                        "",
                        output
                );
            }

            return new ExecutionResponse(
                    true,
                    output,
                    ""
            );

        } catch (Exception e) {

            return new ExecutionResponse(
                    false,
                    "",
                    e.getMessage()
            );

        } finally {

            if (tempDir != null) {
                deleteDirectory(tempDir);
            }
        }
    }

    // ==========================
    // FIND CLASS NAME
    // ==========================

    private String extractClassName(String code) {

        Pattern pattern =
                Pattern.compile(
                        "(?:public\\s+)?class\\s+([A-Za-z_$][A-Za-z0-9_$]*)"
                );

        Matcher matcher =
                pattern.matcher(code);

        if (matcher.find()) {
            return matcher.group(1);
        }

        return null;
    }

    // ==========================
    // READ OUTPUT
    // ==========================

    private String readOutput(InputStream input)
            throws IOException {

        return new String(
                input.readAllBytes()
        );
    }

    // ==========================
    // DELETE TEMP DIRECTORY
    // ==========================

    private void deleteDirectory(Path directory) {

        try {

            Files.walk(directory)
                    .sorted(
                            (a, b) -> b.compareTo(a)
                    )
                    .forEach(path -> {

                        try {
                            Files.deleteIfExists(path);
                        } catch (IOException ignored) {
                        }

                    });

        } catch (IOException ignored) {
        }
    }

    // ==========================
    // REQUEST / RESPONSE
    // ==========================

    public record ExecutionRequest(
            String code
    ) {
    }

    public record ExecutionResponse(
            boolean success,
            String output,
            String error
    ) {
    }
}

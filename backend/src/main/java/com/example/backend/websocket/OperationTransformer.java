package com.example.backend.websocket;

public class OperationTransformer {

    public static Operation transform(
            Operation incoming,
            Operation applied) {

        int position = incoming.position();

        int appliedPosition = applied.position();
        int appliedDelete = applied.deleteCount();
        int appliedInsertLength =
                applied.text() == null
                        ? 0
                        : applied.text().length();

        // Applied operation was before incoming operation
        if (appliedPosition < position) {

            int deletedBefore =
                    Math.min(
                            appliedDelete,
                            position - appliedPosition
                    );

            position =
                    position
                            - deletedBefore
                            + appliedInsertLength;
        }

        // Both operations start at the same position
        else if (appliedPosition == position) {

            // If applied operation inserts text,
            // incoming operation moves after it.
            if (appliedInsertLength > 0) {

                position +=
                        appliedInsertLength;
            }

            // If applied operation deletes text,
            // keep incoming operation at same position.
        }

        // Applied operation starts inside
        // incoming delete range
        else if (
                appliedPosition <
                        position + incoming.deleteCount()
        ) {

            int overlap =
                    Math.min(
                            incoming.deleteCount(),
                            position
                                    + incoming.deleteCount()
                                    - appliedPosition
                    );

            int newDeleteCount =
                    Math.max(
                            0,
                            incoming.deleteCount()
                                    - overlap
                    );

            return new Operation(
                    incoming.userId(),
                    position,
                    newDeleteCount,
                    incoming.text(),
                    incoming.baseVersion()
            );
        }

        return new Operation(
                incoming.userId(),
                position,
                incoming.deleteCount(),
                incoming.text(),
                incoming.baseVersion()
        );
    }
}
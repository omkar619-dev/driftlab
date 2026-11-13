package driftlab;

/**
 * An error that stops the driver. main writes its message to stderr, and the driver exits with
 * status 1, so the runner finds a driver that exited early, and records that it broke (DESIGN.md,
 * When a driver breaks).
 */
final class DriverException extends Exception {
    private static final long serialVersionUID = 1L;

    DriverException(String message) {
        super(message);
    }

    DriverException(String message, Throwable cause) {
        super(message, cause);
    }
}

package org.cloudname.a3;

import org.cloudname.a3.domain.User;

/**
 * Authentication result.
 *
 * @author borud
 */
public class AuthnResult {
    private User user = null;
    private State state = State.UNKNOWN;
    private String message = null;
    private Throwable cause = null;

    public enum State {
        OK,
        UNKNOWN_USER,
        WRONG_PASSWORD,
        INTERNAL_ERROR,
        UNKNOWN
    }

    public AuthnResult(User user, State state) {
        this.user = user;
        this.state = state;
    }

    public AuthnResult(State state) {
        this.state = state;
    }

    public AuthnResult(State state, String message) {
        this.state = state;
        this.message = message;
    }

    public AuthnResult(State state, String message, Throwable cause) {
        this.state = state;
        this.message = message;
        this.cause = cause;
    }

    public User getUser() {
        return user;
    }

    public State getState() {
        return state;
    }

    public String getMessage() {
        return message;
    }

    public boolean isOk() {
        return ((null != user) && (State.OK == state));
    }

    public boolean isWrongPassword() {
        return (State.WRONG_PASSWORD == state);
    }
}
package org.cloudname.a3;

import java.util.HashSet;
import java.util.Set;
import org.cloudname.a3.domain.User;

public class GetUserResult {
    private Set<User> users = new HashSet<User>();
    private State state = State.UNKNOWN;
    private String message = null;
    private Throwable cause = null;

    public enum State {
        OK,
        UNKNOWN_USER,
        INTERNAL_ERROR,
        UNKNOWN
    }

    public GetUserResult(Set<User> users, State state) {
        this.users = users;
        this.state = state;
    }

    public GetUserResult(State state) {
        this.state = state;
    }

    public GetUserResult(State state, String message) {
        this.state = state;
        this.message = message;
    }

    public GetUserResult(State state, String message, Throwable cause) {
        this.state = state;
        this.message = message;
        this.cause = cause;
    }

    public Set<User> getUsers() {
        return users;
    }

    public State getState() {
        return state;
    }

    public String getMessage() {
        return message;
    }

    public boolean isOk() {
        return ((!users.isEmpty()) && (State.OK == state));
    }
}

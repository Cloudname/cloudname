package org.cloudname.a3.domain;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;

/**
 * User domain object.  This is supposed to be immutable which is why
 * we have the ugly constructor and no setters.
 *
 * If anyone can tell me how to get jackson to make use of sensible
 * builders without inflicting too much chaos on the code, please feel
 * free to fix.
 *
 * @author borud
 */
public class User {
    private String username;
    private String password;
    private String oldPassword;
    private DateTime oldPasswordExpiry;
    private String realName;
    private String email;
    private Set<String> roles;
    private Map<String,String> properties;

    private int precomputedHashCode;

    // Since this class is immutable we can calculate a hashCode up
    // front.
    private int hashCode;

    public User() {
        super();
    }

    /**
     * This constructor is mainly used by Jackson in order to create
     * instances of the User object from JSON.
     *
     * The Set[string] of roles should be lowercase role names.
     *
     * @param username user name
     * @param password user's password
     * @param oldPassword old password for user
     * @param oldPasswordExpiry expiry date for old password
     * @param realName User's real name
     * @param email User's email
     * @param roles User roles
     * @param properties User properties
     */
    @JsonCreator
    public User(@JsonProperty("username") String username,
                @JsonProperty("password") String password,
                @JsonProperty("oldPassword") String oldPassword,
                @JsonProperty("oldPasswordExpiry") DateTime oldPasswordExpiry,
                @JsonProperty("realName") String realName,
                @JsonProperty("email") String email,
                @JsonProperty("roles") Set<String> roles,
                @JsonProperty("properties") Map<String,String> properties)
    {
        if (null == roles) {
            throw new NullPointerException("roles cannot be null.  use Collections.EMPTY_SET if you want empty roles");
        }
        if (oldPassword != null && oldPasswordExpiry == null) {
            throw new IllegalArgumentException("Old password must have an expiry date");
        }

        this.username = username;
        this.password = password;
        this.oldPassword = oldPassword;
        this.oldPasswordExpiry = oldPasswordExpiry;
        this.realName = realName;
        this.email = email;
        this.roles = roles;
        this.properties = properties;

        // I can't be arsed to do lazy initialization.
        precomputedHashCode = computeHashCode();
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getOldPassword() {
        return oldPassword;
    }

    public DateTime getOldPasswordExpiry() {
        return oldPasswordExpiry;
    }

    public String getRealName() {
        return realName;
    }

    public String getEmail() {
        return email;
    }

    public Set<String> getRoles() {
        return roles;
    }

    public Map<String,String> getProperties() {
        return properties;
    }

    public boolean hasRole(String role) {
        return roles.contains(role.toLowerCase());
    }

    @Override
    public int hashCode() {
        return precomputedHashCode;
    }

    /**
     * @return the hashCode for the User.
     */
    private int computeHashCode() {
        // DateTime class has buggy hashCode(), so we can't use it.
        return (username.hashCode() * 23)
            ^  (password.hashCode() * 37)
            ^  (oldPassword == null ? 0 : (oldPassword.hashCode() * 41))
            ^  (oldPasswordExpiry == null ? 0 : ((int)oldPasswordExpiry.getMillis() * 43))
            ^  (realName.hashCode() * 47)
            ^  (email.hashCode() * 59)
            ^  (roles.hashCode() * 61)
            ^  (properties.hashCode() * 67);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof User)) {
            return false;
        }

        final User other = (User) o;

        // Optimizations, not strictly necessary.
        if (precomputedHashCode != other.precomputedHashCode) {
            return false;
        }
        if (this == other) {
            return true;
        }

        // DateTime class has buggy equals(), so we can't use it.
        return (username.equals(other.username)
                && password.equals(other.password)
                && ((oldPassword == null && other.oldPassword == null)
                    || (oldPassword != null && oldPassword.equals(other.oldPassword)))
                && ((oldPasswordExpiry == null && other.oldPasswordExpiry == null)
                    || (oldPasswordExpiry != null && oldPasswordExpiry.compareTo(other.oldPasswordExpiry) == 0))
                && realName.equals(other.realName)
                && email.equals(other.email)
                && roles.equals(other.roles)
                && properties.equals(other.properties)
        );
    }

    /**
     * @return JSON representation of the User.
     */
    public String toJson() {
        try {
            final ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new JodaModule());
            return mapper.writeValueAsString(this);
        } catch (IOException e) {
            return null;
        }
    }

    /**
     * @param json JSON String
     * @return User instance from a JSON string.
     * @throws IOException if there's an error reading from JSON
     */
    public static User fromJson(String json) throws IOException {
        final ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JodaModule());
        return mapper.readValue(json, User.class);
    }

    @Override
    public String toString() {
        return toJson();
    }
}

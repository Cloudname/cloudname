package org.cloudname.a3.domain;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Extremely simple user database which is simply a collection of
 * users that can be looked up by username.  This implementation has
 * one wart in that it tracks the stat entry from ZooKeeper.
 *
 * @author borud
 */
public class UserDB {
    private Map<String, User> userMap;
    private int version;

    public UserDB() {
        userMap = new HashMap<String,User>();
    }

    private UserDB(Map<String, User> userMap) {
        this.userMap = userMap;
    }

    public UserDB setVersion(int version) {
        this.version = version;
        return this;
    }

    /**
     * @return version of the user database.
     */
    public int getVersion() {
        return version;
    }

    /**
     * Add a user to the user database.  If a user by the same
     * username already exists we replace this user.
     *
     * @param user the user we wish to add to the user database
     */
    public void addUser(User user) {
        userMap.put(user.getUsername(), user);
    }

    /**
     * Fetch user by username.
     *
     * @param username the username of the user we wish to look up
     * @return the User object with given {@code username} or {@code null}
     *   if the user does not exist.
     */
    public User getUser(String username) {
        return userMap.get(username);
    }

    /**
     * Remove user from user database.
     *
     * @param username the user name of the user we wish to remove.
     */
    public void deleteUser(String username) {
        userMap.remove(username);
    }

    public int getNumEntries() {
        return userMap.size();
    }

    public Set<String> getUserNames() {
        // Defensive copy and sort
        return new TreeSet(userMap.keySet());
    }

    /**
     * Get entire collection of Users from database.
     *
     * @return Unmodifiable Collection of User objects.
     */
    public Collection<User> getAllUsers() {
        return Collections.unmodifiableCollection(userMap.values());
    }

    /**
     * Serialize entire user database to json format.
     *
     * @return a json string containing all users
     */
    public String toJson() {
        // Comparator we use to sort usernames to ensure that when
        // serialized, the usernames are always output in alphabetical
        // order.  This might seem a bit unnecessary, but it can make
        // spotting errors easier.  Also, it is quite nice that users
        // are always sorted.  Don't you think? :-)
        Comparator<User> userCmp = new Comparator<User>() {
            @Override
            public int compare(User a, User b) {
                return a.getUsername().compareTo(b.getUsername());
            }
        };

        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new JodaModule());
            mapper.configure(SerializationFeature.INDENT_OUTPUT, true);

            List<User> users = new ArrayList<User>(userMap.values());

            // I am not particularly fond of the fact that
            // Collections.sort() does not have a version that returns
            // a list but that it only operates on Lists in-place.
            Collections.sort(users, userCmp);
            return mapper.writeValueAsString(users.toArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Create a UserDB instance from a json string.
     *
     * @param json The serialized version of the user database as a
     *   JSON string.
     * @return a UserDB instance
     */
    public static UserDB fromJson(String json) {
        try {
            final ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new JodaModule());
            User[] users = mapper.readValue(json, User[].class);
            HashMap<String, User> m = new HashMap<String, User>();
            for (User user : users) {
                m.put(user.getUsername(), user);
            }
            return new UserDB(m);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
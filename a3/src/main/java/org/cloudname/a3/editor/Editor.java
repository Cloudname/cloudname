package org.cloudname.a3.editor;

import org.cloudname.a3.Password;
import org.cloudname.a3.domain.User;
import org.cloudname.a3.domain.UserDB;

import jline.ConsoleReader;
import jline.Completor;
import jline.ArgumentCompletor;
import jline.FileNameCompletor;
import jline.SimpleCompletor;
import jline.MultiCompletor;
import jline.NullCompletor;

import org.joda.time.DateTime;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Collections;

import java.io.IOException;
import java.io.EOFException;

/**
 * A simple editor utility for A3
 *
 * @author borud
 */
public class Editor {
    private ConsoleReader reader;

    // Current UserDB instance.  If any.
    private UserDB userDB;

    private static String[] commands = {
        "help",
        "newdb",
        "load",
        "save",
        "ls",
        "delete",
        "password",
        "checkpassword",
        "adduser"
    };


    public Editor() throws IOException {
        reader  = new ConsoleReader();

        // Add completion for the commands
        reader.addCompletor(new SimpleCompletor(commands));
    }

    private String getPrompt() {
        // Prompt if we have no user database
        if (null == userDB) {
            return "(no db) > ";
        }

        // put number of entries in prompt
        return "(" + userDB.getNumEntries() + ") > ";
    }

    private void mainInputLoop() throws IOException {

        String line;
        while ((line = reader.readLine(getPrompt())) != null) {
            line = line.trim();

            if (line.equals("adduser")) {
                cmdAddUser(line);
                continue;
            }

            if (line.equals("newdb")) {
                cmdNewDB(line);
                continue;
            }

            if (line.equals("ls")) {
                cmdList(line);
                continue;
            }

            if (line.startsWith("delete")) {
                cmdDelete(line);
                continue;
            }

            if (line.startsWith("password")) {
                cmdPassword(line);
                continue;
            }

            if (line.startsWith("checkpassword")) {
                cmdCheckPassword(line);
                continue;
            }

            if (line.equals("help")) {
                cmdHelp(line);
                continue;
            }

            System.out.println("*** Unknown command: '" + line + "'");
        }
    }

    private void cmdList(String line) {
        if (null == userDB) {
            return;
        }

        for (String username : userDB.getUserNames()) {
            System.out.println(" - " + userDB.getUser(username).toJson());
        }
    }

    private void cmdNewDB(String line) throws IOException {
        if (null != userDB) {
            System.out.println("*** Warning, discarding existing userdb");
            String response = reader.readLine("  Discard user database? (yes/NO): ").trim();
            if (! response.equals("yes")) {
                System.out.println("*** User database kept");
                return;
            } else {
                System.out.println("*** Discarded user database");
            }
        }

        userDB = new UserDB();
        System.out.println("*** Creating new user database");
    }

    private void cmdDelete(String line) throws IOException {
        if (null == userDB) {
            System.out.println("*** No current userdatabase.  Please create using newdb or load user database.");
        }

        String[] parts = line.split("\\s+");
        if (parts.length < 2) {
            System.out.println("*** Missing username(s)");
        }

        for (int i = 1; i < parts.length; i++) {
            String username = parts[i];
            if (null == userDB.getUser(username)) {
                System.out.println("*** User '" + username + "' did not exist");
                continue;
            }

            userDB.deleteUser(username);
            System.out.println("*** User '" + username + "' deleted");
        }
    }

    private void cmdPassword(String line) throws IOException {
        String pass1 = reader.readLine("  | Password : ", '*').trim();
        String pass2 = reader.readLine("  |   Repeat : ", '*').trim();

        if (pass1.equals(pass2)) {
            System.out.println("*** Password string: " + Password.hashSecret(pass1));
        } else {
            System.out.println("*** Passwords did not match");
        }
    }

    private void cmdCheckPassword(String line) throws IOException {
        String pass = reader.readLine("  | Password : ", '*').trim();
        String hash = reader.readLine("  |     Hash : ").trim();

        if (Password.matchSecret(pass, hash)) {
            System.out.println("*** Password ok");
        } else {
            System.out.println("*** Password does not validate");
        }
    }

    /**
     * Presumes that UserDB is non-null.
     */
    private void cmdAddUser(String line) throws IOException {
        if (null == userDB) {
            System.out.println("*** No current user database either load a user database or create a new user database");
            return;
        }

        Set<String> roles = new HashSet<String>();

        reader.setUseHistory(false);
        String username  = reader.readLine("  | username : ").trim();

        String password;
        String password2;
        Map<String,String> properties = null;

        do {
            password  = reader.readLine("  | password : ", '*').trim();
            password2 = reader.readLine("  | ..verify : ", '*').trim();
            // If passwords are equal and non-empty we are done
            if (!password.isEmpty() && password.equals(password2)) {
                break;
            }
            System.out.println("\n*** Try harder\n");
        } while (true);

        String oldPassword = reader.readLine("  | old password (hashed): ").trim();
        if (oldPassword.isEmpty()) {
            oldPassword = null;
        }

        DateTime oldPasswordExpiry = null;
        if (oldPassword != null) {
            while (oldPasswordExpiry == null) {
                final String expiryString
                    = reader.readLine(
                        "  | old password expiry (ISO-8601 format): ")
                    .trim();
                try {
                    oldPasswordExpiry = DateTime.parse(expiryString);
                } catch (Exception e) {
                    System.out.println("\n*** Could not parse, try again\n");
                }
            }
        }
        String realName  = reader.readLine("  | realName   : ").trim();
        String email     = reader.readLine("  | email      : ").trim();
        String rolesStr  = reader.readLine("  | roles      : ").trim();

        System.out.println("Properties using JSON syntax {\"key\":\"value\", ...})");

        // Process properties
        while (null == properties) {
            String propStr   = reader.readLine("  | properties : ").trim();
            try {
                properties = (HashMap<String,String>) new ObjectMapper().readValue(propStr, HashMap.class);
                break;
            } catch (JsonParseException e) {
                System.out.println("Invalid property syntax.  Try again.  Remember: JSON map syntax.");
            } catch (EOFException e) {
                // User entered nothing.  Allocate empty map.
                properties = Collections.emptyMap();
            }
        }

        // Process roles
        for (String role : rolesStr.split("\\s+")) {
            roles.add(role);
        }


        // Create a user
        User user = new User(username,
                             Password.hashSecret(password),
                             oldPassword,
                             oldPasswordExpiry,
                             realName,
                             email,
                             roles,
                             properties);

        // Add user to userDB instance
        userDB.addUser(user);

        System.out.println("\n===> " + user.toJson() + "\n");
        reader.setUseHistory(true);

    }

    private void cmdHelp(String line) {
        System.out.println(
            ""
            + "  newdb - create new database\n"
            + "  load <service coordinate | file> - load database from coordinate or file\n"
            + "  save <service coordinate | file> - save database to coordinate or file\n"
            + "  ls - list current user database\n"
            + "  adduser - add a user\n"
            + "  password - convert password to bcrypt hash\n"
            + "  checkpassword - check a password against a bcrypt hash\n"
            + "  delete <username>\n"
            + ""
        );
    }

    public static void main(String[] args) throws IOException {
        Editor editor = new Editor();
        editor.mainInputLoop();
    }
}
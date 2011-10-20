package org.cloudname.a3.storage;

/**
 * Created by IntelliJ IDEA.
 *
 * @author borud
 */
public class A3StorageException extends RuntimeException {
    public A3StorageException(String s) {
        super(s);
    }

    public A3StorageException(Throwable e) {
        super(e);
    }
}

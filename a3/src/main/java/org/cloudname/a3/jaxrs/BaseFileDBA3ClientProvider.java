package org.cloudname.a3.jaxrs;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.util.logging.Logger;

import javax.ws.rs.core.Context;

import org.cloudname.a3.A3Client;

import com.sun.jersey.core.spi.component.ComponentContext;
import com.sun.jersey.core.spi.component.ComponentScope;
import com.sun.jersey.spi.inject.Injectable;
import com.sun.jersey.spi.inject.InjectableProvider;

/**
 * Abstract class for use to provide A3client with DB read from file.
 */
public abstract class BaseFileDBA3ClientProvider
    implements InjectableProvider<Context, Type>
{
    private static final Logger logger = Logger.getLogger(
        BaseFileDBA3ClientProvider.class.getName());

    private static A3Client makeEmptyDbA3Client()
    {
        final String emptyUserDbJson = "[]";
        final Reader reader = new StringReader(emptyUserDbJson);
        try {
            return A3Client.newMemoryOnlyClient(reader);
        } catch (IOException e) {
            throw new RuntimeException("Unexpected", e);
        }
    }

    private static A3Client makeA3ClientFromFile(final String file) throws IOException {
        final InputStream stream = new FileInputStream(file);
        return makeA3ClientFromStream(stream);
    }

    private static A3Client makeA3ClientFromStream(final InputStream stream) throws IOException {
        final Charset charset = Charset.forName("UTF-8");
        final Reader reader = new InputStreamReader(stream, charset);
        return A3Client.newMemoryOnlyClient(reader);
    }

    private static A3Client a3Client;

    protected static void initProvider(String file) {
        if (file == null || file.isEmpty()) {
            logger.warning("No user database file specified.");
            a3Client = makeEmptyDbA3Client();
        } else {
            try {
                a3Client = makeA3ClientFromFile(file);
            } catch (IOException e) {
                throw new RuntimeException("Unable to read user database from file " + file, e);
            }
        }
        a3Client.open();
    }

    @Override
    public ComponentScope getScope() {
        return ComponentScope.Singleton;
    }

    @Override
    public Injectable getInjectable(
        final ComponentContext componentContext,
        final Context annotation,
        final Type type)
    {
        if (type.equals(A3Client.class)) {
            return new Injectable<A3Client>() {
                @Override
                public A3Client getValue() {
                    return a3Client;
                }
            };
        }

        // Asked to provide something other than A3Client. We don't do that.
        return null;
    }
}

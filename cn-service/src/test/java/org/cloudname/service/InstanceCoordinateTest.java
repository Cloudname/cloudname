package org.cloudname.service;

import org.cloudname.core.CloudnamePath;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

public class InstanceCoordinateTest {
    @Test
    public void testCreation() {
        final String[] path = new String[] { "region", "tag", "service", "instance" };
        final InstanceCoordinate coordinate = new InstanceCoordinate(new CloudnamePath(path));

        final String canonicalString = coordinate.toCanonicalString();
        assertThat(canonicalString, is("instance.service.tag.region"));

        final InstanceCoordinate fromCanonical = InstanceCoordinate.parse(canonicalString);
        assertThat(fromCanonical.toCanonicalString(), is(canonicalString));
        assertThat(fromCanonical.getRegion(), is(coordinate.getRegion()));
        assertThat(fromCanonical.getTag(), is(coordinate.getTag()));
        assertThat(fromCanonical.getService(), is(coordinate.getService()));
        assertThat(fromCanonical.getInstance(), is(coordinate.getInstance()));

        final String jsonString = coordinate.toJsonString();
        final InstanceCoordinate fromJson = InstanceCoordinate.fromJson(jsonString);
        assertThat(fromJson.getRegion(), is(coordinate.getRegion()));
        assertThat(fromJson.getTag(), is(coordinate.getTag()));
        assertThat(fromJson.getService(), is(coordinate.getService()));
        assertThat(fromJson.getInstance(), is(coordinate.getInstance()));
        assertThat(fromJson.toCanonicalString(), is(coordinate.toCanonicalString()));
    }

    @Test
    public void testPathConversion() {
        final CloudnamePath path = new CloudnamePath(
                new String[] {"test", "local", "service", "instance" });

        final InstanceCoordinate coordinate = new InstanceCoordinate(path);

        final CloudnamePath cnPath = coordinate.toCloudnamePath();
        assertThat(cnPath.length(), is(path.length()));
        assertThat(cnPath, is(equalTo(path)));
    }

    /**
     * Ensure toString() has a sensible representation ('ish)
     */
    @Test
    public void toStringMethod() {
        final CloudnamePath pathA = new CloudnamePath(
                new String[] {"test", "local", "service", "instance" });
        final CloudnamePath pathB = new CloudnamePath(
                new String[] {"test", "local", "service", "instance" });
        final CloudnamePath pathC = new CloudnamePath(
                new String[] {"test", "local", "service", "x" });

        final InstanceCoordinate a = new InstanceCoordinate(pathA);
        final InstanceCoordinate b = new InstanceCoordinate(pathB);
        final InstanceCoordinate c = new InstanceCoordinate(pathC);
        assertThat(a.toString(), is(a.toString()));
        assertThat(a.toString(), is(not(c.toString())));

        assertThat(a.toCanonicalString(), is(b.toCanonicalString()));
    }

    @Test
    public void invalidStringConversion() {
        assertThat(InstanceCoordinate.parse("foo:bar.baz"), is(nullValue()));
        assertThat(InstanceCoordinate.parse(null), is(nullValue()));
        assertThat(InstanceCoordinate.parse("foo.bar.baz"), is(nullValue()));
        assertThat(InstanceCoordinate.parse(""), is(nullValue()));
    }

    @Test (expected = IllegalArgumentException.class)
    public void invalidNames2() {
        assertThat(InstanceCoordinate.parse("æ.ø.å.a"), is(nullValue()));
    }

    @Test (expected = IllegalArgumentException.class)
    public void nullPathInConstructor() {
        new InstanceCoordinate(null);
    }

    @Test (expected = IllegalArgumentException.class)
    public void invalidPathInConstructor() {
        new InstanceCoordinate(new CloudnamePath(new String[] { "foo" }));
    }
}

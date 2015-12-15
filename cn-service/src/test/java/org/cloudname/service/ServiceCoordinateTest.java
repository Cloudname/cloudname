package org.cloudname.service;

import org.cloudname.core.CloudnamePath;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

public class ServiceCoordinateTest {
    private final CloudnamePath cnPath = new CloudnamePath(
            new String[] { "local", "test", "service" });


    @Test
    public void testCreation() {
        final ServiceCoordinate coordinate = new ServiceCoordinate(cnPath);
        assertThat(coordinate.getRegion(), is(cnPath.get(0)));
        assertThat(coordinate.getTag(), is(cnPath.get(1)));
        assertThat(coordinate.getService(), is(cnPath.get(2)));
    }

    @Test
    public void testParse() {
        final ServiceCoordinate coord = ServiceCoordinate.parse("service.tag.region");
        assertThat(coord.getRegion(), is("region"));
        assertThat(coord.getTag(), is("tag"));
        assertThat(coord.getService(), is("service"));
    }

    @Test
    public void testEquals() {
        final ServiceCoordinate coordA = ServiceCoordinate.parse("a.b.c");
        final ServiceCoordinate coordB = ServiceCoordinate.parse("a.b.c");
        final ServiceCoordinate coordC = ServiceCoordinate.parse("a.b.d");
        final ServiceCoordinate coordD = ServiceCoordinate.parse("a.a.c");
        final ServiceCoordinate coordE = ServiceCoordinate.parse("a.a.a");
        final ServiceCoordinate coordF = ServiceCoordinate.parse("c.b.c");

        assertThat(coordA, is(equalTo(coordB)));
        assertThat(coordB, is(equalTo(coordA)));

        assertThat(coordA, is(not(equalTo(coordC))));
        assertThat(coordA, is(not(equalTo(coordD))));
        assertThat(coordA, is(not(equalTo(coordE))));
        assertThat(coordA, is(not(equalTo(coordF))));

        assertThat(coordA.equals(null), is(false));
        assertThat(coordA.equals(new Object()), is(false));
    }

    @Test
    public void testHashCode() {
        final ServiceCoordinate coordA = ServiceCoordinate.parse("a.b.c");
        final ServiceCoordinate coordB = ServiceCoordinate.parse("a.b.c");
        final ServiceCoordinate coordC = ServiceCoordinate.parse("x.x.x");
        assertThat(coordA.hashCode(), is(coordB.hashCode()));
        assertThat(coordC.hashCode(), is(not(coordA.hashCode())));
    }
    @Test
    public void testInvalidCoordinateString0() {
        assertThat(ServiceCoordinate.parse("foo bar baz"), is(nullValue()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidCoordinateString1() {
        ServiceCoordinate.parse("..");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidCoordinateString2() {
        ServiceCoordinate.parse("_._._");
    }

    @Test(expected = IllegalArgumentException.class)
    public void nullPathParameter() {
        new ServiceCoordinate(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void illegalPathParameter() {
        new ServiceCoordinate(new CloudnamePath(new String[] { "foo" }));
    }

}

package org.cloudname;

import org.junit.*;
import static org.junit.Assert.*;

/**
 * Unit tests for Coordinate.
 *
 * @author borud
 */
public class CoordinateTest {
    @Test
    public void testSimple() throws Exception {
        Coordinate c = Coordinate.parse("1.service.user.cell");
        assertNotNull(c);
        assertEquals(1, c.getInstance());
        assertEquals("service", c.getService());
        assertEquals("user", c.getUser());
        assertEquals("cell", c.getCell());
    }

    @Test (expected = IllegalArgumentException.class)
    public void testInvalidInstanceNumber() throws Exception {
        new Coordinate(-1, "service", "user", "cell");
    }

    @Test
    public void testEquals() throws Exception {
        assertEquals(
            new Coordinate(1,"foo", "bar", "baz"),
            new Coordinate(1, "foo", "bar", "baz")
        );
    }

    @Test
    public void testSymmetry() throws Exception {
        String s = "0.fooservice.baruser.bazcell";
        assertEquals(s, Coordinate.parse(s).asString());
        assertEquals(s, new Coordinate(0,
                                       "fooservice",
                                       "baruser",
                                       "bazcell").asString());

        System.out.println(Coordinate.parse(s));
    }

    @Test (expected = IllegalArgumentException.class)
    public void testInvalidInstance() throws Exception {
        Coordinate.parse("invalid.service.user.cell");
    }

    @Test (expected = IllegalArgumentException.class)
    public void testInvalidCharacters() throws Exception {
        Coordinate.parse("0.ser!vice.user.cell");
    }

    @Test
    public void testLegalCharacters() throws Exception {
        Coordinate.parse("0.service-test.user.cell");
        Coordinate.parse("0.service_test.user.cell");
        Coordinate.parse("0.service.user-foo.cell");
        Coordinate.parse("0.service.user_foo.ce_ll");
    }

    @Test (expected = IllegalArgumentException.class)
    public void testRequireStartsWithLetter() throws Exception {
        Coordinate.parse("0._aaa._bbb._ccc");
    }

    @Test (expected = IllegalArgumentException.class)
    public void testIllegalArgumentsConstructor() throws Exception {
        new Coordinate(1, "service", "_user", "cell");
    }
}
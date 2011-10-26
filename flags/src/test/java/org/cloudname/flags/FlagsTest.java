package org.cloudname.flags;

import junit.framework.Assert;

import org.junit.Test;

/**
 * Test class for Flags.
 *
 * @author acidmoose
 *
 */
public class FlagsTest {

    /**
     * Test all supported field types.
     */
    @Test
    public void testSimpleRun() {
        Flags flags = new Flags()
        .loadOpts(FlagsAllLegalFields.class)
        .parse(new String[]{"--boolean", "true",
                "--Boolean", "true",
                "--string", "stringtest",
                "--int", "10",
                "--Integer", "20",
                "--long", "30",
                "--Long", "40"});

        Assert.assertFalse("Help seems to have been called. It should not have been.", flags.helpFlagged());

        Assert.assertEquals(true, FlagsAllLegalFields.bool);
        Assert.assertEquals(new Boolean(true), FlagsAllLegalFields.bool2);
        Assert.assertEquals("stringtest", FlagsAllLegalFields.string);
        Assert.assertEquals(10, FlagsAllLegalFields.integer);
        Assert.assertEquals(new Integer(20), FlagsAllLegalFields.integer2);
        Assert.assertEquals(30, FlagsAllLegalFields.longNum);
        Assert.assertEquals(40, FlagsAllLegalFields.longNum2);
        flags.printHelpSorted(System.out);
    }

    /**
     * Test all supported field types with default value.
     */
    @Test
    public void testDefaultValues() {
        Flags flags = new Flags()
        .loadOpts(FlagsAllLegalFields.class)
        .parse(new String[]{});

        Assert.assertFalse("Help seems to have been called. It should not have been.", flags.helpFlagged());

        Assert.assertEquals(false, FlagsAllLegalFields.bool);
        Assert.assertEquals(new Boolean(false), FlagsAllLegalFields.bool2);
        Assert.assertEquals("NA", FlagsAllLegalFields.string);
        Assert.assertEquals(1, FlagsAllLegalFields.integer);
        Assert.assertEquals(new Integer(1), FlagsAllLegalFields.integer2);
        Assert.assertEquals(1, FlagsAllLegalFields.longNum);
        Assert.assertEquals(1, FlagsAllLegalFields.longNum2);
    }

    /**
     * Test the fail event when a required parameter is not supplied.
     */
    @Test (expected = IllegalArgumentException.class)
    public void testRequiredArg() {
        Flags flags = new Flags()
        .loadOpts(FlagsRequiredArg.class)
        .parse(new String[]{});
    }

    /**
     * Test unsupported argument.
     */
    @Test (expected = IllegalArgumentException.class)
    public void testUnsupportedVariable() {
        Flags flags = new Flags()
        .loadOpts(FlagsWrongVariable.class);
    }

    /**
     * Test non static field.
     */
    @Test (expected = IllegalArgumentException.class)
    public void testNotStaticVariable() {
        Flags flags = new Flags()
        .loadOpts(FlagsNonStaticVariable.class);
    }

}

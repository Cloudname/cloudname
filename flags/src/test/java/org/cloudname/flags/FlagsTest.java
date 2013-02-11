package org.cloudname.flags;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;

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
        .parse(new String[]{});

        assertFalse("Help seems to have been called. It should not have been.", flags.helpFlagged());

        assertEquals(false, FlagsAllLegalFields.bool);
        assertEquals(new Boolean(false), FlagsAllLegalFields.bool2);
        assertEquals("NA", FlagsAllLegalFields.string);
        assertEquals(1, FlagsAllLegalFields.getValueForPrivateInteger());
        assertEquals(new Integer(1), FlagsAllLegalFields.integer2);
        assertEquals(1, FlagsAllLegalFields.longNum);
        assertEquals(1, FlagsAllLegalFields.longNum2);
        assertEquals(FlagsAllLegalFields.SimpleEnum.OPTION1, FlagsAllLegalFields.option);
        flags.printFlags();
        flags.printHelp(System.out);

        flags.parse(new String[]{"--boolean", "true",
                "--Boolean", "true",
                "--string", "stringtest",
                "--int", "10",
                "--Integer", "20",
                "--long", "30",
                "--Long", "40",
                "--option", "OPTION2"});

        assertFalse("Help seems to have been called. It should not have been.", flags.helpFlagged());

        assertEquals(true, FlagsAllLegalFields.bool);
        assertEquals(new Boolean(true), FlagsAllLegalFields.bool2);
        assertEquals("stringtest", FlagsAllLegalFields.string);
        assertEquals(10, FlagsAllLegalFields.getValueForPrivateInteger());
        assertEquals(new Integer(20), FlagsAllLegalFields.integer2);
        assertEquals(30, FlagsAllLegalFields.longNum);
        assertEquals(40, FlagsAllLegalFields.longNum2);
        assertEquals(FlagsAllLegalFields.SimpleEnum.OPTION2, FlagsAllLegalFields.option);

        // Just make sure that printHelp() produces something.  Since
        // the format should change to something more sensible we do
        // not do any checks on content just yet.
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        flags.printHelp(baos);
        assertTrue(baos.size() > 0);
    }

    /**
     * Test all supported field types with an instanced flagged class.
     */
    @Test
    public void testInstanceConfiguration() {
        final FlagsInstanced flaggedClass = new FlagsInstanced();
        Flags flags = new Flags()
            .loadOpts(flaggedClass)
            .parse(new String[]{});

        assertEquals(false, flaggedClass.bool);
        assertEquals(new Boolean(false), flaggedClass.bool2);
        assertEquals("NA", flaggedClass.string);
        assertEquals(1, flaggedClass.getValueForPrivateInteger());
        assertEquals(new Integer(1), flaggedClass.integer2);
        assertEquals(1, flaggedClass.longNum);
        assertEquals(1, flaggedClass.longNum2);
        assertEquals(FlagsInstanced.SimpleEnum.OPTION1, flaggedClass.option);

        flags.parse(new String[]{"--boolean", "true",
            "--Boolean", "true",
            "--string", "stringtest",
            "--int", "10",
            "--Integer", "20",
            "--long", "30",
            "--Long", "40",
            "--option", "OPTION2"});

        assertEquals(true, flaggedClass.bool);
        assertEquals(new Boolean(true), flaggedClass.bool2);
        assertEquals("stringtest", flaggedClass.string);
        assertEquals(10, flaggedClass.getValueForPrivateInteger());
        assertEquals(new Integer(20), flaggedClass.integer2);
        assertEquals(30, flaggedClass.longNum);
        assertEquals(40, flaggedClass.longNum2);
        assertEquals(FlagsInstanced.SimpleEnum.OPTION2, flaggedClass.option);

        // Check that a new config class does not inherent parses from the first.
        final FlagsInstanced flaggedClass2 = new FlagsInstanced();
        assertEquals(false, flaggedClass2.bool);
        assertEquals(new Boolean(false), flaggedClass2.bool2);
        assertEquals("NA", flaggedClass2.string);
        assertEquals(1, flaggedClass2.getValueForPrivateInteger());
        assertEquals(new Integer(1), flaggedClass2.integer2);
        assertEquals(1, flaggedClass2.longNum);
        assertEquals(1, flaggedClass2.longNum2);
        assertEquals(FlagsInstanced.SimpleEnum.OPTION1, flaggedClass2.option);
    }

    /**
     * Boolean flags should be set to true if no parameter is set, or parameter is set to true.
     * False otherwise.
     */
    @Test
    public void testBooleanFlag() {
        Flags flags = new Flags()
            .loadOpts(FlagsBooleanFlag.class)
        .parse(new String[] {"--boolean", "--Boolean"});

        assertEquals(true, FlagsBooleanFlag.bool);
        assertEquals(true, FlagsBooleanFlag.bool2);

        flags.parse(new String[] {"--boolean", "false", "--Boolean=false"});

        assertEquals(false, FlagsBooleanFlag.bool);
        assertEquals(false, FlagsBooleanFlag.bool2);
    }

    /**
     * Test that printing help does not crash on various cases.
     */
    @Test
    public void testPrintHelp() {
        try {
            Flags flags = new Flags()
            .loadOpts(FlagsHelpTest.class);
            flags.printHelp(System.out);
        } catch (Exception e) {
            assertFalse("Cought exception.", true);
        }
    }

    /**
     * Flagged enum option should not work with wrong case.
     */
    @Test (expected = IllegalArgumentException.class)
    public void testEnumFlagWithWrongCase() {
        Flags flags = new Flags()
            .loadOpts(FlagsEnum.class)
        .parse(new String[]{"--option", "option2"});
    }

    /**
     * Test flags.getNonOptionArguments().
     */
    @Test
    public void testNonOptionArgs() {
        Flags flags = new Flags()
            .loadOpts(FlagsOptionTest.class)
        .parse(new String[]{"--int", "10",
                "--Integer", "20",
                "nonoptionarg1",
                "nonoptionarg2"});
        assertEquals(true, flags.getNonOptionArguments().contains("nonoptionarg1"));
        assertEquals(true, flags.getNonOptionArguments().contains("nonoptionarg2"));
    }

    /**
     * Test flags.getFlagsAsSet.
     */
    @Test
    public void testFlagsAsList() {
        Flags flags = new Flags()
            .loadOpts(FlagsOptionTest.class);

        assertEquals(flags.getFlagsAsList().size(), 2);
        assertEquals(flags.getFlagsAsList().get(0).name(), "int");
        assertEquals(flags.getFlagsAsList().get(1).name(), "Integer");
    }

    /**
     * Two flags with the same name should not work.
     */
    @Test (expected = IllegalArgumentException.class)
    public void testMultipleFlagsWithSameName() {
        Flags flags = new Flags()
        .loadOpts(FlagsAllLegalFields.class)
        .loadOpts(FlagsDoubleName.class);
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

    /**
     * Test no default value and not given in argument.
     * Should not do anything, and of course not crash.
     */
    @Test
    public void testArgumentNotGivenForValueWithoutDefault() {
        Assert.assertNull(FlagsArgumentNPE.argWithoutDefault);

        Flags flags = new Flags()
        .loadOpts(FlagsArgumentNPE.class)
        .parse(new String[] {});

        Assert.assertNull(FlagsArgumentNPE.argWithoutDefault);
    }

}

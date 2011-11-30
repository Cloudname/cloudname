package org.cloudname.flags;

import java.io.ByteArrayOutputStream;

import org.junit.*;
import static org.junit.Assert.*;

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
        assertEquals(1, FlagsAllLegalFields.integer);
        assertEquals(new Integer(1), FlagsAllLegalFields.integer2);
        assertEquals(1, FlagsAllLegalFields.longNum);
        assertEquals(1, FlagsAllLegalFields.longNum2);
        flags.printFlags();

        flags.parse(new String[]{"--boolean", "true",
                "--Boolean", "true",
                "--string", "stringtest",
                "--int", "10",
                "--Integer", "20",
                "--long", "30",
                "--Long", "40"});

        assertFalse("Help seems to have been called. It should not have been.", flags.helpFlagged());

        assertEquals(true, FlagsAllLegalFields.bool);
        assertEquals(new Boolean(true), FlagsAllLegalFields.bool2);
        assertEquals("stringtest", FlagsAllLegalFields.string);
        assertEquals(10, FlagsAllLegalFields.integer);
        assertEquals(new Integer(20), FlagsAllLegalFields.integer2);
        assertEquals(30, FlagsAllLegalFields.longNum);
        assertEquals(40, FlagsAllLegalFields.longNum2);

        // Just make sure that printHelp() produces something.  Since
        // the format should change to something more sensible we do
        // not do any checks on content just yet.
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        flags.printHelp(baos);
        assertTrue(baos.size() > 0);
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

    @Test
    public void testNonOptionArgs() {
        Flags flags = new Flags()
        .loadOpts(FlagsAllLegalFields.class)
        .parse(new String[]{"--int", "10",
                "--Integer", "20",
                "nonoptionarg1",
                "nonoptionarg2"});
        assertEquals(true, flags.getNonOptionArguments().contains("nonoptionarg1"));
        assertEquals(true, flags.getNonOptionArguments().contains("nonoptionarg2"));
    }

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

}

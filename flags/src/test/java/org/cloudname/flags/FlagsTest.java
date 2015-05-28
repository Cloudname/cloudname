package org.cloudname.flags;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import javax.annotation.PostConstruct;
import junit.framework.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Test class for Flags.
 *
 * @author acidmoose
 *
 */
public class FlagsTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

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
     * Test all supported field types with an instanced flagged class.
     */
    @Test
    public void testInstanceAndClassConfiguration() {
        final InstancedFlags instancedFlags = new InstancedFlags();
        Flags flags = new Flags()
            .loadOpts(instancedFlags)
            .loadOpts(ClassFlags.class)
            .parse(new String[]{});

        assertEquals("NA", instancedFlags.string);
        assertEquals("NA", ClassFlags.string);

        flags.parse(new String[]{"--classString", "A",
            "--instanceString", "A"});

        assertEquals("A", instancedFlags.string);
        assertEquals("A", ClassFlags.string);

        // Check that printHelp does not crash.
        flags.printHelp(System.out);
        flags.printVersion(System.out);
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
     * Test that --help and --version does not trigger ArgumentException when parsing flags are required.
     */
    @Test
    public void testRequiredArgWithHelp() {
        try {
            Flags helpFlags = new Flags()
                .loadOpts(FlagsRequiredArg.class)
                .parse(new String[]{"--help"});
            helpFlags.printHelp(System.out);

            Flags versionFlags = new Flags()
                .loadOpts(FlagsRequiredArg.class)
                .parse(new String[]{"--version"});
            versionFlags.printVersion(System.out);
        } catch (Exception e) {
            e.printStackTrace();
            assertFalse("Should not throw exceptions", true);
        }
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

    /**
     * Test that --properties-file loads flags from a file
     */
    @Test
    public void testLoadingFromPropertiesFile() throws Exception {
        File propertiesFile = File.createTempFile("test", "properties");
        propertiesFile.setWritable(true);
        FileOutputStream fio = new FileOutputStream(propertiesFile);
        String properties = "integer=1\n\n"
                + "#comments=not included\n"
                + "name=myName\n"
                + "booleanvalue\n"
                + "help\n";
        fio.write(properties.getBytes());
        fio.close();
        Flags flags = new Flags()
            .loadOpts(FlagsPropertiesFile.class)
            .parse(new String[]{"--properties-file", propertiesFile.getAbsolutePath()});
        assertEquals("myName", FlagsPropertiesFile.name);
        assertTrue(FlagsPropertiesFile.booleanValue);
        assertTrue(flags.helpFlagged());
        assertEquals(1, FlagsPropertiesFile.integer);
        Assert.assertNull(FlagsPropertiesFile.comments);
    }

    /**
     * Test that --properties-file does not accept keys which are not defined as flags
     */
    @Test
    public void testLoadingUndefinedFlagFromPropertiesFile() throws Exception {
        exception.expect(joptsimple.OptionException.class);
        File propertiesFile = File.createTempFile("test", "properties");
        propertiesFile.setWritable(true);
        FileOutputStream fio = new FileOutputStream(propertiesFile);
        String properties = "not-in-options=abc\n";
        fio.write(properties.getBytes());
        fio.close();
        new Flags()
                .loadOpts(FlagsPropertiesFile.class)
                .parse(new String[]{"--properties-file", propertiesFile.getAbsolutePath()});
    }

    /**
     * Test that properties are loaded from two files using two --properties-files options
     */
    @Test
    public void testLoadingFromMultiplePropertyFiles() throws Exception {
        File propertiesFile1 = File.createTempFile("test1", "properties");
        propertiesFile1.setWritable(true);
        FileOutputStream fio1 = new FileOutputStream(propertiesFile1);
        String properties1 = "integer=1\n\n" +
            "help\n" + "version\n";
        fio1.write(properties1.getBytes());
        fio1.close();
        File propertiesFile2 = File.createTempFile("test2", "properties");
        propertiesFile2.setWritable(true);
        FileOutputStream fio2 = new FileOutputStream(propertiesFile2);
        String properties2 = "name=myName\n\n" +
            "help\n" + "version\n";
        fio2.write(properties2.getBytes());
        fio2.close();
        Flags flags = new Flags()
            .loadOpts(FlagsPropertiesFile.class)
            .parse(new String[]{"--properties-file", propertiesFile1.getAbsolutePath(),
                                "--properties-file", propertiesFile2.getAbsolutePath()
            });
        assertEquals("myName", FlagsPropertiesFile.name);
        assertEquals(1, FlagsPropertiesFile.integer);
        Assert.assertNull(FlagsPropertiesFile.comments);
    }

    /**
     * Test that properties are loaded from two files seperated by ';'
     */
    @Test
    public void testLoadingFromMultiplePropertyFiles2() throws Exception {
        File propertiesFile1 = File.createTempFile("test1", "properties");
        propertiesFile1.setWritable(true);
        FileOutputStream fio1 = new FileOutputStream(propertiesFile1);
        String properties1 = "integer=1\n\n" +
            "help\n" + "version\n";
        fio1.write(properties1.getBytes());
        fio1.close();
        File propertiesFile2 = File.createTempFile("test2", "properties");
        propertiesFile2.setWritable(true);
        FileOutputStream fio2 = new FileOutputStream(propertiesFile2);
        String properties2 = "name=myName\n\n" +
            "help\n" + "version\n";
        fio2.write(properties2.getBytes());
        fio2.close();
        new Flags()
            .loadOpts(FlagsPropertiesFile.class)
            .parse(new String[]{"--properties-file", propertiesFile1.getAbsolutePath() + ';'
                +propertiesFile2.getAbsolutePath()
            });
        assertEquals("myName", FlagsPropertiesFile.name);
        assertEquals(1, FlagsPropertiesFile.integer);
        Assert.assertNull(FlagsPropertiesFile.comments);
    }

    static class BaseFlaggedTestClass {
        static int baseCallCounter;

        @PostConstruct
        protected void base1() {baseCallCounter++;}

        @PostConstruct
        private void base2() {baseCallCounter++;}

    }
    static class FlaggedTestClass extends BaseFlaggedTestClass {
        @Flag (name="flag-one")
        private String flagOne;

        static int callCounter;

        @PostConstruct
        public void instance1() { callCounter++; }

        @PostConstruct
        private void instance2() {  callCounter++; }

        @PostConstruct
        protected void instance3() { callCounter++;}

        @PostConstruct
        static void instance4() {
            callCounter++;
        }
    }

    static class FlaggedTestStaticClass {
        @Flag (name="flag-two")
        private static String flagTwo;
        public static int callCounter;

        @PostConstruct
        public static void static1() { callCounter++; }

        // check that instance methods are ignored
        @PostConstruct
        public void instsance1() { callCounter++; }

    }

    @Test
    public void testPostConstructForInstanceConfiguration() {
        final String[] args = new String[] { "--flag-one", "xyz"};
        final FlaggedTestClass instance = new FlaggedTestClass();
        new Flags().loadOpts(instance).parse(args);

        assertThat(instance.flagOne, is("xyz"));
        assertThat(FlaggedTestClass.callCounter, is(3));

        // check that we don't support inherited methods for now
        assertThat(BaseFlaggedTestClass.baseCallCounter, is(0));
    }

    @Test
    public void testPostConstructForСlassConfiguration() {
        final String[] args = new String[] { "--flag-two", "abc"};
        new Flags().loadOpts(FlaggedTestStaticClass.class).parse(args);

        assertThat(FlaggedTestStaticClass.flagTwo, is("abc"));
        assertThat(FlaggedTestStaticClass.callCounter, is(1));
        // don't support i methods.
        assertThat(BaseFlaggedTestClass.baseCallCounter, is(0));
    }

    static class FlaggedTestClassInvalidMethod {
        @PostConstruct
        public void i1(final String string) { }
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testPostConstructInvalidMethod() {
        final FlaggedTestClassInvalidMethod invalidMethod = new FlaggedTestClassInvalidMethod();
        expectedException.expect(IllegalArgumentException.class);
        new Flags().loadOpts(invalidMethod).parse(new String[0]);
    }

    static class FlaggedTestClassThrowsException {
        @Flag (name="flag-one")
        private String flagOne;

        @PostConstruct
        public void i1() { throw new UnsupportedOperationException(); }
    }

    @Test
    public void testPostConstructExceptionInMethod() {
        final FlaggedTestClassThrowsException invalidMethod = new FlaggedTestClassThrowsException();
        expectedException.expectCause(isA(UnsupportedOperationException.class));
        new Flags().loadOpts(invalidMethod).parse(new String[0]);
    }
}

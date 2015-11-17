package org.cloudname.core;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;


public class CloudnamePathTest {
    private final String[] emptyElements = new String[] {};
    private final String[] oneElement = new String[] { "foo" };
    private final String[] twoElements = new String[] { "foo", "bar" };

    @Test (expected = IllegalArgumentException.class)
    public void elementsCantBeNull() {
        new CloudnamePath(null);
    }

    @Test (expected = IllegalArgumentException.class)
    public void pathCantBeNull() {
        new CloudnamePath(null, "foof");
    }

    @Test (expected = IllegalArgumentException.class)
    public void additionalElementCantBeNull() {
        new CloudnamePath(new CloudnamePath(new String[] { "foo" }), null);
    }

    @Test
    public void appendPath() {
        final CloudnamePath singleElement = new CloudnamePath(new String[] { "one" });
        final CloudnamePath twoElements = new CloudnamePath(new String[] { "one", "two" });
        assertThat("Elements aren't equal", singleElement, is(not(equalTo(twoElements))));
        final CloudnamePath appendedElement = new CloudnamePath(singleElement, "two");
        assertThat("Appended are equal", appendedElement, is(equalTo(twoElements)));
    }

    @Test
    public void elementAccess() {
        final CloudnamePath path = new CloudnamePath(twoElements);
        assertThat(path.get(0), is(twoElements[0]));
        assertThat(path.get(1), is(twoElements[1]));
    }

    @Test (expected = IndexOutOfBoundsException.class)
    public void elementAccessMustBeWithinBounds() {
        final CloudnamePath path = new CloudnamePath(twoElements);
        path.get(2);
    }

    @Test
    public void joinPaths() {
        final CloudnamePath empty = new CloudnamePath(emptyElements);
        assertThat("The empty path is length = 0", empty.length(), is(0));
        assertThat("String representation of emmpty path is empty string", empty.join('.'), is(""));

        final CloudnamePath one = new CloudnamePath(oneElement);
        assertThat("A single element path has length 1", one.length(), is(1));
        assertThat("String representation of a single element path is the element",
                one.join('.'), is(oneElement[0]));

        final CloudnamePath two = new CloudnamePath(twoElements);
        assertThat("Two element paths have length 2", two.length(), is(2));
        assertThat("String representation of two element paths includes both elements",
                two.join('.'), is(twoElements[0] + '.' + twoElements[1]));
    }

    @Test
    public void equalsTest() {
        final CloudnamePath twoA = new CloudnamePath(twoElements);
        final CloudnamePath twoB = new CloudnamePath(twoElements);
        final CloudnamePath none = new CloudnamePath(emptyElements);
        final CloudnamePath entirelyDifferent = new CloudnamePath(new String[] { "foo", "2" });

        assertThat("Identical paths are equal", twoA.equals(twoB), is(true));
        assertThat("Hash codes for equal objects are the same",
                twoA.hashCode(), is(twoB.hashCode()));
        assertThat("Identical paths are equal, ignore order", twoB, is(equalTo(twoA)));
        assertThat("Paths aren't equal to strings", twoA.equals(""), is(false));
        assertThat("Empty path does not equal actual path", twoA, is(not(equalTo(none))));
        assertThat("Null elements aren't equal", twoA.equals(null), is(false));
        assertThat("Differen is just different", twoA, is(not(equalTo(entirelyDifferent))));
    }

    @Test
    public void subpaths() {
        final String[] e1 = new String[] { "1", "2", "3", "4" };
        final String[] e2 = new String[] { "1", "2" };

        final CloudnamePath first = new CloudnamePath(e1);
        final CloudnamePath second = new CloudnamePath(e2);
        final CloudnamePath last = new CloudnamePath(twoElements);


        assertThat("More specific paths can't be subpaths", first.isSubpathOf(second), is(false));
        assertThat("More generic paths are subpaths", second.isSubpathOf(first), is(true));
        assertThat("A path can be subpath of itself", first.isSubpathOf(first), is(true));

        assertThat("Paths must match at root levels", last.isSubpathOf(second), is(false));

        assertThat("Null paths are not subpaths of anything", first.isSubpathOf(null), is(false));

        final CloudnamePath empty = new CloudnamePath(emptyElements);
        assertThat("An empty path is a subpath of everything", empty.isSubpathOf(first), is(true));
        assertThat("Empty paths can't have subpaths", first.isSubpathOf(empty), is(false));
    }

    @Test
    public void parentPaths() {
        final CloudnamePath originalPath = new CloudnamePath(new String[] { "foo", "bar", "baz" });

        assertTrue(originalPath.getParent().isSubpathOf(originalPath));

        assertThat(originalPath.getParent(), is(equalTo(
                new CloudnamePath(new String[] { "foo", "bar" }))));

        assertThat(originalPath.getParent().getParent(),
                is(equalTo(new CloudnamePath(new String[] { "foo" }))));

        final CloudnamePath emptyPath = new CloudnamePath(new String[] { });

        assertThat(originalPath.getParent().getParent().getParent(),
                is(equalTo(emptyPath)));

        assertThat(originalPath.getParent().getParent().getParent().getParent(),
                is(equalTo(emptyPath)));

        assertThat(emptyPath.getParent(), is(equalTo(emptyPath)));
    }
    @Test
    public void testToString() {
        final CloudnamePath one = new CloudnamePath(oneElement);
        final CloudnamePath two = new CloudnamePath(twoElements);
        final CloudnamePath three = new CloudnamePath(emptyElements);

        assertThat(one.toString(), is(notNullValue()));
        assertThat(two.toString(), is(notNullValue()));
        assertThat(three.toString(), is(notNullValue()));
    }

    @Test
    public void invalidPathNameWithHyphenFirst() {
        assertThat(CloudnamePath.isValidPathElementName("-invalid"), is(false));
    }

    @Test
    public void invalidPathNameIsNull() {
        assertThat(CloudnamePath.isValidPathElementName(null), is(false));
    }
    @Test
    public void invalidPathNameWithHyphenLast() {
        assertThat(CloudnamePath.isValidPathElementName("invalid-"), is(false));
    }

    @Test
    public void invalidPathNameWithEmptyString() {
        assertThat(CloudnamePath.isValidPathElementName(""), is(false));
    }

    @Test
    public void invalidPathNameWithIllegalChars() {
        assertThat(CloudnamePath.isValidPathElementName("__"), is(false));
    }

    @Test
    public void invalidPathNameWithTooLongLabel() {
        assertThat(CloudnamePath.isValidPathElementName(
                "sojarindfleischetikettierungsueberwachungsaufgabenuebertragungsgesetz"), is(false));
    }

    @Test
    public void labelNamesAreCaseInsensitive() {
        final CloudnamePath one = new CloudnamePath(new String[] { "FirstSecond" });
        final CloudnamePath two = new CloudnamePath(new String[] { "fIRSTsECOND" });
        assertTrue("Label names aren't case sensitive", one.equals(two));
    }

    @Test (expected = IllegalArgumentException.class)
    public void pathCanNotBeNull() {
        new CloudnamePath(null);
    }

    @Test (expected = IllegalArgumentException.class)
    public void pathElementsCanNotBeNull() {
        new CloudnamePath(new String[] { null, null });
    }

    @Test (expected = IllegalArgumentException.class)
    public void pathElementNamesCanNotBeInvalid() {
        new CloudnamePath(new String[] { "__", "foo", "bar"});
    }

    @Test (expected = IllegalArgumentException.class)
    public void additionalElementsMustBeValid() {
        new CloudnamePath(new CloudnamePath(new String[] { "foo" }), "__");
    }
}

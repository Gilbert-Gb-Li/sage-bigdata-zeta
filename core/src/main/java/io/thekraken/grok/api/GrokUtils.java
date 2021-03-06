package io.thekraken.grok.api;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * {@code GrokUtils} contain set of useful tools or methods.
 *
 * @author anthonycorbacho
 * @since 0.0.6
 */
class GrokUtils {

    /**
     * Extract Grok patter like %{FOO} to FOO, Also Grok pattern with semantic.
     */
    static final Pattern GROK_PATTERN = Pattern.compile(
            "%\\{" +
                    "(?<name>" +
                    "(?<pattern>[A-z0-9]+)" +
                    "(?::(?<subname>[A-z0-9_:;\\/\\s\\.]+))?" +
                    ")" +
                    "(?:=(?<definition>" +
                    "(?:" +
                    "(?:[^{}]+|\\.+)+" +
                    ")+" +
                    ")" +
                    ")?" +
                    "\\}");

    private static final Pattern NAMED_REGEX = Pattern
            .compile("\\(\\?<([a-zA-Z][a-zA-Z0-9]*)>");

    private static Set<String> getNameGroups(String regex) {
        Set<String> namedGroups = new LinkedHashSet<String>();
        Matcher m = NAMED_REGEX.matcher(regex);
        while (m.find()) {
            namedGroups.add(m.group(1));
        }
        return namedGroups;
    }

    static Map<String, String> namedGroups(Matcher matcher,
                                           String namedRegex) {
        Set<String> groupNames = getNameGroups(matcher.pattern().pattern());
        Matcher localMatcher = matcher.pattern().matcher(namedRegex);
        Map<String, String> namedGroups = new LinkedHashMap<>();
        if (localMatcher.find()) {
            for (String groupName : groupNames) {
                String groupValue = localMatcher.group(groupName);
                namedGroups.put(groupName, groupValue);
            }
        }
        return namedGroups;
    }
}

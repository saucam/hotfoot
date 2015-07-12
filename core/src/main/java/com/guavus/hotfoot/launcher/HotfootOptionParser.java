package com.guavus.hotfoot.launcher;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parser for HotfootGenerate command line options.
 */
public class HotfootOptionParser {
    // The following constants define the "main" name for the available options. They're defined
    // to avoid copy & paste of the raw strings where they're needed.
    //
    // The fields are not static so that they're exposed to Scala code that uses this class. See
    // SparkSubmitArguments.scala. That is also why this class is not abstract - to allow code to
    // easily use these constants without having to create dummy implementations of this class.
    protected final String CONF = "--conf";
    protected final String JARS = "--jars";

    protected final String PROPERTIES_FILE = "--properties-file";
    protected final String SCHEMA_FILE = "--schema-file";
    protected final String RECORDS_NUM = "--num-records";
    protected final String OUTPUT_FORMAT = "--output-format";
    protected final String OUTPUT_PATH = "--output-path";

    // Options that do not take arguments.
    protected final String HELP = "--help";
    protected final String VERBOSE = "--verbose";
    protected final String VERSION = "--version";


    /**
     * This is the canonical list of spark-submit options. Each entry in the array contains the
     * different aliases for the same option; the first element of each entry is the "official"
     * name of the option, passed to {@link #handle(String, String)}.
     * <p/>
     * Options not listed here nor in the "switch" list below will result in a call to
     * {@link $#handleUnknown(String)}.
     * <p/>
     * These two arrays are visible for tests.
     */
    final String[][] opts = {
            { CONF, "-c" },
            { JARS },
            { PROPERTIES_FILE },
            { SCHEMA_FILE },
            { RECORDS_NUM },
            { OUTPUT_FORMAT },
            { OUTPUT_PATH },
    };

    /**
     * List of switches (command line options that do not take parameters) recognized by spark-submit.
     */
    final String[][] switches = {
            { HELP, "-h" },
            { VERBOSE, "-v" },
            { VERSION },
    };

    /**
     * Parse a list of spark-submit command line options.
     * <p/>
     * See SparkSubmitArguments.scala for a more formal description of available options.
     *
     * @throws IllegalArgumentException If an error is found during parsing.
     */
    protected final void parse(List<String> args) {
        Pattern eqSeparatedOpt = Pattern.compile("(--[^=]+)=(.+)");

        int idx = 0;
        for (idx = 0; idx < args.size(); idx++) {
            String arg = args.get(idx);
            String value = null;

            Matcher m = eqSeparatedOpt.matcher(arg);
            if (m.matches()) {
                arg = m.group(1);
                value = m.group(2);
            }

            // Look for options with a value.
            String name = findCliOption(arg, opts);
            if (name != null) {
                if (value == null) {
                    if (idx == args.size() - 1) {
                        throw new IllegalArgumentException(
                                String.format("Missing argument for option '%s'.", arg));
                    }
                    idx++;
                    value = args.get(idx);
                }
                if (!handle(name, value)) {
                    break;
                }
                continue;
            }

            // Look for a switch.
            name = findCliOption(arg, switches);
            if (name != null) {
                if (!handle(name, null)) {
                    break;
                }
                continue;
            }

            if (!handleUnknown(arg)) {
                break;
            }
        }

        if (idx < args.size()) {
            idx++;
        }
        handleExtraArgs(args.subList(idx, args.size()));
    }

    /**
     * Callback for when an option with an argument is parsed.
     *
     * @param opt The long name of the cli option (might differ from actual command line).
     * @param value The value. This will be <i>null</i> if the option does not take a value.
     * @return Whether to continue parsing the argument list.
     */
    protected boolean handle(String opt, String value) {
        throw new UnsupportedOperationException();
    }

    /**
     * Callback for when an unrecognized option is parsed.
     *
     * @param opt Unrecognized option from the command line.
     * @return Whether to continue parsing the argument list.
     */
    protected boolean handleUnknown(String opt) {
        throw new UnsupportedOperationException();
    }

    /**
     * Callback for remaining command line arguments after either {@link #handle(String, String)} or
     * {@link #handleUnknown(String)} return "false". This will be called at the end of parsing even
     * when there are no remaining arguments.
     *
     * @param extra List of remaining arguments.
     */
    protected void handleExtraArgs(List<String> extra) {
        throw new UnsupportedOperationException();
    }

    private String findCliOption(String name, String[][] available) {
        for (String[] candidates : available) {
            for (String candidate : candidates) {
                if (candidate.equals(name)) {
                    return candidates[0];
                }
            }
        }
        return null;
    }


}

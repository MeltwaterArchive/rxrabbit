package com.meltwater.rxrabbit.util;

import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Logger class which provides a standardized way of outputting variables and their values.
 *
 * <p>
 * Example usage:
 * <code><pre>
 * Logger log = new Logger("example");
 * log.infoWithParams("Testing out logging", "excitement", 9001, "name", log.getName());
 * </pre></code>
 * Which would output something like this (depending on you slf4j backend configuration):
 * <pre>example INFO: Testing out logging [excitement=9001, name=example]</pre>
 * </p>
 *
 * <p>Note that variables must have a sane toString() method.</p>
 *
 */
public class Logger {
    private final org.slf4j.Logger logger;

    private static final List<Class<?>> JAVA_WRAPPER_TYPES = new ArrayList<Class<?>>() {{
        add(Boolean.class);
        add(Byte.class);
        add(Character.class);
        add(Double.class);
        add(Float.class);
        add(Integer.class);
        add(Long.class);
        add(Short.class);
        add(Void.class);
    }};

    public Logger(Class<?> clazz) {
        this(LoggerFactory.getLogger(clazz));
    }

    public Logger(String loggerName) {
        this(LoggerFactory.getLogger(loggerName));
    }

    protected Logger(org.slf4j.Logger logger) {
        this.logger = logger;
    }

    public String getName() {
        return logger.getName();
    }

    /**
     * @deprecated use {@link #traceWithParams(String, Object...)} instead.
     */
    @Deprecated
    public void trace(String message, Object... arguments) {
        if (!logger.isTraceEnabled()) {
            return;
        }
        try {
            logger.trace(buildLogMessage(message, arguments));
        } catch (IllegalArgumentException e) {
            logMessageAssemblyFailure(message, arguments);
        }
    }

    /**
     * @deprecated use {@link #traceWithParams(String, Throwable, Object...)} instead.
     */
    @Deprecated
    public void trace(String message, Throwable t, Object... arguments) {
        if (!logger.isTraceEnabled()) {
            return;
        }
        try {
            logger.trace(buildLogMessage(message, arguments), t);
        } catch (IllegalArgumentException e) {
            logMessageAssemblyFailure(message, arguments);
            logger.trace(message, t);
        }
    }

    public void traceWithParams(String message, Object... arguments) {
        if (!logger.isTraceEnabled()) {
            return;
        }
        try {
            logger.trace(buildLogMessage(message, arguments));
        } catch (IllegalArgumentException e) {
            logMessageAssemblyFailure(message, arguments);
        }
    }

    public void traceWithParams(String message, Throwable t, Object... arguments) {
        if (!logger.isTraceEnabled()) {
            return;
        }
        try {
            logger.trace(buildLogMessage(message, arguments), t);
        } catch (IllegalArgumentException e) {
            logMessageAssemblyFailure(message, arguments);
            logger.trace(message, t);
        }
    }

    /**
     * @deprecated use {@link #debugWithParams(String, Object...)} instead.
     */
    @Deprecated
    public void debug(String message, Object... arguments) {
        if (!logger.isDebugEnabled()) {
            return;
        }
        try {
            logger.debug(buildLogMessage(message, arguments));
        } catch (IllegalArgumentException e) {
            logMessageAssemblyFailure(message, arguments);
        }
    }

    /**
     * @deprecated use {@link #debugWithParams(String, Throwable, Object...)} instead.
     */
    @Deprecated
    public void debug(String message, Throwable t, Object... arguments) {
        if (!logger.isDebugEnabled()) {
            return;
        }
        try {
            logger.debug(buildLogMessage(message, arguments), t);
        } catch (IllegalArgumentException e) {
            logMessageAssemblyFailure(message, arguments);
            logger.debug(message, t);
        }
    }

    public void debugWithParams(String message, Object... arguments) {
        if (!logger.isDebugEnabled()) {
            return;
        }
        try {
            logger.debug(buildLogMessage(message, arguments));
        } catch (IllegalArgumentException e) {
            logMessageAssemblyFailure(message, arguments);
        }
    }

    public void debugWithParams(String message, Throwable t, Object... arguments) {
        if (!logger.isDebugEnabled()) {
            return;
        }
        try {
            logger.debug(buildLogMessage(message, arguments), t);
        } catch (IllegalArgumentException e) {
            logMessageAssemblyFailure(message, arguments);
            logger.debug(message, t);
        }
    }

    /**
     * @deprecated use {@link #infoWithParams(String, Object...)} instead.
     */
    @Deprecated
    public void info(String message, Object... arguments) {
        if (!logger.isInfoEnabled()) {
            return;
        }
        try {
            logger.info(buildLogMessage(message, arguments));
        } catch (IllegalArgumentException e) {
            logMessageAssemblyFailure(message, arguments);
        }
    }

    /**
     * @deprecated use {@link #infoWithParams(String, Throwable, Object...)} instead.
     */
    @Deprecated
    public void info(String message, Throwable t, Object... arguments) {
        if (!logger.isInfoEnabled()) {
            return;
        }
        try {
            logger.info(buildLogMessage(message, arguments), t);
        } catch (IllegalArgumentException e) {
            logMessageAssemblyFailure(message, arguments);
            logger.info(message, t);
        }
    }

    public void infoWithParams(String message, Object... arguments) {
        if (!logger.isInfoEnabled()) {
            return;
        }
        try {
            logger.info(buildLogMessage(message, arguments));
        } catch (IllegalArgumentException e) {
            logMessageAssemblyFailure(message, arguments);
        }
    }

    public void infoWithParams(String message, Throwable t, Object... arguments) {
        if (!logger.isInfoEnabled()) {
            return;
        }
        try {
            logger.info(buildLogMessage(message, arguments), t);
        } catch (IllegalArgumentException e) {
            logMessageAssemblyFailure(message, arguments);
            logger.info(message, t);
        }
    }

    /**
     * @deprecated use {@link #warnWithParams(String, Object...)} instead.
     */
    @Deprecated
    public void warn(String message, Object... arguments) {
        if (!logger.isWarnEnabled()) {
            return;
        }
        try {
            logger.warn(buildLogMessage(message, arguments));
        } catch (IllegalArgumentException e) {
            logMessageAssemblyFailure(message, arguments);
        }
    }

    /**
     * @deprecated use {@link #warnWithParams(String, Throwable, Object...)} instead.
     */
    @Deprecated
    public void warn(String message, Throwable t, Object... arguments) {
        if (!logger.isWarnEnabled()) {
            return;
        }
        try {
            logger.warn(buildLogMessage(message, arguments), t);
        } catch (IllegalArgumentException e) {
            logMessageAssemblyFailure(message, arguments);
            logger.warn(message, t);
        }
    }

    public void warnWithParams(String message, Object... arguments) {
        if (!logger.isWarnEnabled()) {
            return;
        }
        try {
            logger.warn(buildLogMessage(message, arguments));
        } catch (IllegalArgumentException e) {
            logMessageAssemblyFailure(message, arguments);
        }
    }

    public void warnWithParams(String message, Throwable t, Object... arguments) {
        if (!logger.isWarnEnabled()) {
            return;
        }
        try {
            logger.warn(buildLogMessage(message, arguments), t);
        } catch (IllegalArgumentException e) {
            logMessageAssemblyFailure(message, arguments);
            logger.warn(message, t);
        }
    }

    /**
     * @deprecated use {@link #errorWithParams(String, Object...)} instead.
     */
    @Deprecated
    public void error(String message, Object... arguments) {
        if (!logger.isErrorEnabled()) {
            return;
        }
        try {
            logger.error(buildLogMessage(message, arguments));
        } catch (IllegalArgumentException e) {
            logMessageAssemblyFailure(message, arguments);
        }
    }

    /**
     * @deprecated use {@link #errorWithParams(String, Throwable, Object...)} instead.
     */
    @Deprecated
    public void error(String message, Throwable t, Object... arguments) {
        if (!logger.isErrorEnabled()) {
            return;
        }
        try {
            logger.error(buildLogMessage(message, arguments), t);
        } catch (IllegalArgumentException e) {
            logMessageAssemblyFailure(message, arguments);
            logger.error(message, t);
        }
    }

    public void errorWithParams(String message, Object... arguments) {
        if (!logger.isErrorEnabled()) {
            return;
        }
        try {
            logger.error(buildLogMessage(message, arguments));
        } catch (IllegalArgumentException e) {
            logMessageAssemblyFailure(message, arguments);
        }
    }

    public void errorWithParams(String message, Throwable t, Object... arguments) {
        if (!logger.isErrorEnabled()) {
            return;
        }
        try {
            logger.error(buildLogMessage(message, arguments), t);
        } catch (IllegalArgumentException e) {
            logMessageAssemblyFailure(message, arguments);
            logger.error(message, t);
        }
    }

    private void logMessageAssemblyFailure(String message, Object... arguments) {
        logger.error(
                "Failed to assemble log message for logger {}! Arguments must be declared in pairs! message={}, arguments={}",
                getName(), message, Arrays.toString(arguments));
    }

    protected String buildLogMessage(String message, Object[] arguments) {
        if (arguments.length % 2 != 0) {
            throw new IllegalArgumentException(
                    "Arguments must be declared in pairs: (message, key, value, key2, value2, ...)");
        }
        final StringBuilder sb = new StringBuilder(message);
        if (arguments.length == 0) {
            return sb.toString();
        }
        sb.append(" [ ");
        for (int i = 0; i < arguments.length; i += 2) {
            append(sb, arguments[i], arguments[i+1]);

            if (i + 2 < arguments.length) {
                sb.append(", ");
            }
        }
        sb.append(" ]");
        return sb.toString();
    }

    private void appendList(StringBuilder sb, Object key, List list) {
        for(int i=0; i<list.size(); i++) {
            append(sb, key, list.get(i));

            if (i + 1 < list.size()) {
                sb.append(", ");
            }
        }
    }

    private void append(StringBuilder sb, Object key, Object value) {
        if(value instanceof Object[]) {
            appendList(sb, key, Arrays.asList((Object[]) value));
        } else if (value instanceof List) {
            appendList(sb, key, (List) value);
        } else {
            sb.append(key);
            sb.append('=');
            if(!isPrimitive(value)) {
                sb.append('"');
                sb.append(value);
                sb.append('"');
            } else {
                sb.append(value);
            }
        }
    }

    private boolean isPrimitive(Object o) {
        return o==null || JAVA_WRAPPER_TYPES.contains(o.getClass());
    }
}
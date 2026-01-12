package edu.yu.dbimpl.config;

/** A singleton API to get/set configuration state that is global to a PetiteDB
 * instance.
 *
 * Students MAY NOT modify this class in any way.
 * 
 * Design note: implementation is based on Joshua Bloch's "singleton as enum"
 * design pattern.  (@see
 * https://java-design-patterns.com/patterns/singleton/).  To deal with
 * multi-threading issues (raised by implementation approach, see usage note
 * below), I've made all getters/setters synchronized: I don't foresee
 * performance issues (famous last words).
 *
 * Important usage note: configuration state is supplied by a Properties
 * object.  Since I don't see how to elegantly have a default Properties
 * instance available on JVM startup, clients MUST supply the necessary state
 * via DBConfiguration.get().setConfiguration() on system startup.
 *
 * Important usage note: PetiteDB clients are responsible for setting
 * DBConfiguration state before instantiating ANY "Manager" instance.
 * 
 * @author Avraham Leff
 */

import java.util.Properties;

public enum DBConfiguration {

    INSTANCE(new Properties());

    /**
     * Constructor: loads state from Properties parameter, will throw IAE if
     * subsequent getters depend on state that is missing or malformed from this
     * parameter.
     *
     * @param properties contains state that can be accessed from getter APIs.
     */
    private DBConfiguration(final Properties properties) {
        setConfiguration(properties);
    }

    /**
     * Returns the singleton instance.
     */
    public DBConfiguration get() {
        return INSTANCE;
    }

    /**
     * Supplies the configuration state that will be used going forward.
     */
    public synchronized void setConfiguration(final Properties properties) {
        if (properties == null) {
            throw new IllegalArgumentException("Null properties parameter");
        }

        this.properties = properties;
    }

    /**
     * Returns true iff "database created for the first time" state has been set to
     * true, false otherwise.
     *
     * @throws IllegalStateException if client hasn't configured state one way or
     *                               the other
     */
    public synchronized boolean isDBStartup() {
        final String value = assertPropertyExists(DB_STARTUP);
        // I don't like Boolean.parseBoolean semantics
        if (value.equals("true")) {
            return true;
        } else if (value.equals("false")) {
            return false;
        } else {
            throw new IllegalArgumentException("Value can't be converted to boolean: " + value);
        }
    }

    /**
     * Returns a positive integer representing the number of buckets used in the
     * DBMS's static hash index implementation. By default returns 100.
     *
     */
    public synchronized int nStaticHashBuckets() {
        final String value = properties.getProperty(N_STATIC_HASH_BUCKETS);
        if (null == value) {
            return 100;
        }

        int retval = -1;
        try {
            retval = Integer.valueOf(value);
        } catch (Exception e) {
            throw new IllegalArgumentException("Value can't be converted to integer: " + value);
        }

        if (retval <= 1) {
            throw new IllegalArgumentException("Value can't be converted to positive integer: " + value);
        }

        return retval;
    }

    private String assertPropertyExists(final String property) {
        final String value = properties.getProperty(property);
        if (value == null) {
            throw new IllegalStateException("Property is undefined: " + property);
        }

        return value;
    }

    public final static String DB_STARTUP = "db.startup";
    public final static String N_STATIC_HASH_BUCKETS = "n.static.hash.buckets";
    private Properties properties;
} // class

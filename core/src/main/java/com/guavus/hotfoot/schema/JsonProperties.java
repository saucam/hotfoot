package com.guavus.hotfoot.schema;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.io.IOException;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.node.TextNode;

/** Base class for objects that have Json-valued properties. */
public abstract class JsonProperties {
    Map<String,JsonNode> props = new LinkedHashMap<String,JsonNode>(1);

    private Set<String> reserved;

    JsonProperties(Set<String> reserved) {
        this.reserved = reserved;
    }

    /**
     * Returns the value of the named, string-valued property in this schema.
     * Returns <tt>null</tt> if there is no string-valued property with that name.
     */
    public String getProp(String name) {
        JsonNode value = getJsonProp(name);
        return value != null && value.isTextual() ? value.getTextValue() : null;
    }

    /**
     * Returns the value of the named property in this schema.
     * Returns <tt>null</tt> if there is no property with that name.
     */
    public synchronized JsonNode getJsonProp(String name) {
        return props.get(name);
    }

    /**
     * Adds a property with the given name <tt>name</tt> and
     * value <tt>value</tt>. Neither <tt>name</tt> nor <tt>value</tt> can be
     * <tt>null</tt>. It is illegal to add a property if another with
     * the same name but different value already exists in this schema.
     *
     * @param name The name of the property to add
     * @param value The value for the property to add
     */
    public void addProp(String name, String value) {
        addProp(name, TextNode.valueOf(value));
    }

    /**
     * Adds a property with the given name <tt>name</tt> and
     * value <tt>value</tt>. Neither <tt>name</tt> nor <tt>value</tt> can be
     * <tt>null</tt>. It is illegal to add a property if another with
     * the same name but different value already exists in this schema.
     *
     * @param name The name of the property to add
     * @param value The value for the property to add
     */
    public synchronized void addProp(String name, JsonNode value) {
        if (reserved.contains(name))
            throw new SchemaParseException("Can't set reserved property: " + name);

        if (value == null)
            throw new SchemaParseException("Can't set a property to null: " + name);

        JsonNode old = props.get(name);
        if (old == null)
            props.put(name, value);
        else if (!old.equals(value))
            throw new SchemaParseException("Can't overwrite property: " + name);
    }

    /** Return the defined properties that have string values. */
    @Deprecated public Map<String,String> getProps() {
        Map<String,String> result = new LinkedHashMap<String,String>();
        for (Map.Entry<String,JsonNode> e : props.entrySet())
            if (e.getValue().isTextual())
                result.put(e.getKey(), e.getValue().getTextValue());
        return result;
    }

    /** Convert a map of string-valued properties to Json properties. */
    Map<String,JsonNode> jsonProps(Map<String,String> stringProps) {
        Map<String,JsonNode> result = new LinkedHashMap<String,JsonNode>();
        for (Map.Entry<String,String> e : stringProps.entrySet())
            result.put(e.getKey(), TextNode.valueOf(e.getValue()));
        return result;
    }

    /** Return the defined properties as an unmodifieable Map. */
    public Map<String,JsonNode> getJsonProps() {
        return Collections.unmodifiableMap(props);
    }

    void writeProps(JsonGenerator gen) throws IOException {
        for (Map.Entry<String,JsonNode> e : props.entrySet())
            gen.writeObjectField(e.getKey(), e.getValue());
    }

}

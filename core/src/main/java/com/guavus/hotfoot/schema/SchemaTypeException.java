package com.guavus.hotfoot.schema;

/** Thrown when an illegal type is used. */
public class SchemaTypeException extends SchemaParseException {
    public SchemaTypeException(String message) { super(message); }
    public SchemaTypeException(String message, Throwable cause) {
        super(message, cause);
    }
}

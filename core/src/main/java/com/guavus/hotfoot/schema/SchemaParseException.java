package com.guavus.hotfoot.schema;

public class SchemaParseException extends RuntimeException {
    public SchemaParseException(Throwable cause) { super(cause); }
    public SchemaParseException(String message) { super(message); }
    public SchemaParseException(String message, Throwable cause) {
        super(message, cause);
    }
}

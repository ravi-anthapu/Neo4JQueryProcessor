package com.neo4j.query;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

public class CustomInputStream extends FilterInputStream {
    public CustomInputStream(InputStream in) {
        super(in);
    }

    public void close() {
        // Does nothing and ignores closing the wrapped stream
    }
}

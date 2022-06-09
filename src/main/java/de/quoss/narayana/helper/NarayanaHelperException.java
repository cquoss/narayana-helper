package de.quoss.narayana.helper;

public class NarayanaHelperException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public NarayanaHelperException(final String s) {
        super(s);
    }

    public NarayanaHelperException(final Throwable t) {
        super(t);
    }

    public NarayanaHelperException(final String s, final Throwable t) {
        super(s, t);
    }

}

package jugistanbul.difc;

public final class DifcOps {

    private DifcOps() {
    }

    public static void requireOk(final int errorCode, final String operation, final String detail) {
        if (errorCode != 0) {
            throw new IllegalStateException(
                    operation + " failed with errorCode=" + errorCode
                            + (detail == null || detail.isEmpty() ? "" : ": " + detail));
        }
    }
}

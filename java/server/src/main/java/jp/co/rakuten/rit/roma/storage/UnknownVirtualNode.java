package jp.co.rakuten.rit.roma.storage;

import java.io.File;

public class UnknownVirtualNode extends StorageException {

    private static final long serialVersionUID = -7600640268899170679L;

    public UnknownVirtualNode(long virtualNodeId, File storagePath) {
        super("Unknown virtual node id("
                + virtualNodeId
                + ")"
                + (storagePath != null ? " in " + storagePath.getAbsolutePath()
                        : ""));
    }
}

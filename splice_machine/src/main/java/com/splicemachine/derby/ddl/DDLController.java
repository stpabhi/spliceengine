package com.splicemachine.derby.ddl;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.ddl.DDLMessage;

public interface DDLController {

    /**
     * Notify remote nodes that a DDL change is in progress (but not yet committed).
     */
    String notifyMetadataChange(DDLMessage.DDLChange change) throws StandardException;

    /**
     * Notify remote nodes that the DDL change has been committed.
     */
    void finishMetadataChange(String changeId) throws StandardException;

}

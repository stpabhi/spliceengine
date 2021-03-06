/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2018 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.dbTesting.functionTests.tests.store;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.splicemachine.dbTesting.functionTests.util.PrivilegedFileOpsForTests;
import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.JDBC;
import com.splicemachine.dbTesting.junit.TestConfiguration;


public class OfflineBackupTest extends BaseJDBCTestCase {


    public OfflineBackupTest(String name) {
        super(name);
    }

    public void testCreateFromRestoreFrom() throws SQLException, IOException {
        getConnection();
        TestConfiguration.getCurrent().shutdownDatabase();
        File origdbloc = new File("system","wombat");
        File backupdbloc = new File("system","wombatbackup");
        PrivilegedFileOpsForTests.copy(origdbloc, backupdbloc);
        Connection connCreateFrom = DriverManager.getConnection(
                "jdbc:splice:wombatCreateFrom;createFrom=system/wombatbackup");
        checkAllConsistency(connCreateFrom);
        try {
            DriverManager.getConnection("jdbc:splice:wombatCreateFrom;shutdown=true");
        } catch (SQLException se) {
            assertSQLState("Database shutdown", "08006", se);
        }
        Connection connRestoreFrom = DriverManager.getConnection(
                "jdbc:splice:wombatRestoreFrom;restoreFrom=system/wombatbackup");
        checkAllConsistency(connRestoreFrom);
        try {
            DriverManager.getConnection("jdbc:splice:wombatRestoreFrom;shutdown=true");
        } catch (SQLException se) {
            assertSQLState("Database shutdown", "08006", se);
        }

        removeDirectory(backupdbloc);
        removeDirectory(new File("system","wombatCreateFrom"));
        removeDirectory(new File("system","wombatRestoreFrom"));

    }



    public static Test suite() {

        if (JDBC.vmSupportsJSR169())
            return new TestSuite("Empty OfflineBackupTest (uses DriverManager)");
        return TestConfiguration.embeddedSuite(OfflineBackupTest.class);
    }


}

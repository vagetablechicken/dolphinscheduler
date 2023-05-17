/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.plugin.task.openmldb;

import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.model.Property;
import org.apache.dolphinscheduler.plugin.task.api.parameters.AbstractParameters;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class OpenmldbTaskTest {

    static class MockOpenmldbTask extends OpenmldbTask {

        /**
         * constructor
         *
         * @param taskRequest taskRequest
         */
        public MockOpenmldbTask(TaskExecutionContext taskRequest) {
            super(taskRequest);
        }

        @Override
        protected Map<String, Property> mergeParamsWithContext(AbstractParameters parameters) {
            return new HashMap<>();
        }
    }

    private OpenmldbTask createOpenmldbTask() {
        return new MockOpenmldbTask(null);
    }

    @Test
    public void buildPythonExecuteCommand() throws Exception {
        OpenmldbTask openmldbTask = createOpenmldbTask();
        String pythonFile = "test.py";
        String result1 = openmldbTask.buildPythonExecuteCommand(pythonFile);
        Assertions.assertEquals("python3 test.py", result1);
    }

    @Test
    public void buildSQLWithComment() throws Exception {
        TaskExecutionContext taskExecutionContext = Mockito.mock(TaskExecutionContext.class);
        OpenmldbParameters openmldbParameters = new OpenmldbParameters();
        openmldbParameters.setExecuteMode("offline");
        openmldbParameters.setZk("localhost:2181");
        openmldbParameters.setZkPath("/openmldb");
        String rawSQLScript = "select * from users\r\n"
                + "-- some comment\n"
                + "inner join order on users.order_id = order.id; \n\n;"
                + "select * from users;";
        openmldbParameters.setSql(rawSQLScript);
        Mockito.when(taskExecutionContext.getTaskParams()).thenReturn(JSONUtils.toJsonString(openmldbParameters));
        OpenmldbTask openmldbTask = new OpenmldbTask(taskExecutionContext);
        openmldbTask.init();
        OpenmldbParameters internal = (OpenmldbParameters) openmldbTask.getParameters();
        Assertions.assertNotNull(internal);
        Assertions.assertEquals(internal.getExecuteMode(), "offline");

        String result1 = openmldbTask.buildPythonScriptContent();
        Assertions.assertEquals("import openmldb\n"
                + "import sqlalchemy as db\n"
                + "engine = db.create_engine('openmldb:///?zk=localhost:2181&zkPath=/openmldb')\n"
                + "con = engine.connect()\n"
                + "\n\n"
                + "def exec(sql):\n"
                + "    cr = con.execute(sql)\n"
                + "    rs = cr.fetchall() if cr.rowcount > 0 else None; assert not rs or (len(rs)==1 and rs[0][2].lower()==\"finished\"), f\"job info {rs}\"\n"
                + "\n\n"
                + "exec(\"set @@execute_mode='offline'\")\n"
                + "exec(\"set @@sync_job=true\")\n"
                + "exec(\"set @@job_timeout=1800000\")\n"
                + "exec(\"select * from users\\n-- some comment\\ninner join order on users.order_id = order.id\")\n"
                + "exec(\"select * from users\")\n", result1);
    }

    @Test
    public void buildSQLDDL() throws Exception {
        TaskExecutionContext taskExecutionContext = Mockito.mock(TaskExecutionContext.class);
        OpenmldbParameters openmldbParameters = new OpenmldbParameters();
        openmldbParameters.setExecuteMode("offline");
        openmldbParameters.setZk("localhost:2181");
        openmldbParameters.setZkPath("/openmldb");
        String rawSQLScript =
                "CREATE DATABASE IF NOT EXISTS demo_db;\nUSE demo_db;\nCREATE TABLE IF NOT EXISTS talkingdata(\n    ip int,\n    app int,\n    device int,\n    os int,\n    channel int,\n    click_time timestamp,\n    is_attributed int);\n";
        openmldbParameters.setSql(rawSQLScript);
        Mockito.when(taskExecutionContext.getTaskParams()).thenReturn(JSONUtils.toJsonString(openmldbParameters));
        OpenmldbTask openmldbTask = new OpenmldbTask(taskExecutionContext);
        openmldbTask.init();
        OpenmldbParameters internal = (OpenmldbParameters) openmldbTask.getParameters();
        Assertions.assertNotNull(internal);
        Assertions.assertEquals(internal.getExecuteMode(), "offline");

        String result1 = openmldbTask.buildPythonScriptContent();
        Assertions.assertEquals("import openmldb\n"
                + "import sqlalchemy as db\n"
                + "engine = db.create_engine('openmldb:///?zk=localhost:2181&zkPath=/openmldb')\n"
                + "con = engine.connect()\n"
                + "\n\n"
                + "def exec(sql):\n"
                + "    cr = con.execute(sql)\n"
                + "    rs = cr.fetchall() if cr.rowcount > 0 else None; assert not rs or (len(rs)==1 and rs[0][2].lower()==\"finished\"), f\"job info {rs}\"\n"
                + "\n\n"
                + "exec(\"set @@execute_mode='offline'\")\n"
                + "exec(\"set @@sync_job=true\")\n"
                + "exec(\"set @@job_timeout=1800000\")\n"
                + "exec(\"CREATE DATABASE IF NOT EXISTS demo_db\")\n"
                + "exec(\"USE demo_db\")\n"
                + "exec(\"CREATE TABLE IF NOT EXISTS talkingdata(\\n    ip int,\\n    app int,\\n    device int,\\n    os int,\\n    channel int,\\n    click_time timestamp,\\n    is_attributed int)\")\n",
                result1);
    }

    @Test
    public void buildSQLOfflineLoad() throws Exception {
        TaskExecutionContext taskExecutionContext = Mockito.mock(TaskExecutionContext.class);
        OpenmldbParameters openmldbParameters = new OpenmldbParameters();
        openmldbParameters.setExecuteMode("offline");
        openmldbParameters.setZk("localhost:2181");
        openmldbParameters.setZkPath("/openmldb");
        String rawSQLScript =
                "USE demo_db;\nset @@job_timeout=600000;\nLOAD DATA INFILE 'file:///tmp/train_sample.csv' \nINTO TABLE talkingdata OPTIONS(mode='overwrite',\n deep_copy=false);\n";
        openmldbParameters.setSql(rawSQLScript);
        Mockito.when(taskExecutionContext.getTaskParams()).thenReturn(JSONUtils.toJsonString(openmldbParameters));
        OpenmldbTask openmldbTask = new OpenmldbTask(taskExecutionContext);
        openmldbTask.init();
        OpenmldbParameters internal = (OpenmldbParameters) openmldbTask.getParameters();
        Assertions.assertNotNull(internal);
        Assertions.assertEquals(internal.getExecuteMode(), "offline");

        String result1 = openmldbTask.buildPythonScriptContent();
        Assertions.assertEquals("import openmldb\n"
                + "import sqlalchemy as db\n"
                + "engine = db.create_engine('openmldb:///?zk=localhost:2181&zkPath=/openmldb')\n"
                + "con = engine.connect()\n"
                + "\n\n"
                + "def exec(sql):\n"
                + "    cr = con.execute(sql)\n"
                + "    rs = cr.fetchall() if cr.rowcount > 0 else None; assert not rs or (len(rs)==1 and rs[0][2].lower()==\"finished\"), f\"job info {rs}\"\n"
                + "\n\n"
                + "exec(\"set @@execute_mode='offline'\")\n"
                + "exec(\"set @@sync_job=true\")\n"
                + "exec(\"set @@job_timeout=1800000\")\n"
                + "exec(\"USE demo_db\")\n"
                + "exec(\"set @@job_timeout=600000\")\n"
                + "exec(\"LOAD DATA INFILE 'file:///tmp/train_sample.csv' \\nINTO TABLE talkingdata OPTIONS(mode='overwrite',\\n deep_copy=false)\")\n",
                result1);
    }

    @Test
    public void buildSQLDeploy() throws Exception {
        TaskExecutionContext taskExecutionContext = Mockito.mock(TaskExecutionContext.class);
        OpenmldbParameters openmldbParameters = new OpenmldbParameters();
        openmldbParameters.setExecuteMode("online");
        openmldbParameters.setZk("localhost:2181");
        openmldbParameters.setZkPath("/openmldb");
        String rawSQLScript =
                "USE demo_db;\nDEPLOY demo select is_attributed, app, device, os, channel, hour(click_time) as hour, day(click_time) as day,\n count(channel) over w1 as qty,\n count(channel) over w2 as ip_app_count,\n count(channel) over w3 as ip_app_os_count\n from talkingdata\n window\n w1 as (partition by ip order by click_time ROWS_RANGE BETWEEN 1h PRECEDING AND CURRENT ROW),\n w2 as(partition by ip, app order by click_time ROWS_RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),\n w3 as(partition by ip, app, os order by click_time ROWS_RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)\n ;";
        openmldbParameters.setSql(rawSQLScript);
        Mockito.when(taskExecutionContext.getTaskParams()).thenReturn(JSONUtils.toJsonString(openmldbParameters));
        OpenmldbTask openmldbTask = new OpenmldbTask(taskExecutionContext);
        openmldbTask.init();
        OpenmldbParameters internal = (OpenmldbParameters) openmldbTask.getParameters();
        Assertions.assertNotNull(internal);
        Assertions.assertEquals(internal.getExecuteMode(), "online");

        String result1 = openmldbTask.buildPythonScriptContent();
        Assertions.assertEquals("import openmldb\n"
                + "import sqlalchemy as db\n"
                + "engine = db.create_engine('openmldb:///?zk=localhost:2181&zkPath=/openmldb')\n"
                + "con = engine.connect()\n"
                + "\n\n"
                + "def exec(sql):\n"
                + "    cr = con.execute(sql)\n"
                + "    rs = cr.fetchall() if cr.rowcount > 0 else None; assert not rs or (len(rs)==1 and rs[0][2].lower()==\"finished\"), f\"job info {rs}\"\n"
                + "\n\n"
                + "exec(\"set @@execute_mode='online'\")\n"
                + "exec(\"set @@sync_job=true\")\n"
                + "exec(\"set @@job_timeout=1800000\")\n"
                + "exec(\"USE demo_db\")\n"
                + "exec(\"DEPLOY demo select is_attributed, app, device, os, channel, hour(click_time) as hour, day(click_time) as day,\\n count(channel) over w1 as qty,\\n count(channel) over w2 as ip_app_count,\\n count(channel) over w3 as ip_app_os_count\\n from talkingdata\\n window\\n w1 as (partition by ip order by click_time ROWS_RANGE BETWEEN 1h PRECEDING AND CURRENT ROW),\\n w2 as(partition by ip, app order by click_time ROWS_RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),\\n w3 as(partition by ip, app, os order by click_time ROWS_RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)\")\n",
                result1);
    }

    @Test
    public void buildSQLOnlineLoad() throws Exception {
        TaskExecutionContext taskExecutionContext = Mockito.mock(TaskExecutionContext.class);
        OpenmldbParameters openmldbParameters = new OpenmldbParameters();
        openmldbParameters.setExecuteMode("online");
        openmldbParameters.setZk("localhost:2181");
        openmldbParameters.setZkPath("/openmldb");
        String rawSQLScript =
                "USE demo_db;\nLOAD DATA INFILE 'file:///tmp/train_sample.csv' \nINTO TABLE talkingdata OPTIONS(mode='append');";
        openmldbParameters.setSql(rawSQLScript);
        Mockito.when(taskExecutionContext.getTaskParams()).thenReturn(JSONUtils.toJsonString(openmldbParameters));
        OpenmldbTask openmldbTask = new OpenmldbTask(taskExecutionContext);
        openmldbTask.init();
        OpenmldbParameters internal = (OpenmldbParameters) openmldbTask.getParameters();
        Assertions.assertNotNull(internal);
        Assertions.assertEquals(internal.getExecuteMode(), "online");

        String result1 = openmldbTask.buildPythonScriptContent();
        Assertions.assertEquals("import openmldb\n"
                + "import sqlalchemy as db\n"
                + "engine = db.create_engine('openmldb:///?zk=localhost:2181&zkPath=/openmldb')\n"
                + "con = engine.connect()\n"
                + "\n\n"
                + "def exec(sql):\n"
                + "    cr = con.execute(sql)\n"
                + "    rs = cr.fetchall() if cr.rowcount > 0 else None; assert not rs or (len(rs)==1 and rs[0][2].lower()==\"finished\"), f\"job info {rs}\"\n"
                + "\n\n"
                + "exec(\"set @@execute_mode='online'\")\n"
                + "exec(\"set @@sync_job=true\")\n"
                + "exec(\"set @@job_timeout=1800000\")\n"
                + "exec(\"USE demo_db\")\n"
                + "exec(\"LOAD DATA INFILE 'file:///tmp/train_sample.csv' \\nINTO TABLE talkingdata OPTIONS(mode='append')\")\n",
                result1);
    }
}

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

package org.apache.dolphinscheduler.plugin.task.python;

import org.junit.Assert;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

public class OpenmldbTaskTest {

    @Test
    public void buildPythonExecuteCommand() throws Exception {
        OpenmldbTask pythonTask = createOpenmldbTask();
        String methodName = "buildPythonExecuteCommand";
        String pythonFile = "test.py";
        String result1 = Whitebox.invokeMethod(pythonTask, methodName, pythonFile);
        Assert.assertEquals("${PYTHON_HOME} test.py", result1);
    }

    private OpenmldbTask createOpenmldbTask() {
        return new OpenmldbTask(null);
    }
}
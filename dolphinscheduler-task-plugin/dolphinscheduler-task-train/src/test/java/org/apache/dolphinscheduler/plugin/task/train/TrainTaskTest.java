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

package org.apache.dolphinscheduler.plugin.task.train;

import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.model.Property;
import org.apache.dolphinscheduler.plugin.task.api.parameters.AbstractParameters;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

public class TrainTaskTest {
    static class MockTrainTask extends TrainTask {
        /**
         * constructor
         *
         * @param taskRequest taskRequest
         */
        public MockTrainTask(TaskExecutionContext taskRequest) {
            super(taskRequest);
        }

        @Override
        protected Map<String, Property> mergeParamsWithContext(AbstractParameters parameters) {
            return new HashMap<>();
        }
    }

    private TrainTask createOpenmldbTask() {
        return new MockTrainTask(null);
    }

    @Test
    public void buildPythonExecuteCommand() throws Exception {
        TrainTask trainTask = createOpenmldbTask();
        String pythonFile = "test.py";
        String result1 = trainTask.buildPythonExecuteCommand(pythonFile);
        Assert.assertEquals("python3 test.py", result1);
    }

    @Test
    public void buildSQLWithComment() throws Exception {
    }

}
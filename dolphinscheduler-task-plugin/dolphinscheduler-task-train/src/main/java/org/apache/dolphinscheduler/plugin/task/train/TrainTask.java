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

import org.apache.dolphinscheduler.plugin.task.api.TaskException;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.parameters.AbstractParameters;
import org.apache.dolphinscheduler.plugin.task.python.PythonTask;
import org.apache.dolphinscheduler.spi.utils.JSONUtils;
import org.apache.dolphinscheduler.spi.utils.StringUtils;

import java.io.File;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;

/**
 * openmldb task
 */
public class TrainTask extends PythonTask {

    /**
     * train parameters
     */
    private TrainParameters trainParameters;

    /**
     * python process(prefer python3)
     */
    private static final String TRAIN_PYTHON = "python3";
    private static final Pattern PYTHON_PATH_PATTERN = Pattern.compile("/bin/python[\\d.]*$");

    /**
     * constructor
     *
     * @param taskRequest taskRequest
     */
    public TrainTask(TaskExecutionContext taskRequest) {
        super(taskRequest);
    }

    @Override
    public void init() {
        logger.info("train task params {}", taskRequest.getTaskParams());

        trainParameters = JSONUtils.parseObject(taskRequest.getTaskParams(), TrainParameters.class);

        if (trainParameters == null || !trainParameters.checkParameters()) {
            throw new TaskException("train task params is not valid");
        }
    }

    @Override
    @Deprecated
    public String getPreScript() {
        return "";
    }

    @Override
    public AbstractParameters getParameters() {
        return trainParameters;
    }

    /**
     * build python command file path
     *
     * @return python command file path
     */
    @Override
    protected String buildPythonCommandFilePath() {
        return String.format("%s/train_%s.py", taskRequest.getExecutePath(), taskRequest.getTaskAppId());
    }

    /**
     * build python script content from sql
     *
     * @return raw python script
     */
    @Override
    protected String buildPythonScriptContent() {
        logger.info("raw train parameters : {}", trainParameters);
        // convert sql to python script
        String pythonScriptContent = buildPythonScriptContent(trainParameters);
        logger.info("rendered python script content: {}", pythonScriptContent);
        return pythonScriptContent;
    }

    private String buildPythonScriptContent(TrainParameters trainParameters) {
        // imports
        StringBuilder builder = new StringBuilder("import glob\n" +
                "import os\n" +
                "import pandas as pd\n" +
                "import xgboost as xgb\n" +
                "from xgboost.sklearn import XGBClassifier\n" +
                "from sklearn.model_selection import train_test_split\n" +
                "from sklearn.metrics import classification_report\n" +
                "from sklearn.metrics import accuracy_score\n\n");

        // read dataset
        builder.append(readDataScript(trainParameters.getTrainDataPath()));
        // modify columns
        builder.append(prepareDataAndLabel(trainParameters.getLabelColumn(), trainParameters.getDropColumns()));
        // split train and test data
        builder.append(splitTrainTest(trainParameters.getExtraParams()));

        String objective = trainParameters.getObjective();
        if (objective.equalsIgnoreCase("binary:logistic")) {
            builder.append("train_model = XGBClassifier().fit(X_train, y_train)\n" +
                    "pred = train_model.predict(X_test)\n" +
                    "print(\"Classification report:\\n\", classification_report(y_test, pred))\n" +
                    "print(\"Accuracy: %.2f\" % (accuracy_score(y_test, pred) * 100))\n");
        } else {
            throw new TaskException("objective " + objective + "unsupported");
        }
        String modelSavePath = trainParameters.getModelSavePath();
        if (modelSavePath == null || !modelSavePath.startsWith("/")) {
            throw new TaskException("remote save path is unsupported");
        }
        builder.append(String.format("print('Save model.json to ', '%s')\n" +
                "train_model.save_model('%s')\n", modelSavePath, modelSavePath));
        return builder.toString();
    }

    private String readDataScript(String trainDataPath) {
        StringBuilder builder = new StringBuilder("train_df = ");
        if (trainDataPath.startsWith("/")) {
            // local path, if contains *, multi files
            if (trainDataPath.contains("*")) {
                builder.append("pd.concat(map(pd.read_csv, glob.glob(os.path.join('','").
                        append(trainDataPath).append("'))))\n");
            } else {
                File file = new File(trainDataPath);
                Preconditions.checkState(file.isFile() && file.exists(), "file path is invalid");
                builder.append("pd.read_csv('").append(trainDataPath).append("')\n");
            }
        } else {
            throw new TaskException("remote path or relative path unsupported");
        }
        return builder.toString();
    }

    private String prepareDataAndLabel(String labelColumn, String dropColumns) {
        StringBuilder builder = new StringBuilder();
        builder.append("X_data = train_df.drop('").append(labelColumn).append("', axis=1)\n");
        if (dropColumns != null && !dropColumns.isEmpty()) {
            // col1, col2, ...
            for (String col : dropColumns.split(",")) {
                col = col.trim();
                builder.append("X_data = X_data.drop('").append(col).append("', axis=1)\n");
            }
        }
        builder.append("y = train_df.").append(labelColumn).append("\n");
        return builder.toString();
    }

    private String splitTrainTest(String extraParams) {
        int seed = 7;
        double testSize = 0.25;
        if (extraParams != null && !extraParams.isEmpty()) {
            // TODO(hw): extract params from ...
        }
        return String.format("X_train, X_test, y_train, y_test = train_test_split(X_data, y, test_size=%s, " +
                "random_state=%s)\n", testSize, seed);
    }

    /**
     * Build the python task command.
     * If user have set the 'PYTHON_HOME' environment, we will use the 'PYTHON_HOME',
     * if not, we will default use python.
     *
     * @param pythonFile Python file, cannot be empty.
     * @return Python execute command, e.g. 'python test.py'.
     */
    @Override
    protected String buildPythonExecuteCommand(String pythonFile) {
        Preconditions.checkNotNull(pythonFile, "Python file cannot be null");
        return getPythonCommand() + " " + pythonFile;
    }

    private String getPythonCommand() {
        String pythonHome = System.getenv(PYTHON_HOME);
        return getPythonCommand(pythonHome);
    }

    private String getPythonCommand(String pythonHome) {
        if (StringUtils.isEmpty(pythonHome)) {
            return TRAIN_PYTHON;
        }
        // If your python home is "xx/bin/python[xx]", you are forced to use python3
        String pythonBinPath = "/bin/" + TRAIN_PYTHON;
        Matcher matcher = PYTHON_PATH_PATTERN.matcher(pythonHome);
        if (matcher.find()) {
            return matcher.replaceAll(pythonBinPath);
        }
        return Paths.get(pythonHome, pythonBinPath).toString();
    }
}

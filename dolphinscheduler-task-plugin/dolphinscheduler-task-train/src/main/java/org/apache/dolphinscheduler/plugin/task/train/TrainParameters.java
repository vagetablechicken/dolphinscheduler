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

import org.apache.dolphinscheduler.plugin.task.api.model.ResourceInfo;
import org.apache.dolphinscheduler.plugin.task.api.parameters.AbstractParameters;
import org.apache.dolphinscheduler.spi.utils.StringUtils;

import java.util.List;

public class TrainParameters extends AbstractParameters {

    private String trainDataPath;
    private String objective;
    private String labelColumn;
    private String dropColumns; // col1,col2,...
    private String extraParams; // TODO(hw): seed=7,testSize=0.25

    private String modelSavePath; // TODO(hw): save to remote, local path
    /**
     * resource list
     */
    private List<ResourceInfo> resourceList;

    public List<ResourceInfo> getResourceList() {
        return resourceList;
    }

    public void setResourceList(List<ResourceInfo> resourceList) {
        this.resourceList = resourceList;
    }

    @Override
    public boolean checkParameters() {
        return StringUtils.isNotEmpty(labelColumn); // TODO(hw):
    }

    @Override
    public List<ResourceInfo> getResourceFilesList() {
        return this.resourceList;
    }

    public String getObjective() {
        return objective;
    }

    public void setObjective(String objective) {
        this.objective = objective;
    }

    public String getLabelColumn() {
        return labelColumn;
    }

    public void setLabelColumn(String labelColumn) {
        this.labelColumn = labelColumn;
    }

    public String getDropColumns() {
        return dropColumns;
    }

    public void setDropColumns(String dropColumns) {
        this.dropColumns = dropColumns;
    }

    public String getExtraParams() {
        return extraParams;
    }

    public void setExtraParams(String extraParams) {
        this.extraParams = extraParams;
    }

    public String getTrainDataPath() {
        return trainDataPath;
    }

    public void setTrainDataPath(String trainDataPath) {
        this.trainDataPath = trainDataPath;
    }

    public String getModelSavePath() {
        return modelSavePath;
    }

    public void setModelSavePath(String modelSavePath) {
        this.modelSavePath = modelSavePath;
    }
}

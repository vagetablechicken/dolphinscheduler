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
import { useI18n } from 'vue-i18n'
import { useCustomParams, useResources } from '.'
import type { IJsonItem } from '../types'

export function useTrain(model: { [field: string]: any }): IJsonItem[] {
  const { t } = useI18n()

  return [
    {
      type: 'input',
      field: 'trainDataPath',
      name: t('project.node.train_data_path'),
      props: {
        placeholder: t('project.node.train_data_path_tips')
      },
      validate: {
        trigger: ['input', 'trigger'],
        required: true,
        message: t('project.node.train_data_path_tips')
      }
    },
    {
      type: 'select',
      field: 'trainAlgo',
      span: 12,
      name: t('project.node.train_algo'),
      options: ALGO_TYPES,
      value: 'XGBClassifier'
    },
    {
      type: 'input',
      field: 'trainObjective',
      span: 12,
      name: t('project.node.train_objective'),
      props: {
        placeholder: t('project.node.train_objective_tips')
      }
    },
    {
      type: 'input',
      field: 'labelColumn',
      name: t('project.node.train_label_column'),
      props: {
        placeholder: t('project.node.train_label_column_tips')
      }
    },
    {
      type: 'input',
      field: 'modelSavePath',
      name: t('project.node.train_model_save_path'),
      props: {
        placeholder: t('project.node.train_model_save_path_tips')
      },
      validate: {
        trigger: ['input', 'trigger'],
        required: true,
        message: t('project.node.train_model_save_path_tips')
      }
    },
    {
      type: 'input',
      field: 'extraParams',
      name: t('project.node.train_extra_params'),
      props: {
        placeholder: t('project.node.train_extra_params_tips')
      }
    },
    useResources(),
    ...useCustomParams({ model, field: 'localParams', isSimple: false })
  ]
}

// TODO(hw): label: 'LGBMClassifier' -> lightgbm.LGBMClassifier
export const ALGO_TYPES = [
  {
    label: 'XGBClassifier',
    value: 'XGBClassifier'
  },
  {
    label: 'LogisticRegression',
    value: 'LogisticRegression'
  }
]

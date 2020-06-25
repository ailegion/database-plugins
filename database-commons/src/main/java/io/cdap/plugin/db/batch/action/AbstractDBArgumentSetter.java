/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.db.batch.action;

import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;
import io.cdap.cdap.etl.api.action.SettableArguments;
import io.cdap.plugin.util.DBUtils;
import io.cdap.plugin.util.DriverCleanup;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Action that converts db column into pipeline argument.
 */
public class AbstractDBArgumentSetter extends Action {

  private static final String JDBC_PLUGIN_ID = "driver";
  private final ArgumentSetterConfig config;

  public AbstractDBArgumentSetter(ArgumentSetterConfig config) {
    this.config = config;
  }

  @Override
  public void run(ActionContext context) throws Exception {
    Class<? extends Driver> driverClass = context.loadPluginClass(JDBC_PLUGIN_ID);
    FailureCollector failureCollector = context.getFailureCollector();
    SettableArguments settableArguments = context.getArguments();
    DriverCleanup driverCleanup = null;
    try {
      driverCleanup = DBUtils.ensureJDBCDriverIsAvailable(driverClass, config.getConnectionString(),
          config.jdbcPluginName);
      Properties connectionProperties = new Properties();
      connectionProperties.putAll(config.getConnectionArguments());
      try (Connection connection = DriverManager
          .getConnection(config.getConnectionString(), connectionProperties)) {
        try (Statement statement = connection.createStatement()) {
          ResultSet resultSet = statement.executeQuery(config.getQuery());
          setArguments(resultSet, failureCollector, settableArguments);
        }
      }
    } finally {
      if (driverCleanup != null) {
        driverCleanup.destroy();
      }
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer)
      throws IllegalArgumentException {
    DBUtils.validateJDBCPluginPipeline(pipelineConfigurer, config, JDBC_PLUGIN_ID);
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    FailureCollector collector = stageConfigurer.getFailureCollector();
    config.validate(collector);
  }

  /**
   * Converts column from jdbc results set into pipeline arguments
   *
   * @param resultSet        - result set from db {@link ResultSet}
   * @param failureCollector - context failure collector @{link FailureCollector}
   * @param arguments        - context argument setter {@link SettableArguments}
   * @throws SQLException - raises {@link SQLException} when configuration is not valid
   */
  private void setArguments(ResultSet resultSet, FailureCollector failureCollector,
      SettableArguments arguments) throws SQLException {

    boolean hasRecord = resultSet.next();
    if (!hasRecord) {
      failureCollector.addFailure("Ne record found",
          "No data is returned for the argument selection conditions");
      failureCollector.getOrThrowException();
    }
    arguments.set(config.getArgumentsColumn(), resultSet.getString(config.getArgumentsColumn()));

    if (resultSet.next()) {
      failureCollector
          .addFailure("More than one record found",
              "The argument selection conditions return multiple rows");
    }

    failureCollector.getOrThrowException();
  }
}

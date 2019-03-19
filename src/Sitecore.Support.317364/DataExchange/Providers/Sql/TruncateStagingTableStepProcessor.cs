using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using Sitecore.DataExchange.Contexts;
using Sitecore.DataExchange.Extensions;
using Sitecore.DataExchange.Models;
using Sitecore.DataExchange.Plugins;
using Sitecore.DataExchange.Processors.PipelineSteps;
using Sitecore.DataExchange.Providers.Sql.Endpoints;
using Sitecore.Services.Core.Diagnostics;

namespace Sitecore.Support.DataExchange.Providers.Sql
{
  public class CleanupStagingStepProcessor : BasePipelineStepWithEndpointsProcessor
  {
    protected override void ProcessPipelineStep(PipelineStep pipelineStep, PipelineContext pipelineContext, ILogger logger)
    {
      EndpointSettings endpointSettings = pipelineStep.GetEndpointSettings();
      if (endpointSettings == null)
      {
        Log(logger.Error, pipelineContext, "Pipeline step processing will abort because the pipeline step is missing a plugin.", $"plugin: {typeof(EndpointSettings).FullName}");
      }
      else
      {
        Endpoint endpointFrom = endpointSettings.EndpointFrom;
        if (endpointFrom == null)
        {
          Log(logger.Error, pipelineContext, "Pipeline step processing will abort because the pipeline step is missing an endpoint to read from.", $"plugin: {typeof(EndpointSettings).FullName}", string.Format("property: {0}", "EndpointFrom"));
        }
        else if (!IsEndpointValid(endpointFrom, pipelineStep, pipelineContext, logger))
        {
          Log(logger.Error, pipelineContext, "Pipeline step processing will abort because the endpoint to read from is not valid.", $"endpoint: {endpointFrom.Name}");
        }
        else
        {
          TruncateData(endpointFrom, pipelineStep, pipelineContext, logger);
        }
      }
    }

    private void TruncateData(Endpoint endpoint, PipelineStep pipelineStep, PipelineContext pipelineContext,
      ILogger logger)
    {
      if (endpoint == null)
      {
        throw new ArgumentNullException("endpoint");
      }

      if (pipelineStep == null)
      {
        throw new ArgumentNullException("pipelineStep");
      }

      if (pipelineContext == null)
      {
        throw new ArgumentNullException("pipelineContext");
      }

      DatabaseConnectionEndpointSettings plugin = endpoint.GetPlugin<DatabaseConnectionEndpointSettings>();
      IDbConnection connection = GetConnection(plugin, pipelineContext, logger);
      if (connection == null)
      {
        Log(logger.Error, pipelineContext, "Cannot read data because no IDbConnection was resolved.",
          Array.Empty<string>());
        pipelineContext.CriticalError = true;
      }
      else
      {
        connection.ConnectionString = plugin.ConnectionString;
        IDbCommand command = GetCommand(plugin, pipelineContext, logger);
        if (connection == null)
        {
          Log(logger.Error, pipelineContext, "Cannot read data because no IDbCommand was resolved.", Array.Empty<string>());
          pipelineContext.CriticalError = true;
        }
        else
        {
          CleanupStagingDataSettings plugin2 = pipelineStep.GetPlugin<CleanupStagingDataSettings>();
          CleanupData(plugin, plugin2);
        }
      }
    }

    private static void CleanupData(DatabaseConnectionEndpointSettings endpointSettings,
      CleanupStagingDataSettings settings)
    {
      List<string> list = new List<string>();
      using (SqlConnection sqlConnection = new SqlConnection(endpointSettings.ConnectionString))
      {
        sqlConnection.Open();
        using (IDbCommand dbCommand = sqlConnection.CreateCommand())
        {
          dbCommand.CommandType = CommandType.StoredProcedure;
          dbCommand.CommandText = settings.StoredProcedureName;
          IDataParameter dataParameter = dbCommand.CreateParameter();
          dataParameter.ParameterName = "@MaxBatchesToKeep";
          dataParameter.Value = settings.MaxBatchesToKeep;
          dbCommand.Parameters.Add(dataParameter);
          dbCommand.ExecuteNonQuery();
        }
      }
    }


    protected virtual IDbConnection GetConnection(DatabaseConnectionEndpointSettings settings, PipelineContext pipelineContext, ILogger logger)
    {
      return Activator.CreateInstance(settings.ConnectionType, settings.ConnectionParameters) as IDbConnection;
    }
    protected virtual IDbCommand GetCommand(DatabaseConnectionEndpointSettings settings, PipelineContext pipelineContext, ILogger logger)
    {
      return Activator.CreateInstance(settings.CommandType, settings.CommandParameters) as IDbCommand;
    }
  }
}
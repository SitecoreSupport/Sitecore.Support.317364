using System;
using Sitecore.DataExchange;
using Sitecore.DataExchange.Converters.PipelineSteps;
using Sitecore.DataExchange.Models;
using Sitecore.DataExchange.Plugins;
using Sitecore.DataExchange.Providers.Sql.Endpoints;
using Sitecore.DataExchange.Repositories;
using Sitecore.Services.Core.Model;

namespace Sitecore.Support.DataExchange.Providers.Sql
{
  public class CleanupStagingStepConverter : BasePipelineStepConverter
  {
    public CleanupStagingStepConverter(IItemModelRepository repository) : base(repository)
    {
    }

    protected override void AddPlugins(ItemModel source, PipelineStep pipelineStep)
    {
      IPlugin plugin = null;
      plugin = GetEndpointSettings(source);
      if (plugin != null)
      {
        pipelineStep.AddPlugin(plugin);
      }

      plugin = GetDatabaseConnectionEndpointSettings(source, plugin);
      if (plugin != null)
      {
        pipelineStep.AddPlugin(plugin);
      }
      plugin = GetCleanupStagingDataSettings(source);
      if (plugin != null)
      {
        pipelineStep.AddPlugin(plugin);
      }
    }

    protected virtual DatabaseConnectionEndpointSettings GetDatabaseConnectionEndpointSettings(ItemModel source, IPlugin plugin)
    {
      string connectionStringValue = GetConnectionStringValue(source, "ConnectionStringName");
      Type connectionType = null;
      Type commandType = null;
      ItemModel referenceAsModel = GetReferenceAsModel(source, "DatabaseType");
      if (referenceAsModel != null)
      {
        connectionType = GetTypeFromTypeName(referenceAsModel, "ConnectionType");
        commandType = GetTypeFromTypeName(referenceAsModel, "CommandType");
      }
      DatabaseConnectionEndpointSettings newPlugin = new DatabaseConnectionEndpointSettings(connectionStringValue, connectionType, commandType);
      return newPlugin;
    }

    protected virtual CleanupStagingDataSettings GetCleanupStagingDataSettings(ItemModel source)
    {
      int maxBatchesToKeep = GetIntValue(source, "MaxBatchesToKeep");
      if (maxBatchesToKeep < 1)
        maxBatchesToKeep = 1;
      return new CleanupStagingDataSettings()
      {
        StoredProcedureName = GetStringValueFromReference(source, "StoredProcedure", "StoredProcedureName"),
        MaxBatchesToKeep = maxBatchesToKeep
      };
    }

    protected virtual EndpointSettings GetEndpointSettings(ItemModel source)
    {
      EndpointSettings endpointSettings = new EndpointSettings();
      Endpoint endpoint = ConvertReferenceToModel<Endpoint>(source, "EndpointFrom");
      if (endpoint != null)
      {
        endpointSettings.EndpointFrom = endpoint;
      }
      return endpointSettings;
    }
  }
}
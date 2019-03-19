using Sitecore.DataExchange;

namespace Sitecore.Support.DataExchange.Providers.Sql
{
  public class CleanupStagingDataSettings:IPlugin
  {
    public string StoredProcedureName
    {
      get;
      set;
    }
    public int MaxBatchesToKeep
    {
      get;
      set;
    }
  }
}
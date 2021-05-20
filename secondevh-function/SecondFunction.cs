using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;


namespace Company.Function
{
  public static class SecondFunction
  {
    [FunctionName("SecondFunction")]
    public static async Task Run([EventHubTrigger("%secondEventhub%", Connection = "secondEventHubConnectionString")] EventData[] events,
     ILogger log, ExecutionContext context)
    {
      var exceptions = new List<Exception>();

      foreach (EventData eventData in events)
      {
        try
        {
          var activityCurrent = Activity.Current;
          string messageBody = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);
          log.LogInformation($"Second function processed a message: {messageBody}");
        }
        catch (Exception e)
        {
          // We need to keep processing the rest of the batch - capture this exception and continue.
          // Also, consider capturing details of the message that failed processing so it can be processed again later.
          exceptions.Add(e);
        }
      }
      if (exceptions.Count > 1)
        throw new AggregateException(exceptions);

      if (exceptions.Count == 1)
        throw exceptions.Single();
    }
  }
}

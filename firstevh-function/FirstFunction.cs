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
  public static class FirstFunction
  {
    [FunctionName("FirstFunction")]
    public static async Task Run([EventHubTrigger("%firstEventhub%", Connection = "firstEventHubConnectionString")] EventData[] events,
    [EventHub("%secondEventhub%", Connection = "secondEventHubConnectionString")] IAsyncCollector<EventData> secondEvh,
     ILogger log)
    {
      var exceptions = new List<Exception>();

      foreach (EventData eventData in events)
      {
        try
        {
          DateTime enqueuedTimeUtc = eventData.SystemProperties.EnqueuedTimeUtc;

          // Just for checking the current Activity while debugging.
          var activityCurrent = Activity.Current;

          string messageBody = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);
          log.LogInformation($"First function processed a message: {messageBody}");

          // Keeping the initial enqueued time just for personal use. You can delete this logic.
          var newEventBody = new
          {
            initialEnqueuedTimeUtc = enqueuedTimeUtc,
            originalBody = messageBody
          };
          EventData newEvent = new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(newEventBody)));
          await secondEvh.AddAsync(newEvent);
 
        }
        catch (Exception e)
        {
          // We need to keep processing the rest of the batch - capture this exception and continue.
          // Also, consider capturing details of the message that failed processing so it can be processed again later.
          exceptions.Add(e);
        }
      }
      // Once processing of the batch is complete, if any messages in the batch failed processing throw an exception so that there is a record of the failure.

      if (exceptions.Count > 1)
        throw new AggregateException(exceptions);

      if (exceptions.Count == 1)
        throw exceptions.Single();
    }
  }
}

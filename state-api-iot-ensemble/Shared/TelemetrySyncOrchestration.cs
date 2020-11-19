using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.Azure.WebJobs.Extensions.SignalRService;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Fathym;
using Microsoft.Azure.Storage.Blob;
using System.Runtime.Serialization;
using Fathym.API;
using System.Collections.Generic;
using System.Linq;
using LCU.Personas.Client.Applications;
using LCU.StateAPI;
using LCU.StateAPI.Utilities;
using System.Security.Claims;
using LCU.Personas.Client.Enterprises;
using LCU.State.API.IoTEnsemble.State;
using LCU.Personas.Client.Security;
using System.Threading;
using DurableTask.Core.Exceptions;
using Microsoft.Azure.Documents.Client;

namespace LCU.State.API.IoTEnsemble.Shared
{
    [Serializable]
    [DataContract]
    public class TelemetrySyncRequest : BaseRequest
    { }

    public class TelemetrySyncOrchestration
    {
        protected SecurityManagerClient secMgr;

        public TelemetrySyncOrchestration(SecurityManagerClient secMgr)
        {
            this.secMgr = secMgr;
        }

        #region API Methods
        [FunctionName("TelemetrySyncOrchestration")]
        public virtual async Task<Status> RunOrchestrator([OrchestrationTrigger] IDurableOrchestrationContext context, ILogger log)
        {
            var stateCtxt = context.GetInput<StateActionContext>();

            if (!context.IsReplaying)
                log.LogInformation($"Setting up telemetry sync: {stateCtxt.ToJSON()}");

            var genericRetryOptions = new DurableTask.Core.RetryOptions(TimeSpan.FromSeconds(1), 10)
            {
                BackoffCoefficient = 1.5,
                Handle = handleRetryException
            };

            var status = Status.Initialized;

            try
            {
                status = await handleTelemetrySync(context, log, stateCtxt);

                if (status && !context.IsReplaying)
                    log.LogInformation($"Telemetry sync shut down");
                else if (!context.IsReplaying)
                    log.LogError($"Telemetry sync shut down due to error: {status.ToJSON()}");
            }
            catch (FunctionFailedException fex)
            {
                if (fex.InnerException is StatusException sex)
                {
                    status = Status.GeneralError.Clone(sex.Message, new { Exception = fex.InnerException.ToString() });
                    // status = Status.GeneralError.Clone("Unable to finish booting organization, please contact support.", new { Exception = fex.InnerException.ToString() });

                    if (!context.IsReplaying)
                        log.LogInformation($"Booting organization failed: {fex.ToString()}");

                    if (stateCtxt.ActionRequest == null)
                        stateCtxt.ActionRequest = new Presentation.State.ReqRes.ExecuteActionRequest();

                    stateCtxt.ActionRequest.Arguments = status.JSONConvert<MetadataModel>();

                    status = await context.CallActivityAsync<Status>("TelemetrySyncOrchestration_UpdateStatus", stateCtxt);
                }
            }

            return status;
        }

        [FunctionName("TelemetrySyncOrchestration_Sync")]
        public virtual async Task<Status> Sync([ActivityTrigger] StateActionContext stateCtxt, ILogger log,
            [SignalR(HubName = IoTEnsembleSharedState.HUB_NAME)] IAsyncCollector<SignalRMessage> signalRMessages,
            [Blob("state-api/{stateCtxt.StateDetails.EnterpriseLookup}/{stateCtxt.StateDetails.HubName}/{stateCtxt.StateDetails.Username}/{stateCtxt.StateDetails.StateKey}", FileAccess.ReadWrite)] CloudBlockBlob stateBlob,
            [CosmosDB(
                databaseName: "%LCU-WARM-TELEMETRY-DATABASE%",
                collectionName: "%LCU-WARM-TELEMETRY-CONTAINER%",
                ConnectionStringSetting = "LCU-WARM-TELEMETRY-CONNECTION-STRING")]DocumentClient docClient)
            // SqlQuery = "SELECT top 2 * FROM c order by c._ts desc")]IEnumerable<object> docs)
        {
            return await stateBlob.WithStateHarness<IoTEnsembleSharedState, TelemetrySyncRequest, IoTEnsembleSharedStateHarness>(stateCtxt.StateDetails,
                stateCtxt.ActionRequest, signalRMessages, log, async (harness, reqData) =>
                {
                    log.LogInformation($"Checking if build and release are complete...");

                    var loaded = await harness.LoadDeviceTelemetry(secMgr, docClient);

                    return loaded;
                }, preventStatusException: true);
            // });
        }

        #endregion

        #region Helpers
        protected virtual bool handleRetryException(Exception ex)
        {
            if (ex is TaskFailedException tex)
            {
                if (tex.InnerException is StatusException sex)
                    return true;
                else
                    return false;
            }
            else
                return false;
        }

        protected virtual async Task<Status> handleTelemetrySync(IDurableOrchestrationContext context, ILogger log, StateActionContext stateCtxt)
        {
            var synced = Status.GeneralError;

            //  Max telemetry sync cycle
            var operationTimeoutTime = context.CurrentUtcDateTime.AddMinutes(30);

            if (!context.IsReplaying)
                log.LogInformation($"Instantiating telemtry sync loop for: {stateCtxt.ToJSON()}");

            while (context.CurrentUtcDateTime < operationTimeoutTime)
            {
                if (!context.IsReplaying)
                    log.LogInformation($"Waiting for telemetry sync for: {stateCtxt.ToJSON()}");

                synced = await context.CallActivityAsync<Status>("TelemetrySyncOrchestration_Sync", stateCtxt);

                if (!synced)
                {
                    if (!context.IsReplaying)
                        log.LogInformation($"Error durring sync process: {synced.ToJSON()}");

                    break;
                }
                else
                {
                    var refreshRate = synced.Metadata.ContainsKey("RefreshRate") ? synced.Metadata["RefreshRate"].ToString().As<int>() : 30 ;
                    
                    // Wait for the next checkpoint
                    var nextCheckpoint = context.CurrentUtcDateTime.AddSeconds(refreshRate);

                    if (!context.IsReplaying)
                        log.LogInformation($"Continuing telemtry sync at {nextCheckpoint} for: {stateCtxt.ToJSON()}");

                    await context.CreateTimer(nextCheckpoint, CancellationToken.None);
                }
            }

            synced = await context.CallActivityAsync<Status>("TelemetrySyncOrchestration_Sync", stateCtxt);

            return synced;
        }
        #endregion
    }
}
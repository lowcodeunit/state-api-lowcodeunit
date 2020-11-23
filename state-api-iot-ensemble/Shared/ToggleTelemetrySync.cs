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
using Fathym;
using Microsoft.Azure.Storage.Blob;
using System.Runtime.Serialization;
using Fathym.API;
using System.Collections.Generic;
using System.Linq;
using LCU.Personas.Client.Applications;
using LCU.StateAPI.Utilities;
using System.Security.Claims;
using LCU.Personas.Client.Enterprises;
using LCU.State.API.IoTEnsemble.State;
using LCU.Personas.Client.Security;
using System.Net.Http;
using System.Net;
using System.Text;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;

namespace LCU.State.API.IoTEnsemble.Shared
{
    [Serializable]
    [DataContract]
    public class ToggleTelemetrySyncRequest : BaseRequest
    {
        [DataMember]
        public virtual int? PageSize { get; set; }
    }

    public class ToggleTelemetrySync
    {
        protected SecurityManagerClient secMgr;

        public ToggleTelemetrySync(SecurityManagerClient secMgr)
        {
            this.secMgr = secMgr;
        }

        [FunctionName("ToggleTelemetrySync")]
        public virtual async Task<Status> Run([HttpTrigger] HttpRequest req, ILogger log,
            [DurableClient] IDurableOrchestrationClient starter,
            [SignalR(HubName = IoTEnsembleSharedState.HUB_NAME)] IAsyncCollector<SignalRMessage> signalRMessages,
            [Blob("state-api/{headers.lcu-ent-lookup}/{headers.lcu-hub-name}/{headers.x-ms-client-principal-id}/{headers.lcu-state-key}", FileAccess.ReadWrite)] CloudBlockBlob stateBlob)
        {
            return await stateBlob.WithStateHarness<IoTEnsembleSharedState, ToggleTelemetrySyncRequest, IoTEnsembleSharedStateHarness>(req, signalRMessages, log,
                async (harness, dataReq, actReq) =>
            {
                log.LogInformation($"ToggleTelemetrySync");

                var stateDetails = StateUtils.LoadStateDetails(req);

                await harness.ToggleTelemetrySyncEnabled(starter, stateDetails, actReq, secMgr, 
                    dataReq.PageSize.HasValue ? dataReq.PageSize.Value : 20);

                return Status.Success;
            });
        }
    }
}

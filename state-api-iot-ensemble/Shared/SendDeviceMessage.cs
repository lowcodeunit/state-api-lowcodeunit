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

namespace LCU.State.API.IoTEnsemble.Shared
{
    [Serializable]
    [DataContract]
    public class SendDeviceMessageRequest : BaseRequest
    { 
        [DataMember]
        public virtual string DeviceName { get; set; }
        
        [DataMember]
        public virtual IoTEnsembleTelemetryPayload Payload { get; set; }
    }

    public class SendDeviceMessage
    {
        protected ApplicationArchitectClient appArch;

        public SendDeviceMessage(ApplicationArchitectClient appArch)
        {
            this.appArch = appArch;
        }

        [FunctionName("SendDeviceMessage")]
        public virtual async Task<Status> Run([HttpTrigger] HttpRequest req, ILogger log,
            [SignalR(HubName = IoTEnsembleSharedState.HUB_NAME)]IAsyncCollector<SignalRMessage> signalRMessages,
            [Blob("state-api/{headers.lcu-ent-lookup}/{headers.lcu-hub-name}/{headers.x-ms-client-principal-id}/{headers.lcu-state-key}", FileAccess.ReadWrite)] CloudBlockBlob stateBlob)
        {
            return await stateBlob.WithStateHarness<IoTEnsembleSharedState, SendDeviceMessageRequest, IoTEnsembleSharedStateHarness>(req, signalRMessages, log,
                async (harness, dataReq, actReq) =>
            {
                log.LogInformation($"SendDeviceMessage");

                var stateDetails = StateUtils.LoadStateDetails(req);

                await harness.SendDeviceMessage(appArch, dataReq.DeviceName, dataReq.Payload);

                return Status.Success;
            });
        }
    }
}

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
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using LCU.Personas.Client.Identity;

namespace LCU.State.API.IoTEnsemble.Host
{
    [Serializable]
    [DataContract]
    public class RefreshRequest : BaseRequest
    { }

    public class Refresh
    {
        protected ApplicationArchitectClient appArch;

        protected EnterpriseArchitectClient entArch;

        protected EnterpriseManagerClient entMgr;

        protected IdentityManagerClient idMgr;

        protected SecurityManagerClient secMgr;

        public Refresh(ApplicationArchitectClient appArch, EnterpriseArchitectClient entArch, EnterpriseManagerClient entMgr, IdentityManagerClient idMgr,
            SecurityManagerClient secMgr)
        {
            this.appArch = appArch;
            
            this.entArch = entArch;
            
            this.entMgr = entMgr;

            this.idMgr = idMgr;
            
            this.secMgr = secMgr;           
        }

        [FunctionName("Refresh")]
        public virtual async Task<Status> Run([HttpTrigger] HttpRequest req, ILogger log,
            [DurableClient] IDurableOrchestrationClient starter,
            [SignalR(HubName = IoTEnsembleSharedState.HUB_NAME)]IAsyncCollector<SignalRMessage> signalRMessages,
            [Blob("state-api/{headers.lcu-ent-lookup}/{headers.lcu-hub-name}/{headers.x-ms-client-principal-id}/{headers.lcu-state-key}", FileAccess.ReadWrite)] CloudBlockBlob stateBlob)
        {
            return await stateBlob.WithStateHarness<IoTEnsembleSharedState, RefreshRequest, IoTEnsembleSharedStateHarness>(req, signalRMessages, log,
                async (harness, refreshReq, actReq) =>
            {
                log.LogInformation($"Refresh");

                var stateDetails = StateUtils.LoadStateDetails(req);

                await harness.Refresh(starter, stateDetails, actReq, appArch, entArch, entMgr, idMgr, secMgr);

                return Status.Success;
            });
        }
    }
}

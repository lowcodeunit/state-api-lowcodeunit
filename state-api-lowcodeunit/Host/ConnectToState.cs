using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using System.Security.Claims;
using Microsoft.Azure.WebJobs.Extensions.SignalRService;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using LCU.StateAPI;
using Microsoft.Azure.Storage.Blob;
using LCU.StateAPI.Utilities;
using LCU.State.API.LowCodeUnit.State;

namespace LCU.State.API.LowCodeUnit.Host
{
    public static class ConnectToState
    {
        [FunctionName("ConnectToState")]
        public static async Task<ConnectToStateResponse> Run([HttpTrigger(AuthorizationLevel.Anonymous, "post")]HttpRequest req, ILogger log,
            ClaimsPrincipal claimsPrincipal, //[LCUStateDetails]StateDetails stateDetails,
            [SignalR(HubName = LowCodeUnitSharedState.HUB_NAME)]IAsyncCollector<SignalRMessage> signalRMessages,
            [SignalR(HubName = LowCodeUnitSharedState.HUB_NAME)]IAsyncCollector<SignalRGroupAction> signalRGroupActions,
            [Blob("state-api/{headers.lcu-ent-lookup}/{headers.lcu-hub-name}/{headers.x-ms-client-principal-id}/{headers.lcu-state-key}", FileAccess.ReadWrite)] CloudBlockBlob stateBlob)
        {
            return await signalRMessages.ConnectToState<LowCodeUnitSharedState>(req, log, claimsPrincipal, stateBlob, signalRGroupActions);
        }
    }
}

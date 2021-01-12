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
using LCU.State.API.LowCodeUnit.State;
using LCU.Personas.Client.Security;

namespace LCU.State.API.LowCodeUnit.Shared
{
    [Serializable]
    [DataContract]
    public class EnrollDeviceRequest : BaseRequest
    {
    }

    public class EnrollDevice
    {
        protected ApplicationArchitectClient appArch;

        public EnrollDevice(ApplicationArchitectClient appArch)
        {
            this.appArch = appArch;
        }

        [FunctionName("EnrollDevice")]
        public virtual async Task<Status> Run([HttpTrigger] HttpRequest req, ILogger log,
            [SignalR(HubName = LowCodeUnitSharedState.HUB_NAME)] IAsyncCollector<SignalRMessage> signalRMessages,
            [Blob("state-api/{headers.lcu-ent-lookup}/{headers.lcu-hub-name}/{headers.x-ms-client-principal-id}/{headers.lcu-state-key}", FileAccess.ReadWrite)] CloudBlockBlob stateBlob)
        {
            var status = await stateBlob.WithStateHarness<LowCodeUnitSharedState, EnrollDeviceRequest, LowCodeUnitSharedStateHarness>(req, signalRMessages, log,
                async (harness, dataReq, actReq) =>
                {
                    log.LogInformation($"Setting Loading device telemetry from UpdateTelemetrySync...");

                    harness.State.HomePage.Loading = true;

                    return Status.Success;
                }, preventStatusException: true);

            if (status)
                status = await stateBlob.WithStateHarness<LowCodeUnitSharedState, EnrollDeviceRequest, LowCodeUnitSharedStateHarness>(req, signalRMessages, log,
                    async (harness, enrollReq, actReq) =>
                    {
                        log.LogInformation($"EnrollDevice");

                        var stateDetails = StateUtils.LoadStateDetails(req);

                        await harness.EnrollDevice(appArch);

                        harness.State.HomePage.Loading = false;

                        return Status.Success;
                    });

            return status;
        }
    }
}

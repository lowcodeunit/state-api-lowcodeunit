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
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json.Converters;

namespace LCU.State.API.IoTEnsemble.Shared.StorageAccess
{
    [Serializable]
    [DataContract]
    public class ColdQueryRequest : BaseRequest
    {
        [JsonConverter(typeof(StringEnumConverter))]
        public virtual ColdQueryDataTypes? DataType { get; set; }

        [DataMember]
        public virtual DateTime EndDate { get; set; }

        [DataMember]
        public virtual bool Flatten { get; set; }

        [DataMember]
        public virtual bool IncludeEmulated { get; set; }

        [DataMember]
        public virtual int Page { get; set; }

        [DataMember]
        public virtual int PageSize { get; set; }

        [JsonConverter(typeof(StringEnumConverter))]
        public virtual ColdQueryResultTypes? ResultType { get; set; }

        [DataMember]
        public virtual List<string> SelectedDeviceIDs { get; set; }

        [DataMember]
        public virtual DateTime StartDate { get; set; }
    }

    public class ColdQuery
    {
        protected SecurityManagerClient secMgr;

        public ColdQuery(SecurityManagerClient secMgr)
        {
            this.secMgr = secMgr;
        }

        [FunctionName("ColdQuery")]
        public virtual async Task<HttpResponseMessage> Run([HttpTrigger] HttpRequest req, ILogger log,
            [SignalR(HubName = IoTEnsembleSharedState.HUB_NAME)] IAsyncCollector<SignalRMessage> signalRMessages,
            [Blob("state-api/{headers.lcu-ent-lookup}/{headers.lcu-hub-name}/{headers.x-ms-client-principal-id}/{headers.lcu-state-key}", FileAccess.ReadWrite)] CloudBlockBlob stateBlob,
            [Blob("cold-storage", FileAccess.Read, Connection = "LCU-COLD-STORAGE-CONNECTION-STRING")] CloudBlobDirectory coldBlobs)
        {
            var queried = new byte[] { };

            var status = await stateBlob.WithStateHarness<IoTEnsembleSharedState, ColdQueryRequest, IoTEnsembleSharedStateHarness>(req, signalRMessages, log,
                async (harness, dataReq, actReq) =>
                {
                    log.LogInformation($"Running a ColdQuery: {dataReq}");

                    var stateDetails = StateUtils.LoadStateDetails(req);

                    queried = await harness.ColdQuery(coldBlobs, dataReq.SelectedDeviceIDs, dataReq.PageSize, dataReq.Page,
                        dataReq.IncludeEmulated, dataReq.StartDate, dataReq.EndDate, dataReq.ResultType, dataReq.Flatten, dataReq.DataType);

                    return !queried.IsNullOrEmpty();
                }, preventStatusException: true);

            HttpContent content;

            if (status)
                content = new ByteArrayContent(queried);
            else
            {
                var resp = new BaseResponse() { Status = status };

                content = new StringContent(resp.ToJSON(), Encoding.UTF8, "application/json");
            }

            var statusCode = status ? HttpStatusCode.OK : HttpStatusCode.InternalServerError;

            return new HttpResponseMessage(statusCode)
            {
                Content = content
            };
        }
    }
}

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
using System.Net.Http.Headers;

namespace LCU.State.API.IoTEnsemble.Shared.StorageAccess
{
    [Serializable]
    [DataContract]
    public class ColdQueryRequest : BaseRequest
    {
        [JsonConverter(typeof(StringEnumConverter))]
        public virtual ColdQueryDataTypes? DataType { get; set; }

        [DataMember]
        public virtual DateTime? EndDate { get; set; }

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
        public virtual DateTime? StartDate { get; set; }

        [DataMember]
        public virtual bool Zip { get; set; }
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
            [Blob("cold-storage/data", FileAccess.Read, Connection = "LCU-COLD-STORAGE-CONNECTION-STRING")] CloudBlobDirectory coldBlob)
        {
            var queried = new byte[] { };

            var fileName = String.Empty;

            var contentType = String.Empty;

            var status = await stateBlob.WithStateHarness<IoTEnsembleSharedState, ColdQueryRequest, IoTEnsembleSharedStateHarness>(req, signalRMessages, log,
                async (harness, dataReq, actReq) =>
                {
                    log.LogInformation($"Running a ColdQuery: {dataReq}");

                    var stateDetails = StateUtils.LoadStateDetails(req);

                    var now = DateTime.UtcNow;

                    if (dataReq.StartDate == null)
                        dataReq.StartDate = now.AddDays(-30);

                    if (dataReq.EndDate == null)
                        dataReq.EndDate = now;

                    if (dataReq.ResultType == null)
                        dataReq.ResultType = ColdQueryResultTypes.JSON;

                    if (dataReq.DataType == null)
                        dataReq.DataType = ColdQueryDataTypes.Telemetry;

                    var fileExtension = getFileExtension(dataReq.ResultType);

                    fileName = buildFileName(dataReq.DataType.Value, dataReq.StartDate.Value, dataReq.EndDate.Value, fileExtension);

                    queried = await harness.ColdQuery(coldBlob, dataReq.SelectedDeviceIDs, dataReq.PageSize, dataReq.Page,
                        dataReq.IncludeEmulated, dataReq.StartDate, dataReq.EndDate, dataReq.ResultType, dataReq.Flatten, dataReq.DataType,
                        dataReq.Zip, fileName, fileExtension);

                    if (dataReq.ResultType == ColdQueryResultTypes.CSV)
                        contentType = "text/csv";
                    else if (dataReq.ResultType == ColdQueryResultTypes.JSON)
                        contentType = "application/json";
                    else if (dataReq.ResultType == ColdQueryResultTypes.JSONLines)
                        contentType = "application/jsonl";

                    return !queried.IsNullOrEmpty();
                }, preventStatusException: true);

            HttpContent content;

            if (status)
            {
                content = new ByteArrayContent(queried);

                content.Headers.ContentDisposition = new System.Net.Http.Headers.ContentDispositionHeaderValue("attachment");

                content.Headers.ContentDisposition.FileName = fileName;

                content.Headers.ContentType = new MediaTypeHeaderValue(contentType);
            }
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

        private static string buildFileName(ColdQueryDataTypes dataType, DateTime startDate, DateTime endDate, string fileExtension)
        {
            var dtTypeStr = dataType.ToString().ToLower();

            var startStr = startDate.ToString("YYYYMMDDHHmmss");

            var endStr = endDate.ToString("YYYYMMDDHHmmss");

            var fileName = $"{dtTypeStr}-{startStr}-{endStr}.{fileExtension}";

            return fileName;
        }

        private static string getFileExtension(ColdQueryResultTypes? resultType)
        {
            var fileExtension = String.Empty;

            if (resultType == null || resultType == ColdQueryResultTypes.JSON)
                fileExtension = "json";
            else if (resultType == ColdQueryResultTypes.JSONLines)
                fileExtension = "jsonl";
            else if (resultType == ColdQueryResultTypes.CSV)
                fileExtension = "csv";

            return fileExtension;
        }
    }
}

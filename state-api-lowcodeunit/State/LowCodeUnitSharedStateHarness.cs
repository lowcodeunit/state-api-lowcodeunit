using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Fathym;
using LCU.Presentation.State.ReqRes;
using LCU.StateAPI.Utilities;
using LCU.StateAPI;
using Microsoft.Azure.WebJobs.Extensions.SignalRService;
using Newtonsoft.Json.Converters;
using System.Runtime.Serialization;
using System.Collections.Generic;
using LCU.Personas.Client.Enterprises;
using LCU.Personas.Client.DevOps;
using LCU.Personas.Enterprises;
using LCU.Personas.Client.Applications;
using Fathym.API;
using LCU.Personas.Applications;
using LCU.Personas.Client.Security;
using System.Net.Http;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using LCU.Personas.Client.Identity;
using Microsoft.Azure.Storage.Blob;
using Newtonsoft.Json.Linq;
using System.Text;
using System.IO.Compression;
using System.Net.Http.Headers;
using System.Net;
using CsvHelper;

namespace LCU.State.API.LowCodeUnit.State
{
    public class LowCodeUnitSharedStateHarness : LCUStateHarness<LowCodeUnitSharedState>
    {
        #region Constants
        #endregion

        #region Fields
        #endregion

        #region Properties
        #endregion

        #region Constructors
        public LowCodeUnitSharedStateHarness(LowCodeUnitSharedState state, ILogger logger)
            : base(state ?? new LowCodeUnitSharedState(), logger)
        { }
        #endregion

        #region API Methods
        public virtual async Task<bool> EnrollDevice(ApplicationArchitectClient appArch)
        {
            return false;
        }

        public virtual async Task EnsureUserEnterprise(EnterpriseArchitectClient entArch, EnterpriseManagerClient entMgr,
            SecurityManagerClient secMgr, string parentEntLookup, string username)
        {
            if (State.UserEnterpriseLookup.IsNullOrEmpty())
            {
                var hostLookup = $"{parentEntLookup}|{username}";

                var getResp = await entMgr.ResolveHost(hostLookup, false);

                if (!getResp.Status || getResp.Model == null)
                {
                    var createResp = await entArch.CreateEnterprise(new CreateEnterpriseRequest()
                    {
                        Name = username,
                        Description = username,
                        Host = hostLookup
                    }, parentEntLookup, username);

                    if (createResp.Status)
                        State.UserEnterpriseLookup = createResp.Model.EnterpriseLookup;
                }
                else
                    State.UserEnterpriseLookup = getResp.Model.EnterpriseLookup;
            }

            if (State.UserEnterpriseLookup.IsNullOrEmpty())
                throw new Exception("Unable to establish the user's enterprise, please try again.");
        }

        public virtual async Task Refresh(IDurableOrchestrationClient starter, StateDetails stateDetails, ExecuteActionRequest exActReq,
            ApplicationArchitectClient appArch, EnterpriseArchitectClient entArch, EnterpriseManagerClient entMgr, IdentityManagerClient idMgr,
            SecurityManagerClient secMgr, DocumentClient client)
        {
            // await EnsureUserEnterprise(entArch, entMgr, secMgr, stateDetails.EnterpriseLookup, stateDetails.Username);

            State.Loading = false;

            State.HomePage.Loading = false;

            State.SSL.Loading = false;
        }
        #endregion

        #region Helpers
        #endregion
    }

    [Serializable]
    [DataContract]
    public enum ColdQueryResultTypes
    {
        [EnumMember]
        CSV,

        [EnumMember]
        JSON,

        [EnumMember]
        JSONLines
    }

    [Serializable]
    [DataContract]
    public enum ColdQueryDataTypes
    {
        [EnumMember]
        Telemetry,

        [EnumMember]
        Observations,

        [EnumMember]
        SensorMetadata
    }
}

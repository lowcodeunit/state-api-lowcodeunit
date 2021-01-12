using System;
using System.IO;
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
using LCU.Personas.Applications;
using Fathym.API;

namespace LCU.State.API.LowCodeUnit.State
{
    [Serializable]
    [DataContract]
    public class LowCodeUnitSharedState
    {
        #region Constants
        public const string HUB_NAME = "lowcodeunit";
        #endregion

        [DataMember]
        public virtual HomePageInfo HomePage { get; set; }

        [DataMember]
        public virtual bool Loading { get; set; }

        [DataMember]
        public virtual SSLSetup SSL { get; set; }

        [DataMember]
        public virtual string UserEnterpriseLookup { get; set; }
    }

    [Serializable]
    [DataContract]
    public class HomePageInfo
    {
        [DataMember]
        public virtual bool Loading { get; set; }
    }

    [Serializable]
    [DataContract]
    public class SSLSetup
    {
        [DataMember]
        public virtual bool Loading { get; set; }
    }
}

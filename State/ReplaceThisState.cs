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

namespace LCU.State.API.NapkinIDE.NapkinIDE.ReplaceThis.State
{
    [Serializable]
    [DataContract]
    public class ReplaceThisState
    {
        #region Constants
        public const string HUB_NAME = "replacethis";
        #endregion
        
        [DataMember]
        public virtual bool Loading { get; set; }
    }
}

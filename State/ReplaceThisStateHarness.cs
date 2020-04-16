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

namespace LCU.State.API.NapkinIDE.NapkinIDE.ReplaceThis.State
{
    public class ReplaceThisStateHarness : LCUStateHarness<ReplaceThisState>
    {
        #region Fields
        #endregion

        #region Properties
        #endregion

        #region Constructors
        public ReplaceThisStateHarness(ReplaceThisState state)
            : base(state ?? new ReplaceThisState())
        { }
        #endregion

        #region API Methods
        public virtual void CompleteBoot()
        {
            // State.L = true;
        }
        #endregion
    }
}

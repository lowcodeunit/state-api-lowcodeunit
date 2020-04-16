using Microsoft.AspNetCore.Http;
using Microsoft.Azure.Functions.Extensions.DependencyInjection;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using LCU.Personas.Client.Applications;
using LCU.Personas.Client.Enterprises;
using LCU.Personas.Client.Identity;
using LCU.Personas.Client.Security;
using System.Linq;
using System;
using LCU.StateAPI;

[assembly: FunctionsStartup(typeof(LCU.State.API.NapkinIDE.NapkinIDE.ReplaceThis.Host.Startup))]

namespace LCU.State.API.NapkinIDE.NapkinIDE.ReplaceThis.Host
{
    public class Startup : StateAPIStartup
    {
        #region Fields
        #endregion

        #region Constructors
        public Startup()
        { }
        #endregion

        #region API Methods
        #endregion
    }
}
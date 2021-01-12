using Microsoft.Azure.Functions.Extensions.DependencyInjection;
using LCU.StateAPI.Hosting;
using System.Linq;

[assembly: FunctionsStartup(typeof(LCU.State.API.LowCodeUnit.Host.Startup))]

namespace LCU.State.API.LowCodeUnit.Host
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
		// public override void Configure(IFunctionsHostBuilder builder)
		// {
		// 	builder.Services.FirstOrDefault(svc => svc.ServiceType.FullName.Contains("MVC"));
        // }
        #endregion
    }
}
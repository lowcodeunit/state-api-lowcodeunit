using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using LCU.Graphs.Registry.Enterprises;
using LCU.Personas.Client.Applications;
using LCU.Personas.Client.Enterprises;
using LCU.Personas.Client.Identity;
using LCU.Personas.Enterprises;
using LCU.State.API.IoTEnsemble.State;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.SignalRService;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;

namespace LCU.State.API.IoTEnsemble.Shared
{
    public class GenerateDeviceReferenceData
    {
        protected ApplicationArchitectClient appArch;

        protected EnterpriseManagerClient entMgr;

        protected IdentityManagerClient idMgr;

        public GenerateDeviceReferenceData(ApplicationArchitectClient appArch, EnterpriseManagerClient entMgr, IdentityManagerClient idMgr)
        {
            this.appArch = appArch;

            this.entMgr = entMgr;

            this.idMgr = idMgr;
        }

        [FunctionName("GenerateDeviceReferenceData")]
        public virtual async Task Run([TimerTrigger("0 */1 * * * *")] TimerInfo myTimer, ILogger log,
            [Blob("cold-storage/reference-data", FileAccess.Read, Connection = "LCU-COLD-STORAGE-CONNECTION-STRING")] CloudBlobDirectory refDataBlob)
        {
            log.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");

            var parentEntLookup = Environment.GetEnvironmentVariable("LCU-ENTERPRISE-LOOKUP");

            var billingPlanOptions = await entMgr.ListBillingPlanOptions(parentEntLookup, "iot");

            var childEnts = await entMgr.ListChildEnterprises(parentEntLookup);

            if (childEnts.Status)
                await childEnts.Model.Each(async childEnt =>
                {
                    await processChildEnt(refDataBlob, childEnt, billingPlanOptions.Model);
                }, parallel: true);
        }

        #region Helpers
        protected virtual async Task processChildEnt(CloudBlobDirectory refDataBlob, Enterprise childEnt, 
            List<BillingPlanOption> billigPlanOptions)
        {
            var entLookupParts = childEnt.EnterpriseLookup.Split('|');

            if (entLookupParts.Length >= 2)
            {
                var parentLookup = entLookupParts[0];

                var username = entLookupParts[1];

                var devices = await appArch.ListEnrolledDevices(username);

                var license = await idMgr.HasLicenseAccess(parentLookup, username, Personas.AllAnyTypes.All, new List<string>() { "iot" });

                if (license.Status && license.Model != null)
                {

                }
                else
                {

                }

                var now = DateTime.UtcNow;

                var dateBlob = refDataBlob.GetDirectoryReference($"{now.ToString("YYYY-MM-DD")}");

                var timeBlob = dateBlob.GetDirectoryReference($"{now.AddMinutes(1).ToString("HH-mm")}");
            }
        }
        #endregion
    }
}

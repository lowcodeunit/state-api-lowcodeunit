using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Fathym;
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
        #region Fields
        protected readonly ApplicationArchitectClient appArch;

        protected readonly EnterpriseManagerClient entMgr;

        protected readonly IdentityManagerClient idMgr;

        protected readonly string parentEntLookup;
        #endregion

        public GenerateDeviceReferenceData(ApplicationArchitectClient appArch, EnterpriseManagerClient entMgr, IdentityManagerClient idMgr)
        {
            this.appArch = appArch;

            this.entMgr = entMgr;

            this.idMgr = idMgr;

            parentEntLookup = Environment.GetEnvironmentVariable("LCU-ENTERPRISE-LOOKUP");
        }

        [FunctionName("GenerateDeviceReferenceData")]
        public virtual async Task Run([TimerTrigger("0 */1 * * * *")] TimerInfo myTimer, ILogger log,
            [Blob("cold-storage/reference-data", FileAccess.Read, Connection = "LCU-COLD-STORAGE-CONNECTION-STRING")] CloudBlobDirectory refDataBlobDir)
        {
            log.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");

            var refData = await loadReferenceData();

            await uploadReferenceData(refDataBlobDir, refData);
        }

        #region Helpers
        protected virtual async Task<List<IoTEnsembleEnterpriseReferenceData>> loadReferenceData()
        {
            var childEnts = await entMgr.ListChildEnterprises(parentEntLookup);

            var refData = new List<IoTEnsembleEnterpriseReferenceData>();

            if (childEnts.Status)
                await childEnts.Model.Each(async childEnt =>
                {
                    var metadata = await processChildEnt(childEnt);

                    lock (refData)
                        refData.AddRange(metadata);
                }, parallel: true);

            return refData;
        }

        protected virtual async Task<List<IoTEnsembleEnterpriseReferenceData>> processChildEnt(Enterprise childEnt)
        {
            var refData = new List<IoTEnsembleEnterpriseReferenceData>();

            await childEnt.Hosts.Each(async childEntHost =>
            {
                var hostLookupParts = childEntHost.Split('|');

                if (hostLookupParts.Length >= 2)
                {
                    var parentLookup = hostLookupParts[0];

                    var username = hostLookupParts[1];

                    var license = await idMgr.HasLicenseAccess(parentLookup, username, Personas.AllAnyTypes.All, new List<string>() { "iot" });

                    IoTEnsembleEnterpriseReferenceData refd;

                    if (license.Status && license.Model != null)
                        refd = license.Model.JSONConvert<IoTEnsembleEnterpriseReferenceData>();
                    else
                        refd = new IoTEnsembleEnterpriseReferenceData()
                        {
                            Devices = 1,
                            DataInterval = 300,
                            DataRetention = 43200
                        };

                    refd.EnterpriseLookup = childEnt.EnterpriseLookup;

                    refData.Add(refd);
                }
            });

            return refData;
        }

        protected virtual async Task uploadReferenceData(CloudBlobDirectory refDataBlobDir, List<IoTEnsembleEnterpriseReferenceData> refData)
        {
            var now = DateTime.UtcNow;

            var dateBlobDir = refDataBlobDir.GetDirectoryReference($"{now.ToString("yyyy-MM-dd")}");

            var timeBlobDir = dateBlobDir.GetDirectoryReference($"{now.AddMinutes(1).ToString("HH-mm")}");

            var refDataBlob = timeBlobDir.GetBlockBlobReference("enterprise.ref-data.json");

            await refDataBlob.UploadTextAsync(refData.ToJSON());
        }
        #endregion
    }
}

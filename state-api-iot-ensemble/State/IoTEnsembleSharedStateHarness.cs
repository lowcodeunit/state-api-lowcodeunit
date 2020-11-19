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

namespace LCU.State.API.IoTEnsemble.State
{
    public class IoTEnsembleSharedStateHarness : LCUStateHarness<IoTEnsembleSharedState>
    {
        #region Constants
        const string DETAILS_PANE_ENABLED = "IoTEnsemble:DetailsPaneEnabled";

        const string EMULATED_DEVICE_ENABLED = "IoTEnsemble:EmulatedDeviceEnabled";

        const string DEVICE_DASHBOARD_FREEBOARD_CONFIG = "IoTEnsemble:DeviceDashboardFreeboardConfig";

        const string TELEMETRY_SYNC_ENABLED = "IoTEnsemble:TelemetrySyncEnabled";
        #endregion

        #region Fields
        protected readonly string warmTelemetryContainer;

        protected readonly string warmTelemetryDatabase;
        #endregion

        #region Properties
        #endregion

        #region Constructors
        public IoTEnsembleSharedStateHarness(IoTEnsembleSharedState state, ILogger logger)
            : base(state ?? new IoTEnsembleSharedState(), logger)
        {
            warmTelemetryContainer = Environment.GetEnvironmentVariable("LCU-WARM-TELEMETRY-CONTAINER");

            warmTelemetryDatabase = Environment.GetEnvironmentVariable("LCU-WARM-TELEMETRY-DATABASE");
        }
        #endregion

        #region API Methods
        public virtual async Task EnsureDevicesDashboard(SecurityManagerClient secMgr)
        {
            if (State.Dashboard == null)
                State.Dashboard = new IoTEnsembleDashboardConfiguration();

            if (!State.UserEnterpriseLookup.IsNullOrEmpty())
            {
                if (State.Dashboard.FreeboardConfig == null)
                {
                    var tpd = await secMgr.RetrieveEnterpriseThirdPartyData(State.UserEnterpriseLookup, DEVICE_DASHBOARD_FREEBOARD_CONFIG);

                    if (tpd.Status && tpd.Model.ContainsKey(DEVICE_DASHBOARD_FREEBOARD_CONFIG) && !tpd.Model[DEVICE_DASHBOARD_FREEBOARD_CONFIG].IsNullOrEmpty())
                        State.Dashboard.FreeboardConfig = tpd.Model[DEVICE_DASHBOARD_FREEBOARD_CONFIG].FromJSON<MetadataModel>();
                    else
                    {
                        var freeboardConfig = loadDefaultFreeboardConfig();

                        var resp = await secMgr.SetEnterpriseThirdPartyData(State.UserEnterpriseLookup, new Dictionary<string, string>()
                        {
                            { DEVICE_DASHBOARD_FREEBOARD_CONFIG, freeboardConfig.ToJSON() }
                        });

                        if (resp.Status)
                            State.Dashboard.FreeboardConfig = freeboardConfig;
                    }

                }
            }
            else
                throw new Exception("Unable to load the user's enterprise, please try again or contact support.");
        }

        public virtual async Task EnsureDrawersConfig(SecurityManagerClient secMgr)
        {
            if (State.Drawers == null)
                State.Drawers = new IoTEnsembleDrawersConfig();

            if (!State.UserEnterpriseLookup.IsNullOrEmpty())
            {
                var tpd = await secMgr.RetrieveEnterpriseThirdPartyData(State.UserEnterpriseLookup, DETAILS_PANE_ENABLED);

                if (tpd.Status && tpd.Model.ContainsKey(DETAILS_PANE_ENABLED) && !tpd.Model[DETAILS_PANE_ENABLED].IsNullOrEmpty())
                    State.Drawers.DetailsActive = tpd.Model[DETAILS_PANE_ENABLED].As<bool>();
                else
                {
                    var resp = await secMgr.SetEnterpriseThirdPartyData(State.UserEnterpriseLookup, new Dictionary<string, string>()
                    {
                        { DETAILS_PANE_ENABLED, true.ToString() }
                    });

                    if (resp.Status)
                        State.Drawers.DetailsActive = true;
                }
            }
            else
                throw new Exception("Unable to load the user's enterprise, please try again or contact support.");
        }

        public virtual async Task EnsureEmulatedDeviceInfo(SecurityManagerClient secMgr)
        {
            if (State.Emulated == null)
                State.Emulated = new EmulatedDeviceInfo();

            if (!State.UserEnterpriseLookup.IsNullOrEmpty())
            {
                var tpd = await secMgr.RetrieveEnterpriseThirdPartyData(State.UserEnterpriseLookup, EMULATED_DEVICE_ENABLED);

                if (tpd.Status && tpd.Model.ContainsKey(EMULATED_DEVICE_ENABLED) && !tpd.Model[EMULATED_DEVICE_ENABLED].IsNullOrEmpty())
                    State.Emulated.Enabled = tpd.Model[EMULATED_DEVICE_ENABLED].As<bool>();
                else
                {
                    State.Emulated.Enabled = true;

                    await ToggleEmulatedEnabled(secMgr);
                }
            }
            else
                throw new Exception("Unable to load the user's enterprise, please try again or contact support.");
        }

        public virtual async Task<bool> EnrollDevice(ApplicationArchitectClient appArch, string deviceName)
        {
            var enrollResp = await appArch.EnrollDevice(new EnrollDeviceRequest()
            {
                DeviceID = $"{State.UserEnterpriseLookup}-{deviceName}"
            }, State.UserEnterpriseLookup, DeviceAttestationTypes.SymmetricKey, DeviceEnrollmentTypes.Individual, envLookup: null);

            var status = enrollResp.Status;

            await LoadDevices(appArch);

            return false;
        }

        public virtual async Task EnsureUserEnterprise(EnterpriseArchitectClient entArch, EnterpriseManagerClient entMgr,
            SecurityManagerClient secMgr, string parentEntLookup, string username)
        {
            if (State.UserEnterpriseLookup.IsNullOrEmpty())
            {
                var getResp = await entMgr.ResolveHost(username.ToMD5Hash(), false);

                if (!getResp.Status || getResp.Model == null)
                {
                    var createResp = await entArch.CreateEnterprise(new CreateEnterpriseRequest()
                    {
                        Name = username,
                        Description = username,
                        Host = username.ToMD5Hash()
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

        public virtual async Task LoadDevices(ApplicationArchitectClient appArch)
        {
            var devicesResp = await appArch.ListEnrolledDevices(State.UserEnterpriseLookup, envLookup: null);

            State.Devices = devicesResp.Model?.Select(m =>
            {
                var devInfo = m.JSONConvert<IoTEnsembleDeviceInfo>();

                devInfo.DeviceName = devInfo.DeviceID.Replace($"{State.UserEnterpriseLookup}-", String.Empty);

                return devInfo;
            }).JSONConvert<List<IoTEnsembleDeviceInfo>>() ?? new List<IoTEnsembleDeviceInfo>();
        }

        public virtual async Task<Status> LoadDeviceTelemetry(SecurityManagerClient secMgr, DocumentClient client)
        {
            var status = Status.Success;

            if (State.DeviceTelemetry == null)
                State.DeviceTelemetry = new IoTEnsembleDeviceTelemetry()
                {
                    RefreshRate = 30
                };

            var tpd = await secMgr.RetrieveEnterpriseThirdPartyData(State.UserEnterpriseLookup, TELEMETRY_SYNC_ENABLED);

            if (tpd.Status && tpd.Model.ContainsKey(TELEMETRY_SYNC_ENABLED) && !tpd.Model[TELEMETRY_SYNC_ENABLED].IsNullOrEmpty())
                State.DeviceTelemetry.Enabled = tpd.Model[TELEMETRY_SYNC_ENABLED].As<bool>();

            State.DeviceTelemetry.Payloads = new List<IoTEnsembleDeviceTelemetryPayload>();

            if (State.DeviceTelemetry.Enabled)
            {
                try
                {
                    var payloads = await queryTelemetryPayloads(client);

                    if (!payloads.IsNullOrEmpty())
                        State.DeviceTelemetry.Payloads.AddRange(payloads);

                    status.Metadata["RefreshRate"] = State.DeviceTelemetry.RefreshRate > 10 ? State.DeviceTelemetry.RefreshRate : 30;

                    State.DeviceTelemetry.RefreshRate = status.Metadata["RefreshRate"].ToString().As<int>();
                }
                catch (Exception ex)
                {
                    log.LogError(ex, "There was an issue loading your device telemetry.");

                    status = Status.GeneralError.Clone("There was an issue loading your device telemetry.");
                }
            }

            return status;
        }

        public virtual async Task Refresh(ApplicationArchitectClient appArch, EnterpriseArchitectClient entArch, EnterpriseManagerClient entMgr,
            SecurityManagerClient secMgr, string parentEntLookup, string username, string host)
        {
            await EnsureUserEnterprise(entArch, entMgr, secMgr, parentEntLookup, username);

            await Task.WhenAll(
                EnsureEmulatedDeviceInfo(secMgr),
                EnsureDevicesDashboard(secMgr),
                EnsureDrawersConfig(secMgr),
                LoadDevices(appArch)
            );
        }

        public virtual async Task ToggleDetailsPane(SecurityManagerClient secMgr)
        {
            if (!State.UserEnterpriseLookup.IsNullOrEmpty())
            {
                var active = !State.Drawers.DetailsActive;

                var resp = await secMgr.SetEnterpriseThirdPartyData(State.UserEnterpriseLookup, new Dictionary<string, string>()
                {
                    { DETAILS_PANE_ENABLED, active.ToString() }
                });

                if (resp.Status)
                    State.Drawers.DetailsActive = active;
            }
            else
                throw new Exception("Unable to load the user's enterprise, please try again or contact support.");
        }

        public virtual async Task ToggleEmulatedEnabled(SecurityManagerClient secMgr)
        {
            if (!State.UserEnterpriseLookup.IsNullOrEmpty())
            {
                var enabled = !State.Emulated.Enabled;

                var resp = await secMgr.SetEnterpriseThirdPartyData(State.UserEnterpriseLookup, new Dictionary<string, string>()
                {
                    { DETAILS_PANE_ENABLED, enabled.ToString() }
                });

                if (resp.Status)
                    State.Emulated.Enabled = enabled;
            }
            else
                throw new Exception("Unable to load the user's enterprise, please try again or contact support.");
        }

        public virtual async Task ToggleTelemetrySyncEnabled(SecurityManagerClient secMgr)
        {
            if (!State.UserEnterpriseLookup.IsNullOrEmpty())
            {
                var enabled = !State.DeviceTelemetry.Enabled;

                var resp = await secMgr.SetEnterpriseThirdPartyData(State.UserEnterpriseLookup, new Dictionary<string, string>()
                {
                    { TELEMETRY_SYNC_ENABLED, enabled.ToString() }
                });

                if (resp.Status)
                    State.DeviceTelemetry.Enabled = enabled;
            }
            else
                throw new Exception("Unable to load the user's enterprise, please try again or contact support.");
        }
        #endregion

        #region Helpers
        protected virtual MetadataModel loadDefaultFreeboardConfig()
        {
            return "{\r\n\t\"version\": 1,\r\n\t\"allow_edit\": true,\r\n\t\"plugins\": [],\r\n\t\"panes\": [\r\n\t\t{\r\n\t\t\t\"width\": 1,\r\n\t\t\t\"row\": {\r\n\t\t\t\t\"3\": 1\r\n\t\t\t},\r\n\t\t\t\"col\": {\r\n\t\t\t\t\"3\": 1\r\n\t\t\t},\r\n\t\t\t\"col_width\": 3,\r\n\t\t\t\"widgets\": [\r\n\t\t\t\t{\r\n\t\t\t\t\t\"type\": \"text_widget\",\r\n\t\t\t\t\t\"settings\": {\r\n\t\t\t\t\t\t\"size\": \"regular\",\r\n\t\t\t\t\t\t\"value\": \"Device Insights & Monitoring\",\r\n\t\t\t\t\t\t\"animate\": true\r\n\t\t\t\t\t}\r\n\t\t\t\t}\r\n\t\t\t]\r\n\t\t},\r\n\t\t{\r\n\t\t\t\"title\": \"Last Processed Device Data\",\r\n\t\t\t\"width\": 1,\r\n\t\t\t\"row\": {\r\n\t\t\t\t\"3\": 5\r\n\t\t\t},\r\n\t\t\t\"col\": {\r\n\t\t\t\t\"3\": 1\r\n\t\t\t},\r\n\t\t\t\"col_width\": 2,\r\n\t\t\t\"widgets\": [\r\n\t\t\t\t{\r\n\t\t\t\t\t\"type\": \"text_widget\",\r\n\t\t\t\t\t\"settings\": {\r\n\t\t\t\t\t\t\"size\": \"regular\",\r\n\t\t\t\t\t\t\"value\": \"datasources[\\\"Query\\\"][datasources[\\\"Query\\\"].length - 1][\\\"DeviceID\\\"]\",\r\n\t\t\t\t\t\t\"animate\": true\r\n\t\t\t\t\t}\r\n\t\t\t\t},\r\n\t\t\t\t{\r\n\t\t\t\t\t\"type\": \"html\",\r\n\t\t\t\t\t\"settings\": {\r\n\t\t\t\t\t\t\"html\": \"JSON.stringify(datasources[\\\"Query\\\"][datasources[\\\"Query\\\"].length - 1])\",\r\n\t\t\t\t\t\t\"height\": 4\r\n\t\t\t\t\t}\r\n\t\t\t\t}\r\n\t\t\t]\r\n\t\t},\r\n\t\t{\r\n\t\t\t\"title\": \"Connected Devices (Last 3 Days)\",\r\n\t\t\t\"width\": 1,\r\n\t\t\t\"row\": {\r\n\t\t\t\t\"3\": 5\r\n\t\t\t},\r\n\t\t\t\"col\": {\r\n\t\t\t\t\"3\": 3\r\n\t\t\t},\r\n\t\t\t\"col_width\": 1,\r\n\t\t\t\"widgets\": [\r\n\t\t\t\t{\r\n\t\t\t\t\t\"type\": \"html\",\r\n\t\t\t\t\t\"settings\": {\r\n\t\t\t\t\t\t\"html\": \"JSON.stringify(Array.from(new Set(datasources[\\\"Query\\\"].map((q) => q.DeviceID))))\",\r\n\t\t\t\t\t\t\"height\": 4\r\n\t\t\t\t\t}\r\n\t\t\t\t}\r\n\t\t\t]\r\n\t\t}\r\n\t],\r\n\t\"datasources\": [\r\n\t\t{\r\n\t\t\t\"name\": \"Query\",\r\n\t\t\t\"type\": \"JSON\",\r\n\t\t\t\"settings\": {\r\n\t\t\t\t\"url\": \"\\/api\\/data-flow\\/iot\\/warm-query\",\r\n\t\t\t\t\"use_thingproxy\": false,\r\n\t\t\t\t\"refresh\": 30,\r\n\t\t\t\t\"method\": \"GET\"\r\n\t\t\t}\r\n\t\t}\r\n\t],\r\n\t\"columns\": 3\r\n}".FromJSON<MetadataModel>();
        }

        protected virtual async Task<List<IoTEnsembleDeviceTelemetryPayload>> queryTelemetryPayloads(DocumentClient client)
        {
            Uri colUri = UriFactory.CreateDocumentCollectionUri(warmTelemetryDatabase, warmTelemetryContainer);

            IQueryable<IoTEnsembleDeviceTelemetryPayload> docsQueryBldr =
                client.CreateDocumentQuery<IoTEnsembleDeviceTelemetryPayload>(colUri)
                .Where(payload => payload.EnterpriseLookup == State.UserEnterpriseLookup);

            if (!State.SelectedDeviceID.IsNullOrEmpty())
                docsQueryBldr = docsQueryBldr
                    .Where(payload => payload.DeviceID == State.SelectedDeviceID);

            docsQueryBldr = docsQueryBldr
                .Take(50)
                .Skip(0)
                .OrderByDescending(payload => payload._ts);

            var docsQuery = docsQueryBldr.AsDocumentQuery();

            var payloads = new List<IoTEnsembleDeviceTelemetryPayload>();

            while (docsQuery.HasMoreResults)
                payloads.AddRange(await docsQuery.ExecuteNextAsync<IoTEnsembleDeviceTelemetryPayload>());

            return payloads;
        }
        #endregion
    }
}

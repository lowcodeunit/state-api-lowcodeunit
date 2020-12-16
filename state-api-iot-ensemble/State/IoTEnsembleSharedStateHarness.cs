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
        protected readonly string telemetryRoot;

        protected readonly string warmTelemetryContainer;

        protected readonly string warmTelemetryDatabase;
        #endregion

        #region Properties
        #endregion

        #region Constructors
        public IoTEnsembleSharedStateHarness(IoTEnsembleSharedState state, ILogger logger)
            : base(state ?? new IoTEnsembleSharedState(), logger)
        {
            telemetryRoot = Environment.GetEnvironmentVariable("LCU-TELEMETRY-ROOT");

            if (telemetryRoot.IsNullOrEmpty())
                telemetryRoot = String.Empty;

            warmTelemetryContainer = Environment.GetEnvironmentVariable("LCU-WARM-TELEMETRY-CONTAINER");

            warmTelemetryDatabase = Environment.GetEnvironmentVariable("LCU-WARM-TELEMETRY-DATABASE");
        }
        #endregion

        #region API Methods
        public virtual async Task<bool> EnrollDevice(ApplicationArchitectClient appArch, IoTEnsembleDeviceEnrollment device)
        {
            var enrollResp = await appArch.EnrollDevice(new EnrollDeviceRequest()
            {
                DeviceID = $"{State.UserEnterpriseLookup}-{device.DeviceName}"
            }, State.UserEnterpriseLookup, DeviceAttestationTypes.SymmetricKey, DeviceEnrollmentTypes.Individual, envLookup: null);

            var status = enrollResp.Status;

            await LoadDevices(appArch);

            return false;
        }

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

        public virtual async Task EnsureTelemetry(IDurableOrchestrationClient starter, StateDetails stateDetails,
            ExecuteActionRequest exActReq, SecurityManagerClient secMgr)
        {
            if (State.Telemetry == null)
                State.Telemetry = new IoTEnsembleTelemetry()
                {
                    RefreshRate = 30,
                    PageSize = 20
                };

            if (!State.UserEnterpriseLookup.IsNullOrEmpty())
            {
                var tpd = await secMgr.RetrieveEnterpriseThirdPartyData(State.UserEnterpriseLookup, TELEMETRY_SYNC_ENABLED);

                if (tpd.Status && tpd.Model.ContainsKey(TELEMETRY_SYNC_ENABLED) && !tpd.Model[TELEMETRY_SYNC_ENABLED].IsNullOrEmpty())
                    State.Telemetry.Enabled = tpd.Model[TELEMETRY_SYNC_ENABLED].As<bool>();
                else
                    await setTelemetryEnabled(secMgr, false);

                await EnsureTelemetrySyncState(starter, stateDetails, exActReq);
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

        public virtual async Task EnsureTelemetrySyncState(IDurableOrchestrationClient starter, StateDetails stateDetails,
            ExecuteActionRequest exActReq)
        {
            var instanceId = $"{stateDetails.EnterpriseLookup}-{stateDetails.HubName}-{stateDetails.Username}-{stateDetails.StateKey}-{State.UserEnterpriseLookup}";

            var existingOrch = await starter.GetStatusAsync(instanceId);

            var isStartState = existingOrch?.RuntimeStatus == OrchestrationRuntimeStatus.Running ||
                existingOrch?.RuntimeStatus == OrchestrationRuntimeStatus.Pending;

            log.LogInformation($"Is telemetry sync enabled ({State.Telemetry.Enabled}) vs start state ({isStartState})...");

            if (State.Telemetry.Enabled && !isStartState)
            {
                log.LogInformation($"Sarting TelemetrySyncOrchestration: {instanceId}");

                await starter.StartAction("TelemetrySyncOrchestration", stateDetails, exActReq, log, instanceId: instanceId);
            }
            else if (!State.Telemetry.Enabled && isStartState)
            {
                log.LogInformation($"Terminating TelemetrySyncOrchestration: {instanceId}");

                await starter.TerminateAsync(instanceId, "Device Telemetry has been disbaled.");
            }
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
            
            if(State.ConnectedDevicesConfig == null)
            {
                State.ConnectedDevicesConfig = new IoTEnsembleConnectedDevicesConfig();
            }

            var devicesResp = await appArch.ListEnrolledDevices(State.UserEnterpriseLookup, envLookup: null);

            State.ConnectedDevicesConfig.Devices = devicesResp.Model?.Select(m =>
            {
                var devInfo = m.JSONConvert<IoTEnsembleDeviceInfo>();

                devInfo.DeviceName = devInfo.DeviceID.Replace($"{State.UserEnterpriseLookup}-", String.Empty);

                return devInfo;

            }).JSONConvert<List<IoTEnsembleDeviceInfo>>() ?? new List<IoTEnsembleDeviceInfo>();
        }

        public virtual async Task<Status> LoadTelemetry(SecurityManagerClient secMgr, DocumentClient client)
        {
            var status = Status.Success;

            if (State.Telemetry == null)
                State.Telemetry = new IoTEnsembleTelemetry()
                {
                    RefreshRate = 30,
                    PageSize = 20
                };

            State.Telemetry.Payloads = new List<IoTEnsembleTelemetryPayload>();

            if (State.Telemetry.Enabled)
            {
                try
                {
                    var payloads = await queryTelemetryPayloads(client, State.UserEnterpriseLookup,
                        State.SelectedDeviceIDs, State.Telemetry.PageSize, State.Emulated.Enabled);

                    if (!payloads.IsNullOrEmpty())
                        State.Telemetry.Payloads.AddRange(payloads);

                    status.Metadata["RefreshRate"] = State.Telemetry.RefreshRate >= 10 ? State.Telemetry.RefreshRate : 30;

                    State.Telemetry.RefreshRate = status.Metadata["RefreshRate"].ToString().As<int>();

                    State.Telemetry.LastSyncedAt = DateTime.Now;
                }
                catch (Exception ex)
                {
                    log.LogError(ex, "There was an issue loading your device telemetry.");

                    status = Status.GeneralError.Clone("There was an issue loading your device telemetry.");
                }
            }
            else
                status = Status.GeneralError.Clone("Device Telemetry is Disabled");

            return status;
        }

        public virtual async Task Refresh(IDurableOrchestrationClient starter, StateDetails stateDetails, ExecuteActionRequest exActReq,
            ApplicationArchitectClient appArch, EnterpriseArchitectClient entArch, EnterpriseManagerClient entMgr,
            SecurityManagerClient secMgr, string parentEntLookup, string username, string host)
        {
            await EnsureUserEnterprise(entArch, entMgr, secMgr, parentEntLookup, username);

            await Task.WhenAll(
                EnsureEmulatedDeviceInfo(secMgr),
                EnsureDevicesDashboard(secMgr),
                EnsureDrawersConfig(secMgr),
                EnsureTelemetry(starter, stateDetails, exActReq, secMgr),
                LoadDevices(appArch)
            );
        }

        public virtual async Task<bool> RevokeDeviceEnrollment(ApplicationArchitectClient appArch, string deviceId)
        {
            var revokeResp = await appArch.RevokeDeviceEnrollment(deviceId, State.UserEnterpriseLookup, envLookup: null);

            var status = revokeResp.Status;

            await LoadDevices(appArch);

            return false;
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
                    { EMULATED_DEVICE_ENABLED, enabled.ToString() }
                });

                if (resp.Status)
                    State.Emulated.Enabled = enabled;
            }
            else
                throw new Exception("Unable to load the user's enterprise, please try again or contact support.");
        }

        public virtual async Task ToggleTelemetrySyncEnabled(IDurableOrchestrationClient starter, StateDetails stateDetails,
            ExecuteActionRequest exActReq, SecurityManagerClient secMgr, DocumentClient client)
        {
            if (!State.UserEnterpriseLookup.IsNullOrEmpty())
            {
                await setTelemetryEnabled(secMgr, !State.Telemetry.Enabled);

                await EnsureTelemetrySyncState(starter, stateDetails, exActReq);

                await LoadTelemetry(secMgr, client);
            }
            else
                throw new Exception("Unable to load the user's enterprise, please try again or contact support.");
        }

        public virtual async Task UpdateTelemetrySync(int refreshRate, int pageSize){
            if (!State.UserEnterpriseLookup.IsNullOrEmpty())
            {
                
                State.Telemetry.RefreshRate = refreshRate;

                State.Telemetry.PageSize = pageSize;

            }
            else
                throw new Exception("Unable to load the user's enterprise, please try again or contact support.");
        }

         public virtual async Task UpdateConnectedDevicesSync(int pageSize){
            if (!State.UserEnterpriseLookup.IsNullOrEmpty())
            {
                State.ConnectedDevicesConfig.PageSize = pageSize;
            }
            else
                throw new Exception("Unable to load the user's enterprise, please try again or contact support.");
        }

        
        #endregion

        #region Helpers
        protected virtual MetadataModel loadDefaultFreeboardConfig()
        {
            return ("{\r\n\t\"version\": 1,\r\n\t\"allow_edit\": true,\r\n\t\"plugins\": [],\r\n\t\"panes\": [\r\n\t\t{\r\n\t\t\t\"title\": \"\",\r\n\t\t\t\"width\": 1,\r\n\t\t\t\"row\": {\r\n\t\t\t\t\"3\": 1,\r\n\t\t\t\t\"4\": 1\r\n\t\t\t},\r\n\t\t\t\"col\": {\r\n\t\t\t\t\"3\": 1,\r\n\t\t\t\t\"4\": 1\r\n\t\t\t},\r\n\t\t\t\"col_width\": 10,\r\n\t\t\t\"widgets\": [\r\n\t\t\t\t{\r\n\t\t\t\t\t\"type\": \"text_widget\",\r\n\t\t\t\t\t\"settings\": {\r\n\t\t\t\t\t\t\"title\": \"\",\r\n\t\t\t\t\t\t\"size\": \"regular\",\r\n\t\t\t\t\t\t\"value\": \"Device Insights & Monitoring\",\r\n\t\t\t\t\t\t\"animate\": true\r\n\t\t\t\t\t}\r\n\t\t\t\t}\r\n\t\t\t]\r\n\t\t},\r\n\t\t{\r\n\t\t\t\"title\": \"Sensor Placement\",\r\n\t\t\t\"width\": 1,\r\n\t\t\t\"row\": {\r\n\t\t\t\t\"3\": 5,\r\n\t\t\t\t\"4\": 5,\r\n\t\t\t\t\"5\": 5,\r\n\t\t\t\t\"11\": 5\r\n\t\t\t},\r\n\t\t\t\"col\": {\r\n\t\t\t\t\"3\": 3,\r\n\t\t\t\t\"4\": -8,\r\n\t\t\t\t\"5\": -8,\r\n\t\t\t\t\"11\": -8\r\n\t\t\t},\r\n\t\t\t\"col_width\": 1,\r\n\t\t\t\"widgets\": [\r\n\t\t\t\t{\r\n\t\t\t\t\t\"type\": \"text_widget\",\r\n\t\t\t\t\t\"settings\": {\r\n\t\t\t\t\t\t\"title\": \"Floor\",\r\n\t\t\t\t\t\t\"size\": \"big\",\r\n\t\t\t\t\t\t\"value\": \"datasources[\\\"Fathym Device Data\\\"][datasources[\\\"Fathym Device Data\\\"].length - 1][\\\"DeviceData\\\"][\\\"Floor\\\"]\",\r\n\t\t\t\t\t\t\"animate\": true\r\n\t\t\t\t\t}\r\n\t\t\t\t},\r\n\t\t\t\t{\r\n\t\t\t\t\t\"type\": \"text_widget\",\r\n\t\t\t\t\t\"settings\": {\r\n\t\t\t\t\t\t\"title\": \"Room\",\r\n\t\t\t\t\t\t\"size\": \"big\",\r\n\t\t\t\t\t\t\"value\": [\r\n\t\t\t\t\t\t\t\"datasources[\\\"Fathym Device Data\\\"][datasources[\\\"Fathym Device Data\\\"].length - 1][\\\"DeviceData\\\"][\\\"Room\\\"]\"\r\n\t\t\t\t\t\t],\r\n\t\t\t\t\t\t\"animate\": true\r\n\t\t\t\t\t}\r\n\t\t\t\t},\r\n\t\t\t\t{\r\n\t\t\t\t\t\"type\": \"text_widget\",\r\n\t\t\t\t\t\"settings\": {\r\n\t\t\t\t\t\t\"title\": \"Occupancy\",\r\n\t\t\t\t\t\t\"size\": \"big\",\r\n\t\t\t\t\t\t\"value\": \"datasources[\\\"Fathym Device Data\\\"][datasources[\\\"Fathym Device Data\\\"].length - 1][\\\"SensorReadings\\\"][\\\"Occupancy\\\"]\",\r\n\t\t\t\t\t\t\"animate\": true\r\n\t\t\t\t\t}\r\n\t\t\t\t},\r\n\t\t\t\t{\r\n\t\t\t\t\t\"type\": \"text_widget\",\r\n\t\t\t\t\t\"settings\": {\r\n\t\t\t\t\t\t\"title\": \"Occupied\",\r\n\t\t\t\t\t\t\"size\": \"big\",\r\n\t\t\t\t\t\t\"value\": \"datasources[\\\"Fathym Device Data\\\"][datasources[\\\"Fathym Device Data\\\"].length - 1][\\\"SensorReadings\\\"][\\\"Occupied\\\"]\",\r\n\t\t\t\t\t\t\"animate\": true\r\n\t\t\t\t\t}\r\n\t\t\t\t}\r\n\t\t\t]\r\n\t\t},\r\n\t\t{\r\n\t\t\t\"title\": \"Sensor Data\",\r\n\t\t\t\"width\": 1,\r\n\t\t\t\"row\": {\r\n\t\t\t\t\"3\": 5,\r\n\t\t\t\t\"4\": 1,\r\n\t\t\t\t\"7\": 1,\r\n\t\t\t\t\"26\": 1\r\n\t\t\t},\r\n\t\t\t\"col\": {\r\n\t\t\t\t\"3\": 1,\r\n\t\t\t\t\"4\": 2,\r\n\t\t\t\t\"7\": 2,\r\n\t\t\t\t\"26\": 2\r\n\t\t\t},\r\n\t\t\t\"col_width\": 2,\r\n\t\t\t\"widgets\": [\r\n\t\t\t\t{\r\n\t\t\t\t\t\"type\": \"text_widget\",\r\n\t\t\t\t\t\"settings\": {\r\n\t\t\t\t\t\t\"title\": \"Device ID\",\r\n\t\t\t\t\t\t\"size\": \"regular\",\r\n\t\t\t\t\t\t\"value\": \"datasources[\\\"Fathym Device Data\\\"][datasources[\\\"Fathym Device Data\\\"].length - 1][\\\"DeviceID\\\"]\",\r\n\t\t\t\t\t\t\"animate\": true\r\n\t\t\t\t\t}\r\n\t\t\t\t},\r\n\t\t\t\t{\r\n\t\t\t\t\t\"type\": \"gauge\",\r\n\t\t\t\t\t\"settings\": {\r\n\t\t\t\t\t\t\"title\": \"Temperature\",\r\n\t\t\t\t\t\t\"value\": \"datasources[\\\"Fathym Device Data\\\"][datasources[\\\"Fathym Device Data\\\"].length - 1][\\\"SensorReadings\\\"][\\\"Temperature\\\"]\",\r\n\t\t\t\t\t\t\"units\": \"\u00B0F\",\r\n\t\t\t\t\t\t\"min_value\": 0,\r\n\t\t\t\t\t\t\"max_value\": \"150\"\r\n\t\t\t\t\t}\r\n\t\t\t\t},\r\n\t\t\t\t{\r\n\t\t\t\t\t\"type\": \"gauge\",\r\n\t\t\t\t\t\"settings\": {\r\n\t\t\t\t\t\t\"title\": \"Humidity\",\r\n\t\t\t\t\t\t\"value\": \"datasources[\\\"Fathym Device Data\\\"][datasources[\\\"Fathym Device Data\\\"].length - 1][\\\"SensorReadings\\\"][\\\"Humidity\\\"]\",\r\n\t\t\t\t\t\t\"units\": \"%\",\r\n\t\t\t\t\t\t\"min_value\": 0,\r\n\t\t\t\t\t\t\"max_value\": 100\r\n\t\t\t\t\t}\r\n\t\t\t\t}\r\n\t\t\t]\r\n\t\t},\r\n\t\t{\r\n\t\t\t\"title\": \"Temperature History\",\r\n\t\t\t\"width\": 1,\r\n\t\t\t\"row\": {\r\n\t\t\t\t\"3\": 23,\r\n\t\t\t\t\"11\": 23\r\n\t\t\t},\r\n\t\t\t\"col\": {\r\n\t\t\t\t\"3\": 1,\r\n\t\t\t\t\"11\": 1\r\n\t\t\t},\r\n\t\t\t\"col_width\": 3,\r\n\t\t\t\"widgets\": [\r\n\t\t\t\t{\r\n\t\t\t\t\t\"type\": \"text_widget\",\r\n\t\t\t\t\t\"settings\": {\r\n\t\t\t\t\t\t\"size\": \"regular\",\r\n\t\t\t\t\t\t\"value\": \"datasources[\\\"Fathym Device Data\\\"][datasources[\\\"Fathym Device Data\\\"].length - 1][\\\"SensorReadings\\\"][\\\"Temperature\\\"]\",\r\n\t\t\t\t\t\t\"sparkline\": true,\r\n\t\t\t\t\t\t\"animate\": true,\r\n\t\t\t\t\t\t\"units\": \"\u00B0F\"\r\n\t\t\t\t\t}\r\n\t\t\t\t}\r\n\t\t\t]\r\n\t\t},\r\n\t\t{\r\n\t\t\t\"title\": \"Map\",\r\n\t\t\t\"width\": 1,\r\n\t\t\t\"row\": {\r\n\t\t\t\t\"3\": 29,\r\n\t\t\t\t\"4\": 9,\r\n\t\t\t\t\"11\": 9,\r\n\t\t\t\t\"15\": 9,\r\n\t\t\t\t\"27\": 9\r\n\t\t\t},\r\n\t\t\t\"col\": {\r\n\t\t\t\t\"3\": 1,\r\n\t\t\t\t\"4\": 1,\r\n\t\t\t\t\"11\": 1,\r\n\t\t\t\t\"15\": 1,\r\n\t\t\t\t\"27\": 1\r\n\t\t\t},\r\n\t\t\t\"col_width\": 10,\r\n\t\t\t\"widgets\": [\r\n\t\t\t\t{\r\n\t\t\t\t\t\"type\": \"google_map\",\r\n\t\t\t\t\t\"settings\": {\r\n\t\t\t\t\t\t\"lat\": \"datasources[\\\"Fathym Device Data\\\"][datasources[\\\"Fathym Device Data\\\"].length - 1][\\\"DeviceData\\\"][\\\"Latitude\\\"]\",\r\n\t\t\t\t\t\t\"lon\": \"datasources[\\\"Fathym Device Data\\\"][datasources[\\\"Fathym Device Data\\\"].length - 1][\\\"DeviceData\\\"][\\\"Longitude\\\"]\"\r\n\t\t\t\t\t}\r\n\t\t\t\t}\r\n\t\t\t]\r\n\t\t},\r\n\t\t{\r\n\t\t\t\"title\": \"Last Processed Device Data\",\r\n\t\t\t\"width\": 1,\r\n\t\t\t\"row\": {\r\n\t\t\t\t\"3\": 39,\r\n\t\t\t\t\"21\": 25,\r\n\t\t\t\t\"24\": 25,\r\n\t\t\t\t\"36\": 25\r\n\t\t\t},\r\n\t\t\t\"col\": {\r\n\t\t\t\t\"3\": 1,\r\n\t\t\t\t\"21\": 1,\r\n\t\t\t\t\"24\": 1,\r\n\t\t\t\t\"36\": 1\r\n\t\t\t},\r\n\t\t\t\"col_width\": 2,\r\n\t\t\t\"widgets\": [\r\n\t\t\t\t{\r\n\t\t\t\t\t\"type\": \"text_widget\",\r\n\t\t\t\t\t\"settings\": {\r\n\t\t\t\t\t\t\"title\": \"Device ID\",\r\n\t\t\t\t\t\t\"size\": \"regular\",\r\n\t\t\t\t\t\t\"value\": \"datasources[\\\"Fathym Device Data\\\"][datasources[\\\"Fathym Device Data\\\"].length - 1][\\\"deviceid\\\"]\",\r\n\t\t\t\t\t\t\"animate\": true\r\n\t\t\t\t\t}\r\n\t\t\t\t},\r\n\t\t\t\t{\r\n\t\t\t\t\t\"type\": \"html\",\r\n\t\t\t\t\t\"settings\": {\r\n\t\t\t\t\t\t\"html\": \"JSON.stringify(datasources[\\\"Fathym Device Data\\\"][datasources[\\\"Fathym Device Data\\\"].length - 1])\",\r\n\t\t\t\t\t\t\"height\": 4\r\n\t\t\t\t\t}\r\n\t\t\t\t}\r\n\t\t\t]\r\n\t\t},\r\n\t\t{\r\n\t\t\t\"title\": \"Connected Devices (Last 3 Days)\",\r\n\t\t\t\"width\": 1,\r\n\t\t\t\"row\": {\r\n\t\t\t\t\"3\": 39,\r\n\t\t\t\t\"22\": 25,\r\n\t\t\t\t\"25\": 25,\r\n\t\t\t\t\"37\": 25\r\n\t\t\t},\r\n\t\t\t\"col\": {\r\n\t\t\t\t\"3\": 3,\r\n\t\t\t\t\"22\": 3,\r\n\t\t\t\t\"25\": 3,\r\n\t\t\t\t\"37\": 3\r\n\t\t\t},\r\n\t\t\t\"col_width\": 1,\r\n\t\t\t\"widgets\": [\r\n\t\t\t\t{\r\n\t\t\t\t\t\"type\": \"html\",\r\n\t\t\t\t\t\"settings\": {\r\n\t\t\t\t\t\t\"html\": \"JSON.stringify(Array.from(new Set(datasources[\\\"Fathym Device Data\\\"].map((q) => q.DeviceID))))\",\r\n\t\t\t\t\t\t\"height\": 4\r\n\t\t\t\t\t}\r\n\t\t\t\t}\r\n\t\t\t]\r\n\t\t}\r\n\t],\r\n\t\"datasources\": [\r\n\t\t{\r\n\t\t\t\"name\": \"Fathym Device Data\",\r\n\t\t\t\"type\": \"JSON\",\r\n\t\t\t\"settings\": {\r\n\t\t\t\t\"url\": \"" + $"{telemetryRoot}\\/api\\/iot-ensemble\\/devices\\/telemetry" + "\",\r\n\t\t\t\t\"use_thingproxy\": true,\r\n\t\t\t\t\"refresh\": 30,\r\n\t\t\t\t\"method\": \"GET\"\r\n\t\t\t}\r\n\t\t}\r\n\t],\r\n\t\"columns\": 3\r\n}").FromJSON<MetadataModel>();
            // return "{\r\n\t\"version\": 1,\r\n\t\"allow_edit\": true,\r\n\t\"plugins\": [],\r\n\t\"panes\": [\r\n\t\t{\r\n\t\t\t\"width\": 1,\r\n\t\t\t\"row\": {\r\n\t\t\t\t\"3\": 1\r\n\t\t\t},\r\n\t\t\t\"col\": {\r\n\t\t\t\t\"3\": 1\r\n\t\t\t},\r\n\t\t\t\"col_width\": 3,\r\n\t\t\t\"widgets\": [\r\n\t\t\t\t{\r\n\t\t\t\t\t\"type\": \"text_widget\",\r\n\t\t\t\t\t\"settings\": {\r\n\t\t\t\t\t\t\"size\": \"regular\",\r\n\t\t\t\t\t\t\"value\": \"Device Insights & Monitoring\",\r\n\t\t\t\t\t\t\"animate\": true\r\n\t\t\t\t\t}\r\n\t\t\t\t}\r\n\t\t\t]\r\n\t\t},\r\n\t\t{\r\n\t\t\t\"title\": \"Last Processed Device Data\",\r\n\t\t\t\"width\": 1,\r\n\t\t\t\"row\": {\r\n\t\t\t\t\"3\": 5\r\n\t\t\t},\r\n\t\t\t\"col\": {\r\n\t\t\t\t\"3\": 1\r\n\t\t\t},\r\n\t\t\t\"col_width\": 2,\r\n\t\t\t\"widgets\": [\r\n\t\t\t\t{\r\n\t\t\t\t\t\"type\": \"text_widget\",\r\n\t\t\t\t\t\"settings\": {\r\n\t\t\t\t\t\t\"size\": \"regular\",\r\n\t\t\t\t\t\t\"value\": \"datasources[\\\"Query\\\"][datasources[\\\"Query\\\"].length - 1][\\\"DeviceID\\\"]\",\r\n\t\t\t\t\t\t\"animate\": true\r\n\t\t\t\t\t}\r\n\t\t\t\t},\r\n\t\t\t\t{\r\n\t\t\t\t\t\"type\": \"html\",\r\n\t\t\t\t\t\"settings\": {\r\n\t\t\t\t\t\t\"html\": \"JSON.stringify(datasources[\\\"Query\\\"][datasources[\\\"Query\\\"].length - 1])\",\r\n\t\t\t\t\t\t\"height\": 4\r\n\t\t\t\t\t}\r\n\t\t\t\t}\r\n\t\t\t]\r\n\t\t},\r\n\t\t{\r\n\t\t\t\"title\": \"Connected Devices (Last 3 Days)\",\r\n\t\t\t\"width\": 1,\r\n\t\t\t\"row\": {\r\n\t\t\t\t\"3\": 5\r\n\t\t\t},\r\n\t\t\t\"col\": {\r\n\t\t\t\t\"3\": 3\r\n\t\t\t},\r\n\t\t\t\"col_width\": 1,\r\n\t\t\t\"widgets\": [\r\n\t\t\t\t{\r\n\t\t\t\t\t\"type\": \"html\",\r\n\t\t\t\t\t\"settings\": {\r\n\t\t\t\t\t\t\"html\": \"JSON.stringify(Array.from(new Set(datasources[\\\"Query\\\"].map((q) => q.DeviceID))))\",\r\n\t\t\t\t\t\t\"height\": 4\r\n\t\t\t\t\t}\r\n\t\t\t\t}\r\n\t\t\t]\r\n\t\t}\r\n\t],\r\n\t\"datasources\": [\r\n\t\t{\r\n\t\t\t\"name\": \"Query\",\r\n\t\t\t\"type\": \"JSON\",\r\n\t\t\t\"settings\": {\r\n\t\t\t\t\"url\": \"\\/api\\/iot-ensemble\\/devices\\/telemetry\",\r\n\t\t\t\t\"use_thingproxy\": false,\r\n\t\t\t\t\"refresh\": 30,\r\n\t\t\t\t\"method\": \"GET\"\r\n\t\t\t}\r\n\t\t}\r\n\t],\r\n\t\"columns\": 3\r\n}".FromJSON<MetadataModel>();
        }

        protected virtual async Task<List<IoTEnsembleTelemetryPayload>> queryTelemetryPayloads(DocumentClient client, string entLookup,
            List<string> selectedDeviceIds, int pageSize, bool emulatedEnabled)
        {
            Uri colUri = UriFactory.CreateDocumentCollectionUri(warmTelemetryDatabase, warmTelemetryContainer);

            IQueryable<IoTEnsembleTelemetryPayload> docsQueryBldr =
                client.CreateDocumentQuery<IoTEnsembleTelemetryPayload>(colUri, new FeedOptions()
                {
                    EnableCrossPartitionQuery = true
                })
                .Where(payload => payload.EnterpriseLookup == entLookup || (emulatedEnabled && payload.EnterpriseLookup == "EMULATED"));

            if (!selectedDeviceIds.IsNullOrEmpty())
                docsQueryBldr = docsQueryBldr.Where(payload => selectedDeviceIds.Contains(payload.DeviceID));

            docsQueryBldr = docsQueryBldr
                .OrderByDescending(payload => payload._ts)
                .Skip(0)
                .Take(pageSize);

            var docsQuery = docsQueryBldr.AsDocumentQuery();

            var payloads = new List<IoTEnsembleTelemetryPayload>();

            while (docsQuery.HasMoreResults)
                payloads.AddRange(await docsQuery.ExecuteNextAsync<IoTEnsembleTelemetryPayload>());

            return payloads;
        }

        protected virtual async Task setTelemetryEnabled(SecurityManagerClient secMgr, bool enabled)
        {
            var resp = await secMgr.SetEnterpriseThirdPartyData(State.UserEnterpriseLookup, new Dictionary<string, string>()
            {
                { TELEMETRY_SYNC_ENABLED, enabled.ToString() }
            });

            if (resp.Status)
                State.Telemetry.Enabled = enabled;
        }
        #endregion
    }
}

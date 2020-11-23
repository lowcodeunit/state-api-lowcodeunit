using Fathym.Testing;
using Microsoft.AspNetCore.Http;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using Fathym;
using Fathym.API;

namespace LCU.State.API.IoTEnsemble.Tests.Host
{
    [TestClass]
    public class ConnectToStateTests : AzFunctionTestBase
    {
        
        public ConnectToStateTests() : base()
        {
            APIRoute = "api/ConnectToState";                
        }

        [TestMethod]
        public async Task TestConnectToState()
        {
            LcuEntApiKey = "";            
            PrincipalId = "";

            addRequestHeaders();

            var url = $"{HostURL}/{APIRoute}";            

            var response = await httpGet(url); 

            Assert.AreEqual(System.Net.HttpStatusCode.OK, response.StatusCode);

            var content = await getContent<BaseResponse<dynamic>>(response);

            Assert.AreEqual(Status.Success, content.Status);        

            throw new NotImplementedException("Implement me!");                  
        }
    }
}

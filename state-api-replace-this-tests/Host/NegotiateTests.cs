using Fathym.Testing;
using Microsoft.AspNetCore.Http;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading.Tasks;

namespace state_api_replace_this_tests
{
    [TestClass]
    public class NegotiateTests : AzFunctionTestBase
    {
        
        public NegotiateTests() : base()
        {
            APIRoute = "api/negotiate";                
        }

        [TestMethod]
        public async Task TestNegotiate()
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

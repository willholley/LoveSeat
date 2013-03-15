using Newtonsoft.Json.Linq;

namespace LoveSeat.Async {
    public class CouchResponse : JObject {
        public CouchResponse(JObject obj) : base(obj)
        {
        }
        public int StatusCode { get; set; }
    }
}

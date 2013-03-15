using Newtonsoft.Json;

namespace LoveSeat.Async.Interfaces
{
    public interface IBaseObject
    {
        [JsonProperty("_id")]
        string Id { get; set; }
        [JsonProperty("_rev")]
        string Rev { get; set; }
        string Type { get; }
    }
}

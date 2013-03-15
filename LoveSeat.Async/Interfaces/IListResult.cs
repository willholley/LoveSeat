﻿using System.Net;

namespace LoveSeat.Async.Interfaces
{
    public interface IListResult: System.IEquatable<IListResult>
    {
        /// <summary>
        /// Typically won't be needed Provided for debuging assitance
        /// </summary>
        HttpWebRequest Request { get; }
        /// <summary>
        /// Typically won't be needed Provided for debuging assitance
        /// </summary>
        HttpWebResponse Response { get; }
        HttpStatusCode StatusCode { get; }
        string Etag { get; }
        string RawString { get; }

    }
}